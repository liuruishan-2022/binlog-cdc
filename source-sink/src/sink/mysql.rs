//! MySQL-based sink implementation
//!
//! Provides MySQL sink using sqlx for writing data to MySQL databases.

use crate::common::DebeziumFormat;
use crate::pipeline::message::{PipelineMessage, RouteInfo};
use crate::sink::Sink;
use async_trait::async_trait;
use futures_util::TryStreamExt;
use moka::future::Cache;
use serde::{Deserialize, Serialize};
use sqlx::MySql;
use sqlx::Row;
use sqlx::mysql::MySqlPoolOptions;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MySqlSinkConfig {
    pub connection_url: String,

    pub max_connections: u32,

    pub min_connections: u32,

    pub connect_timeout: u64,

    pub idle_timeout: u64,

    pub max_lifetime: u64,

    pub use_tls: bool,

    #[serde(default = "default_cache_ttl_secs")]
    pub cache_ttl_secs: u64,

    #[serde(default = "default_cache_max_capacity")]
    pub cache_max_capacity: u64,
}

fn default_cache_ttl_secs() -> u64 {
    300
}

fn default_cache_max_capacity() -> u64 {
    10000
}

impl Default for MySqlSinkConfig {
    fn default() -> Self {
        Self {
            connection_url: "mysql://root:password@localhost:3306/test".to_string(),
            max_connections: 10,
            min_connections: 1,
            connect_timeout: 30,
            idle_timeout: 600,
            max_lifetime: 1800,
            use_tls: false,
            cache_ttl_secs: 300,
            cache_max_capacity: 10000,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub ordinal_position: u32,
    pub column_name: String,
    pub is_primary_key: bool,
}

impl ColumnMeta {
    pub fn new(ordinal_position: u32, column_name: String, is_primary_key: bool) -> Self {
        ColumnMeta {
            ordinal_position,
            column_name,
            is_primary_key,
        }
    }

    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    pub fn is_primary(&self) -> bool {
        self.is_primary_key
    }
}

#[derive(Debug, Clone)]
pub struct TableMeta {
    pub database: String,
    pub table: String,
    pub columns: Vec<ColumnMeta>,
    pub primary_keys: Vec<String>,
}

impl TableMeta {
    pub fn new(database: String, table: String, columns: Vec<ColumnMeta>) -> Self {
        let primary_keys = columns
            .iter()
            .filter(|c| c.is_primary())
            .map(|c| c.column_name.clone())
            .collect::<Vec<String>>();

        if primary_keys.is_empty() {
            warn!(
                "Table {}.{} has no primary key, using first column as fallback",
                database, table
            );
        }

        TableMeta {
            database,
            table,
            columns,
            primary_keys,
        }
    }

    pub fn qualified_name(&self) -> String {
        format!("`{}`.`{}`", self.database, self.table)
    }

    pub fn all_columns(&self) -> String {
        self.columns
            .iter()
            .map(|c| format!("`{}`", c.column_name))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

pub struct MySqlSink {
    config: MySqlSinkConfig,
    pool: Option<sqlx::Pool<MySql>>,
    cache: Cache<String, TableMeta>,
    running: Arc<AtomicBool>,
}

impl MySqlSink {
    pub fn new(connection_url: String) -> Self {
        Self::with_config(MySqlSinkConfig {
            connection_url,
            ..Default::default()
        })
    }

    pub fn with_config(config: MySqlSinkConfig) -> Self {
        let cache = Cache::builder()
            .max_capacity(config.cache_max_capacity)
            .time_to_live(Duration::from_secs(config.cache_ttl_secs))
            .build();

        Self {
            config,
            pool: None,
            cache,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    fn is_running_inner(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    async fn connect(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Connecting to MySQL: {}", self.mask_url());

        let pool = MySqlPoolOptions::new()
            .max_connections(self.config.max_connections)
            .min_connections(self.config.min_connections)
            .acquire_timeout(std::time::Duration::from_secs(self.config.connect_timeout))
            .idle_timeout(std::time::Duration::from_secs(self.config.idle_timeout))
            .max_lifetime(std::time::Duration::from_secs(self.config.max_lifetime))
            .connect(&self.config.connection_url)
            .await?;

        self.pool = Some(pool);
        Ok(())
    }

    fn mask_url(&self) -> String {
        self.config
            .connection_url
            .split('@')
            .enumerate()
            .map(|(i, part)| {
                if i == 0 {
                    part.chars()
                        .map(|c| if c == ':' { ':' } else { 'x' })
                        .collect::<String>()
                } else {
                    part.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("@")
    }

    async fn get_table_meta(&self, database: &str, table: &str) -> Result<TableMeta, Box<dyn std::error::Error>> {
        let key = format!("{}.{}", database, table);

        if let Some(meta) = self.cache.get(&key).await {
            debug!("Cache hit for table: {}", key);
            return Ok(meta);
        }

        info!("Cache miss, fetching table meta for: {}", key);
        let meta = self.desc_table(database, table).await?;
        self.cache.insert(key, meta.clone()).await;
        Ok(meta)
    }

    async fn desc_table(&self, database: &str, table: &str) -> Result<TableMeta, Box<dyn std::error::Error>> {
        let sql = format!("DESC `{}`.`{}`", database, table);
        let pool = self.pool.as_ref().ok_or("Pool not initialized")?;

        let mut rows = sqlx::query(&sql).fetch(pool);
        let mut columns = vec![];
        let mut position = 1;

        while let Some(row) = rows.try_next().await? {
            let field: String = row.try_get("Field")?;
            let key: Result<Vec<u8>, sqlx::Error> = row.try_get("Key");
            let is_primary = Self::judge_primary_key(key);

            columns.push(ColumnMeta::new(position, field, is_primary));
            position += 1;
        }

        if columns.is_empty() {
            return Err(format!("Table {}.{} has no columns", database, table).into());
        }

        Ok(TableMeta::new(database.to_string(), table.to_string(), columns))
    }

    fn judge_primary_key(key: Result<Vec<u8>, sqlx::Error>) -> bool {
        match key {
            Ok(key) => {
                if let Ok(key_str) = String::from_utf8(key) {
                    key_str == "PRI"
                } else {
                    false
                }
            }
            Err(_) => false,
        }
    }

    fn parse_sink_table(sink_table: &str) -> Result<(String, String), Box<dyn std::error::Error>> {
        let parts: Vec<&str> = sink_table.split('.').collect();
        match parts.len() {
            2 => Ok((parts[0].to_string(), parts[1].to_string())),
            _ => Err(format!(
                "Invalid sink_table format '{}', expected 'database.table'",
                sink_table
            )
            .into()),
        }
    }

    async fn process_message(&self, msg: &PipelineMessage, route_info: &RouteInfo) -> Result<(), Box<dyn std::error::Error>> {
        let (database, table) = Self::parse_sink_table(route_info.sink_table())?;
        let table_meta = self.get_table_meta(&database, &table).await?;

        match msg.data.op() {
            "c" => self.execute_insert(&table_meta, &msg.data).await?,
            "d" => self.execute_delete(&table_meta, &msg.data).await?,
            "u" => self.execute_update(&table_meta, &msg.data).await?,
            op => warn!("Unknown operation type: {}", op),
        }

        Ok(())
    }

    async fn execute_insert(&self, table_meta: &TableMeta, data: &DebeziumFormat) -> Result<(), Box<dyn std::error::Error>> {
        let pool = self.pool.as_ref().ok_or("Pool not initialized")?;

        let columns = table_meta.all_columns();
        let placeholders = table_meta
            .columns
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table_meta.qualified_name(),
            columns,
            placeholders
        );

        let mut query = sqlx::query(&sql);

        for column in &table_meta.columns {
            let value = data.after_column(&column.column_name);
            query = match value {
                Some(serde_json::Value::Null) => query.bind::<Option<String>>(None),
                Some(serde_json::Value::Bool(b)) => query.bind(*b),
                Some(serde_json::Value::Number(n)) => {
                    if let Some(i) = n.as_i64() {
                        query.bind(i)
                    } else if let Some(f) = n.as_f64() {
                        query.bind(f)
                    } else {
                        query.bind::<Option<String>>(None)
                    }
                }
                Some(serde_json::Value::String(s)) => query.bind(s.as_str()),
                Some(v) => query.bind(v.to_string()),
                None => query.bind::<Option<String>>(None),
            };
        }

        let result = query.execute(pool).await?;
        debug!(
            "Insert executed on {}, rows affected: {}",
            table_meta.qualified_name(),
            result.rows_affected()
        );

        Ok(())
    }

    async fn execute_delete(&self, table_meta: &TableMeta, data: &DebeziumFormat) -> Result<(), Box<dyn std::error::Error>> {
        let pool = self.pool.as_ref().ok_or("Pool not initialized")?;

        let keys = &table_meta.primary_keys;
        if keys.is_empty() {
            return Err(format!(
                "Cannot delete from {} without primary key",
                table_meta.qualified_name()
            )
            .into());
        }

        let where_clause = keys
            .iter()
            .map(|k| format!("`{}` = ?", k))
            .collect::<Vec<_>>()
            .join(" AND ");

        let sql = format!(
            "DELETE FROM {} WHERE {}",
            table_meta.qualified_name(),
            where_clause
        );

        let mut query = sqlx::query(&sql);

        for key in keys {
            let value = data.before_column(key);
            query = match value {
                Some(serde_json::Value::Null) => query.bind::<Option<String>>(None),
                Some(serde_json::Value::Bool(b)) => query.bind(*b),
                Some(serde_json::Value::Number(n)) => {
                    if let Some(i) = n.as_i64() {
                        query.bind(i)
                    } else if let Some(f) = n.as_f64() {
                        query.bind(f)
                    } else {
                        query.bind::<Option<String>>(None)
                    }
                }
                Some(serde_json::Value::String(s)) => query.bind(s.as_str()),
                Some(v) => query.bind(v.to_string()),
                None => query.bind::<Option<String>>(None),
            };
        }

        let result = query.execute(pool).await?;
        debug!(
            "Delete executed on {}, rows affected: {}",
            table_meta.qualified_name(),
            result.rows_affected()
        );

        Ok(())
    }

    async fn execute_update(&self, table_meta: &TableMeta, data: &DebeziumFormat) -> Result<(), Box<dyn std::error::Error>> {
        let pool = self.pool.as_ref().ok_or("Pool not initialized")?;

        let keys = &table_meta.primary_keys;
        if keys.is_empty() {
            return Err(format!(
                "Cannot update {} without primary key",
                table_meta.qualified_name()
            )
            .into());
        }

        let set_clause = table_meta
            .columns
            .iter()
            .map(|c| format!("`{}` = ?", c.column_name))
            .collect::<Vec<_>>()
            .join(", ");

        let where_clause = keys
            .iter()
            .map(|k| format!("`{}` = ?", k))
            .collect::<Vec<_>>()
            .join(" AND ");

        let sql = format!(
            "UPDATE {} SET {} WHERE {}",
            table_meta.qualified_name(),
            set_clause,
            where_clause
        );

        let mut query = sqlx::query(&sql);

        for column in &table_meta.columns {
            let value = data.after_column(&column.column_name);
            query = match value {
                Some(serde_json::Value::Null) => query.bind::<Option<String>>(None),
                Some(serde_json::Value::Bool(b)) => query.bind(*b),
                Some(serde_json::Value::Number(n)) => {
                    if let Some(i) = n.as_i64() {
                        query.bind(i)
                    } else if let Some(f) = n.as_f64() {
                        query.bind(f)
                    } else {
                        query.bind::<Option<String>>(None)
                    }
                }
                Some(serde_json::Value::String(s)) => query.bind(s.as_str()),
                Some(v) => query.bind(v.to_string()),
                None => query.bind::<Option<String>>(None),
            };
        }

        for key in keys {
            let value = data.before_column(key);
            query = match value {
                Some(serde_json::Value::Null) => query.bind::<Option<String>>(None),
                Some(serde_json::Value::Bool(b)) => query.bind(*b),
                Some(serde_json::Value::Number(n)) => {
                    if let Some(i) = n.as_i64() {
                        query.bind(i)
                    } else if let Some(f) = n.as_f64() {
                        query.bind(f)
                    } else {
                        query.bind::<Option<String>>(None)
                    }
                }
                Some(serde_json::Value::String(s)) => query.bind(s.as_str()),
                Some(v) => query.bind(v.to_string()),
                None => query.bind::<Option<String>>(None),
            };
        }

        let result = query.execute(pool).await?;
        debug!(
            "Update executed on {}, rows affected: {}",
            table_meta.qualified_name(),
            result.rows_affected()
        );

        Ok(())
    }
}

#[async_trait]
impl Sink for MySqlSink {
    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.is_running_inner() {
            info!("MySQL sink is already running");
            return Ok(());
        }

        info!("Starting MySQL sink");

        self.connect().await?;
        self.running.store(true, Ordering::Relaxed);

        info!("MySQL sink started successfully");
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.is_running_inner() {
            info!("MySQL sink is not running");
            return Ok(());
        }

        info!("Stopping MySQL sink");

        self.running.store(false, Ordering::Relaxed);

        if let Some(pool) = &self.pool {
            pool.close().await;
        }

        self.pool = None;

        info!("MySQL sink stopped");
        Ok(())
    }

    async fn write(&mut self, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        if !self.is_running_inner() {
            return Err("MySQL sink is not running".into());
        }

        let msg: PipelineMessage = serde_json::from_slice(&data)?;

        if !msg.has_routes() {
            warn!("PipelineMessage has no routes, skipping");
            return Ok(());
        }

        for route_info in msg.routes() {
            if let Err(e) = self.process_message(&msg, route_info).await {
                error!(
                    "Error processing message for route {}: {}",
                    route_info.sink_table(), e
                );
            }
        }

        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        debug!("MySQL flush: nothing to do (pool auto-commits)");
        Ok(())
    }

    fn is_ready(&self) -> bool {
        self.is_running_inner() && self.pool.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = MySqlSinkConfig::default();
        assert_eq!(config.max_connections, 10);
        assert_eq!(config.min_connections, 1);
        assert_eq!(config.cache_ttl_secs, 300);
        assert_eq!(config.cache_max_capacity, 10000);
        assert!(!config.use_tls);
    }

    #[test]
    fn test_parse_sink_table() {
        let result = MySqlSink::parse_sink_table("db.table").unwrap();
        assert_eq!(result.0, "db");
        assert_eq!(result.1, "table");

        let result = MySqlSink::parse_sink_table("database_name.table_name").unwrap();
        assert_eq!(result.0, "database_name");
        assert_eq!(result.1, "table_name");

        let result = MySqlSink::parse_sink_table("invalid");
        assert!(result.is_err());

        let result = MySqlSink::parse_sink_table("a.b.c");
        assert!(result.is_err());
    }

    #[test]
    fn test_column_meta() {
        let col = ColumnMeta::new(1, "id".to_string(), true);
        assert_eq!(col.column_name(), "id");
        assert!(col.is_primary());
        assert_eq!(col.ordinal_position, 1);
    }

    #[test]
    fn test_table_meta() {
        let columns = vec![
            ColumnMeta::new(1, "id".to_string(), true),
            ColumnMeta::new(2, "name".to_string(), false),
        ];
        let meta = TableMeta::new("db".to_string(), "test".to_string(), columns);

        assert_eq!(meta.database, "db");
        assert_eq!(meta.table, "test");
        assert_eq!(meta.primary_keys.len(), 1);
        assert_eq!(meta.primary_keys[0], "id");
        assert_eq!(meta.qualified_name(), "`db`.`test`");
    }

    #[test]
    fn test_judge_primary_key() {
        let key: Result<Vec<u8>, sqlx::Error> = Ok(b"PRI".to_vec());
        assert!(MySqlSink::judge_primary_key(key));

        let key: Result<Vec<u8>, sqlx::Error> = Ok(b"UNI".to_vec());
        assert!(!MySqlSink::judge_primary_key(key));

        let key: Result<Vec<u8>, sqlx::Error> = Err(sqlx::Error::RowNotFound);
        assert!(!MySqlSink::judge_primary_key(key));
    }
}
