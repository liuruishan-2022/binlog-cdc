use std::collections::HashMap;

use futures_util::TryStreamExt;
use moka::sync::Cache;
use serde_json::Value;
use sqlx::query_builder::Separated;
use sqlx::{MySql, MySqlPool, QueryBuilder, Row};
use tracing::{info, warn};

use crate::common::schema::ColumnMeta;
use crate::config::CdcConfig;
use crate::pipeline::formatter::DebeziumFormat;
use crate::pipeline::message::PipelineRecord;
use crate::sink::SinkStream;

/// MySQL sink for Debezium records.
pub struct MysqlSink {
    pool: MySqlPool,
    cache: Cache<String, TableMeta>,
    route: HashMap<String, String>,
}

impl MysqlSink {
    pub async fn new(url: &str) -> Self {
        Self::new_with_routes(url, HashMap::new()).await
    }

    pub async fn create(config: &CdcConfig) -> Self {
        let sink = match config.sink() {
            crate::config::sink::Sink::Mysql(mysql) => mysql,
            _ => panic!("mysql sink need mysql config"),
        };
        let route = config
            .route()
            .map(|routes| {
                routes
                    .iter()
                    .map(|route| (route.source().to_string(), route.sink().to_string()))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();

        Self::new_with_routes(&sink.url(), route).await
    }

    pub async fn new_with_routes(url: &str, route: HashMap<String, String>) -> Self {
        let pool = MySqlPool::connect(url)
            .await
            .expect(format!("connect to mysql use:{} failed", url).as_str());
        let cache = Cache::new(10000);
        MysqlSink { pool, cache, route }
    }

    pub async fn table_info(&self, source: &str) -> TableMeta {
        let table = self.table_name(source);
        if let Some(meta) = self.cache.get(table.as_str()) {
            return meta;
        }

        let meta = self.desc_table(table.as_str()).await;
        self.cache.insert(table, meta.clone());
        meta
    }

    pub async fn delete(&self, debezium: &DebeziumFormat, topic: &str) {
        let meta = self.table_info(topic).await;
        if meta.primary_keys().is_empty() {
            warn!(
                "skip mysql delete because table has no primary key: {}",
                meta.table()
            );
            return;
        }

        let mut builder = QueryBuilder::<MySql>::new("DELETE FROM ");
        builder.push(meta.table()).push(" WHERE ");
        let mut separated = builder.separated(" AND ");
        for key in meta.primary_keys() {
            let Some(value) = debezium.before_column(key) else {
                warn!("skip mysql delete because primary key {} missing", key);
                return;
            };
            push_bound_equality(&mut separated, key, json_to_mysql_value(value));
        }

        let result = builder.build().execute(&self.pool).await;
        match result {
            Ok(result) => info!(
                "mysql delete rows:{} table:{}",
                result.rows_affected(),
                meta.table()
            ),
            Err(err) => warn!("mysql delete error:{:?} table:{}", err, meta.table()),
        }
    }

    pub async fn upsert_data(&self, debezium: &DebeziumFormat, topic: &str) {
        let meta = self.table_info(topic).await;
        let columns = meta
            .columns
            .iter()
            .filter(|column| debezium.after_column(column.column_name()).is_some())
            .collect::<Vec<_>>();

        if columns.is_empty() {
            warn!(
                "skip mysql upsert because after has no known columns, topic={}",
                topic
            );
            return;
        }

        let values = columns
            .iter()
            .map(|column| {
                let value = debezium
                    .after_column(column.column_name())
                    .unwrap_or(&Value::Null);
                (column.column_name(), json_to_mysql_value(value))
            })
            .collect::<Vec<_>>();
        let mut builder = build_upsert_query(meta.table(), &values, meta.primary_keys());

        let result = builder.build().execute(&self.pool).await;
        match result {
            Ok(result) => info!(
                "mysql upsert op:{} rows:{} table:{}",
                debezium.op(),
                result.rows_affected(),
                meta.table()
            ),
            Err(err) => warn!(
                "mysql upsert op:{} error:{:?} table:{}",
                debezium.op(),
                err,
                meta.table()
            ),
        }
    }

    pub async fn process_record(&self, record: &PipelineRecord) {
        match record {
            PipelineRecord::KafkaDebezium(data) => {
                self.process(data.data(), data.topic()).await;
            }
            _ => {
                warn!("unknown type do data process");
            }
        }
    }

    async fn desc_table(&self, table: &str) -> TableMeta {
        let sql = format!("desc {}", table);
        let mut rows = sqlx::query(&sql).fetch(&self.pool);

        let mut source_position = 1;
        let mut columns = vec![];
        while let Some(row) = rows.try_next().await.unwrap() {
            let field: &str = row.try_get("Field").expect("fetch desc table field error!");
            let key: Result<Vec<u8>, sqlx::Error> = row.try_get("Key");
            let key = Self::judge_primary_key(key);

            columns.push(ColumnMeta::new(source_position, field.to_string(), key));
            source_position += 1;
        }

        TableMeta::new(table.to_string(), columns)
    }

    fn table_name(&self, source: &str) -> String {
        self.route
            .get(source)
            .cloned()
            .or_else(|| self.route.get("*").cloned())
            .unwrap_or_else(|| source.to_string())
    }

    fn judge_primary_key(key: Result<Vec<u8>, sqlx::Error>) -> bool {
        match key {
            Ok(key) => match String::from_utf8(key) {
                Ok(key) => key == "PRI",
                Err(err) => {
                    warn!("can not convert to utf8:{:?}!", err);
                    false
                }
            },
            Err(err) => {
                warn!("error:{:?}", err);
                false
            }
        }
    }
}

impl SinkStream for MysqlSink {
    async fn process(&self, debezium: &DebeziumFormat, topic: &str) {
        match debezium.op() {
            "d" => {
                self.delete(debezium, topic).await;
            }
            "c" | "r" | "u" => {
                self.upsert_data(debezium, topic).await;
            }
            _ => {
                warn!("未知的操作类型:{}", debezium.op());
            }
        }
    }

    async fn handle_messages(&self, _messages: Vec<DebeziumFormat>) {}
}

fn json_to_mysql_value(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Bool(value) => Some(if *value {
            "1".to_string()
        } else {
            "0".to_string()
        }),
        Value::Number(value) => Some(value.to_string()),
        Value::String(value) => Some(value.clone()),
        Value::Array(_) | Value::Object(_) => Some(value.to_string()),
    }
}

fn build_upsert_query(
    table: &str,
    columns: &[(&str, Option<String>)],
    primary_keys: &[String],
) -> QueryBuilder<'static, MySql> {
    let mut builder = QueryBuilder::<MySql>::new("INSERT INTO ");
    builder.push(table).push(" (");
    {
        let mut column_names = builder.separated(", ");
        for (column, _) in columns {
            column_names.push(column);
        }
    }
    builder.push(") VALUES (");
    {
        let mut values = builder.separated(", ");
        for (_, value) in columns {
            values.push_bind(value.clone());
        }
    }
    builder.push(") ON DUPLICATE KEY UPDATE ");
    {
        let mut update_columns = columns
            .iter()
            .map(|(column, _)| *column)
            .filter(|column| !primary_keys.iter().any(|key| key == column))
            .collect::<Vec<_>>();
        if update_columns.is_empty() {
            update_columns.push(columns[0].0);
        }

        let mut assignments = builder.separated(", ");
        for column in update_columns {
            assignments
                .push(column)
                .push_unseparated(" = VALUES(")
                .push_unseparated(column)
                .push_unseparated(")");
        }
    }
    builder
}

fn push_bound_equality<'qb, 'args>(
    separated: &mut Separated<'qb, 'args, MySql, &'static str>,
    column: &str,
    value: Option<String>,
) {
    separated
        .push(column)
        .push_unseparated(" = ")
        .push_bind_unseparated(value);
}

///
/// 放置table的元数据信息的struct
///
#[derive(Debug, Clone)]
pub struct TableMeta {
    table: String,
    columns: Vec<ColumnMeta>,
    primary_keys: Vec<String>,
}

impl TableMeta {
    pub fn new(table: String, columns: Vec<ColumnMeta>) -> Self {
        let keys = columns
            .iter()
            .filter(|ele| ele.is_primary())
            .map(|ele| ele.column_name().to_string())
            .collect::<Vec<String>>();
        TableMeta {
            table,
            columns,
            primary_keys: keys,
        }
    }

    pub fn primary_keys(&self) -> &Vec<String> {
        &self.primary_keys
    }

    pub fn table(&self) -> &str {
        self.table.as_str()
    }
}

#[cfg(test)]
mod tests {
    use sqlx::{MySql, QueryBuilder};

    use super::{build_upsert_query, push_bound_equality};

    #[test]
    fn upsert_inserts_missing_rows_and_updates_non_primary_columns() {
        let columns = vec![
            ("id", Some("1603999".to_string())),
            ("hostname", Some(String::new())),
            ("port", Some("-1".to_string())),
        ];
        let primary_keys = vec!["id".to_string()];

        let builder = build_upsert_query("test_table", &columns, &primary_keys);

        assert_eq!(
            builder.sql(),
            "INSERT INTO test_table (id, hostname, port) VALUES (?, ?, ?) \
ON DUPLICATE KEY UPDATE hostname = VALUES(hostname), port = VALUES(port)"
        );
    }

    #[test]
    fn bound_equalities_in_set_are_separated_by_commas() {
        let mut builder = QueryBuilder::<MySql>::new("UPDATE test_table SET ");
        {
            let mut set = builder.separated(", ");
            push_bound_equality(&mut set, "hostname", Some("db-host".to_string()));
            push_bound_equality(&mut set, "port", Some("3306".to_string()));
        }

        assert_eq!(
            builder.sql(),
            "UPDATE test_table SET hostname = ?, port = ?"
        );
    }

    #[test]
    fn bound_equalities_in_where_are_separated_by_and() {
        let mut builder = QueryBuilder::<MySql>::new("DELETE FROM test_table WHERE ");
        {
            let mut conditions = builder.separated(" AND ");
            push_bound_equality(&mut conditions, "id", Some("1".to_string()));
            push_bound_equality(&mut conditions, "tenant_id", Some("2".to_string()));
        }

        assert_eq!(
            builder.sql(),
            "DELETE FROM test_table WHERE id = ? AND tenant_id = ?"
        );
    }
}
