//! MySQL source implementation
//!
//! Provides MySQL source using sqlx for querying data from MySQL databases.

use crate::source::Source;
use async_trait::async_trait;
use sqlx::{Column, Row};
use sqlx::mysql::MySqlPoolOptions;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

#[derive(Debug, Clone)]
pub struct MySqlSourceConfig {
    pub connection_url: String,

    pub query: String,

    pub poll_interval: u64,

    pub max_connections: u32,

    pub min_connections: u32,

    pub connect_timeout: u64,
}

impl Default for MySqlSourceConfig {
    fn default() -> Self {
        Self {
            connection_url: "mysql://root:password@localhost:3306/test".to_string(),
            query: "SELECT * FROM test_table".to_string(),
            poll_interval: 5,
            max_connections: 5,
            min_connections: 1,
            connect_timeout: 30,
        }
    }
}

/// Row data from MySQL query
#[derive(Debug, Clone)]
pub struct MySqlRow {
    pub columns: Vec<(String, Option<String>)>,
}

/// MySQL source implementation
pub struct MySqlSource {
    config: MySqlSourceConfig,
    pool: Option<sqlx::Pool<sqlx::MySql>>,
    running: Arc<AtomicBool>,
    row_handler: Option<Box<dyn Fn(MySqlRow) + Send + Sync>>,
}

impl MySqlSource {
    pub fn new(connection_url: String, query: String) -> Self {
        Self::with_config(MySqlSourceConfig {
            connection_url,
            query,
            ..Default::default()
        })
    }

    pub fn with_config(config: MySqlSourceConfig) -> Self {
        Self {
            config,
            pool: None,
            running: Arc::new(AtomicBool::new(false)),
            row_handler: None,
        }
    }

    fn is_running_inner(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    pub fn set_row_handler<F>(&mut self, handler: F)
    where
        F: Fn(MySqlRow) + Send + Sync + 'static,
    {
        self.row_handler = Some(Box::new(handler));
    }

    async fn connect(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Connecting to MySQL: {}", self.mask_url());

        let pool = MySqlPoolOptions::new()
            .max_connections(self.config.max_connections)
            .min_connections(self.config.min_connections)
            .acquire_timeout(Duration::from_secs(self.config.connect_timeout))
            .connect(&self.config.connection_url)
            .await?;

        self.pool = Some(pool);
        Ok(())
    }

    fn mask_url(&self) -> String {
        self.config.connection_url
            .split(':')
            .enumerate()
            .map(|(i, part)| {
                if i == 2 {
                    part.chars().map(|_| 'x').collect::<String>()
                } else {
                    part.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(":")
    }

    pub async fn query(&mut self) -> Result<Vec<MySqlRow>, Box<dyn std::error::Error>> {
        if !self.is_running_inner() {
            return Err("MySQL source is not running".into());
        }

        let pool = self.pool.as_ref().ok_or("Pool not initialized")?;

        let rows = sqlx::query(&self.config.query).fetch_all(pool).await?;

        let result = rows
            .into_iter()
            .map(|row| {
                let columns = row
                    .columns()
                    .iter()
                    .map(|col| {
                        let value = row.try_get::<String, _>(col.name())
                            .ok()
                            .or_else(|| {
                                row.try_get::<i64, _>(col.name())
                                    .ok()
                                    .map(|v| v.to_string())
                            })
                            .or_else(|| {
                                row.try_get::<f64, _>(col.name())
                                    .ok()
                                    .map(|v| v.to_string())
                            });
                        (col.name().to_string(), value)
                    })
                    .collect();

                MySqlRow { columns }
            })
            .collect();

        Ok(result)
    }
}

#[async_trait]
impl Source for MySqlSource {
    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.is_running_inner() {
            info!("MySQL source is already running");
            return Ok(());
        }

        info!("Starting MySQL source with query: {}", self.config.query);

        self.connect().await?;
        self.running.store(true, Ordering::Relaxed);

        let running = self.running.clone();
        let poll_interval = Duration::from_secs(self.config.poll_interval);
        let query = self.config.query.clone();
        let pool = self.pool.clone().ok_or("Pool not initialized")?;

        // Spawn polling task
        tokio::spawn(async move {
            info!("Starting MySQL poll task with interval: {:?}", poll_interval);

            while running.load(Ordering::Relaxed) {
                match sqlx::query(&query).fetch_all(&pool).await {
                    Ok(rows) => {
                        for row in rows {
                            let columns: Vec<(String, Option<String>)> = row
                                .columns()
                                .iter()
                                .map(|col| {
                                    let value = row.try_get::<String, _>(col.name())
                                        .ok()
                                        .or_else(|| {
                                            row.try_get::<i64, _>(col.name())
                                                .ok()
                                                .map(|v| v.to_string())
                                        })
                                        .or_else(|| {
                                            row.try_get::<f64, _>(col.name())
                                                .ok()
                                                .map(|v| v.to_string())
                                        });
                                    (col.name().to_string(), value)
                                })
                                .collect();

                            debug!("MySQL row: {:?}", columns);
                            // Process row here - you can emit through a channel or callback
                        }
                    }
                    Err(e) => {
                        error!("Error executing query: {}", e);
                    }
                }

                // Wait for next poll
                tokio::time::sleep(poll_interval).await;
            }

            info!("MySQL poll task stopped");
        });

        info!("MySQL source started successfully");
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.is_running_inner() {
            info!("MySQL source is not running");
            return Ok(());
        }

        info!("Stopping MySQL source");
        self.running.store(false, Ordering::Relaxed);

        // Allow time for consumer to stop gracefully
        tokio::time::sleep(Duration::from_millis(500)).await;

        if let Some(pool) = &self.pool {
            pool.close().await;
        }

        self.pool = None;

        info!("MySQL source stopped");
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.is_running_inner()
    }

    fn set_sender(&mut self, _sender: mpsc::Sender<crate::pipeline::message::PipelineMessage>) {
        // MySQL source doesn't use sender in current implementation
        // Messages are handled via row_handler callback
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = MySqlSourceConfig::default();
        assert_eq!(config.poll_interval, 5);
        assert_eq!(config.max_connections, 5);
        assert_eq!(config.query, "SELECT * FROM test_table");
    }

    #[test]
    fn test_mysql_source_creation() {
        let source = MySqlSource::new(
            "mysql://root:password@localhost:3306/test".to_string(),
            "SELECT * FROM users".to_string(),
        );
        assert!(!source.is_running());
    }
}
