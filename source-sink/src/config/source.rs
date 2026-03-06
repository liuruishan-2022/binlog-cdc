//! Source configuration
//!
//! Defines configuration structures for various data sources.

use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum SourceConfig {
    #[serde(rename = "kafka")]
    Kafka(KafkaSourceConfig),

    #[serde(rename = "mysql")]
    MySql(MySqlSourceConfig),

    #[serde(rename = "file")]
    File(FileSourceConfig),

    #[serde(rename = "console")]
    Console(ConsoleSourceConfig),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct KafkaSourceConfig {
    pub name: String,

    #[serde(rename = "properties.bootstrap.servers")]
    pub bootstrap_servers: String,

    #[serde(rename = "properties.group.id")]
    pub group_id: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<i32>,

    #[serde(default = "default_start_offset")]
    pub start_offset: String,
}

fn default_start_offset() -> String {
    "latest".to_string()
}

impl KafkaSourceConfig {
    pub fn bootstrap_servers(&self) -> Vec<String> {
        self.bootstrap_servers
            .split(',')
            .map(|s| s.to_string())
            .collect()
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MySqlSourceConfig {
    pub name: String,

    #[serde(rename = "connection.url")]
    pub connection_url: String,

    pub query: String,

    #[serde(rename = "poll.interval.secs", default = "default_poll_interval")]
    pub poll_interval_secs: u64,

    #[serde(rename = "pool.max.connections", default = "default_max_connections")]
    pub max_connections: u32,

    #[serde(rename = "connect.timeout.secs", default = "default_connect_timeout")]
    pub connect_timeout_secs: u64,
}

fn default_poll_interval() -> u64 {
    5
}

fn default_max_connections() -> u32 {
    5
}

fn default_connect_timeout() -> u64 {
    30
}

impl MySqlSourceConfig {
    pub fn poll_interval(&self) -> Duration {
        Duration::from_secs(self.poll_interval_secs)
    }

    pub fn connect_timeout(&self) -> Duration {
        Duration::from_secs(self.connect_timeout_secs)
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct FileSourceConfig {
    pub name: String,

    pub path: String,

    #[serde(default = "default_read_mode")]
    pub read_mode: String,
}

fn default_read_mode() -> String {
    "read".to_string()
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ConsoleSourceConfig {
    pub name: String,
}
