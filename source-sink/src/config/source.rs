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
    pub bootstrap_servers: Vec<String>,

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
    pub fn new(
        name: String,
        bootstrap_servers: Vec<String>,
        group_id: String,
        topic: Option<String>,
        partition: Option<i32>,
    ) -> Self {
        Self {
            name,
            bootstrap_servers,
            group_id,
            topic,
            partition,
            start_offset: default_start_offset(),
        }
    }

    pub fn bootstrap_servers_str(&self) -> String {
        self.bootstrap_servers.join(",")
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
    pub fn new(
        name: String,
        connection_url: String,
        query: String,
    ) -> Self {
        Self {
            name,
            connection_url,
            query,
            poll_interval_secs: default_poll_interval(),
            max_connections: default_max_connections(),
            connect_timeout_secs: default_connect_timeout(),
        }
    }

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

impl FileSourceConfig {
    pub fn new(name: String, path: String) -> Self {
        Self {
            name,
            path,
            read_mode: default_read_mode(),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ConsoleSourceConfig {
    pub name: String,
}

impl ConsoleSourceConfig {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_kafka_source() {
        let yaml = r#"
type: kafka
name: test_source
properties.bootstrap.servers:
  - localhost:9092
  - localhost:9093
properties.group.id: test-group
topic: test-topic
partition: 0
"#;

        let config: KafkaSourceConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.name, "test_source");
        assert_eq!(config.bootstrap_servers.len(), 2);
        assert_eq!(config.topic, "test-topic");
    }

    #[test]
    fn test_deserialize_mysql_source() {
        let yaml = r#"
type: mysql
name: mysql_source
connection.url: mysql://root:password@localhost:3306/test
query: SELECT * FROM users
poll.interval.secs: 10
"#;

        let config: MySqlSourceConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.name, "mysql_source");
        assert_eq!(config.poll_interval_secs, 10);
    }

    #[test]
    fn test_serialize_source_config() {
        let config = SourceConfig::Kafka(KafkaSourceConfig {
            name: "test".to_string(),
            bootstrap_servers: vec!["localhost:9092".to_string()],
            group_id: "test-group".to_string(),
            topic: "test".to_string(),
            partition: 0,
            start_offset: "latest".to_string(),
        });

        let yaml = serde_yaml::to_string(&config).unwrap();
        println!("{}", yaml);
        assert!(yaml.contains("type: kafka"));
    }
}
