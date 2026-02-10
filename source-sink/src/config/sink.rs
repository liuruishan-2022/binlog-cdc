//! Sink configuration
//!
//! Defines configuration structures for various data sinks.

use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum SinkConfig {
    #[serde(rename = "kafka")]
    Kafka(KafkaSinkConfig),

    #[serde(rename = "mysql")]
    MySql(MySqlSinkConfig),

    #[serde(rename = "file")]
    File(FileSinkConfig),

    #[serde(rename = "console")]
    Console(ConsoleConfig),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct KafkaSinkConfig {
    pub name: String,

    #[serde(rename = "properties.bootstrap.servers")]
    pub bootstrap_servers: Vec<String>,

    #[serde(rename = "properties.compression.type", default = "default_compression")]
    pub compression_type: String,

    pub topic: String,

    #[serde(rename = "properties.acks", default = "default_acks")]
    pub required_acks: i16,

    #[serde(rename = "properties.ack.timeout.ms", default = "default_ack_timeout")]
    pub ack_timeout_ms: i32,

    #[serde(rename = "properties.batch.size", default = "default_batch_size")]
    pub batch_size: u32,

    #[serde(rename = "properties.linger.ms", default = "default_linger")]
    pub linger_ms: u32,
}

fn default_compression() -> String {
    "none".to_string()
}

fn default_acks() -> i16 {
    1
}

fn default_ack_timeout() -> i32 {
    5000
}

fn default_batch_size() -> u32 {
    16384
}

fn default_linger() -> u32 {
    0
}

impl KafkaSinkConfig {
    pub fn new(
        name: String,
        bootstrap_servers: Vec<String>,
        topic: String,
    ) -> Self {
        Self {
            name,
            bootstrap_servers,
            compression_type: default_compression(),
            topic,
            required_acks: default_acks(),
            ack_timeout_ms: default_ack_timeout(),
            batch_size: default_batch_size(),
            linger_ms: default_linger(),
        }
    }

    pub fn bootstrap_servers_str(&self) -> String {
        self.bootstrap_servers.join(",")
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MySqlSinkConfig {
    pub name: String,

    pub hostname: String,

    pub port: u16,

    pub username: String,

    pub password: String,

    pub database: String,

    #[serde(rename = "pool.max.connections", default = "default_pool_max")]
    pub max_connections: u32,

    #[serde(rename = "connect.timeout.secs", default = "default_conn_timeout")]
    pub connect_timeout_secs: u64,
}

fn default_pool_max() -> u32 {
    10
}

fn default_conn_timeout() -> u64 {
    30
}

impl MySqlSinkConfig {
    pub fn new(
        name: String,
        hostname: String,
        port: u16,
        username: String,
        password: String,
        database: String,
    ) -> Self {
        Self {
            name,
            hostname,
            port,
            username,
            password,
            database,
            max_connections: default_pool_max(),
            connect_timeout_secs: default_conn_timeout(),
        }
    }

    pub fn connection_url(&self) -> String {
        let uri = format!("mysql://{}:{}", self.hostname, self.port);
        let mut url = Url::parse(&uri).unwrap();

        // Set username (URL encoding is handled automatically)
        let _ = url.set_username(&self.username);

        // Set password (URL encoding is handled automatically)
        let _ = url.set_password(Some(&self.password));

        // Add database path
        url.set_path(&format!("/{}", self.database));

        url.to_string()
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct FileSinkConfig {
    pub name: String,

    pub path: String,

    #[serde(default = "default_append")]
    pub append: bool,
}

fn default_append() -> bool {
    true
}

impl FileSinkConfig {
    pub fn new(name: String, path: String) -> Self {
        Self {
            name,
            path,
            append: default_append(),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ConsoleConfig {
    pub name: String,
}

impl ConsoleConfig {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_kafka_sink() {
        let yaml = r#"
type: kafka
name: test_sink
properties.bootstrap.servers:
  - localhost:9092
properties.compression.type: lz4
topic: output-topic
"#;

        let config: KafkaSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.name, "test_sink");
        assert_eq!(config.compression_type, "lz4");
        assert_eq!(config.topic, "output-topic");
    }

    #[test]
    fn test_mysql_connection_url() {
        let config = MySqlSinkConfig {
            name: "test".to_string(),
            hostname: "localhost".to_string(),
            port: 3306,
            username: "root".to_string(),
            password: "p@ss:w0rd".to_string(),
            database: "test_db".to_string(),
            max_connections: 10,
            connect_timeout_secs: 30,
        };

        let url = config.connection_url();
        assert!(url.contains("mysql://"));
        assert!(url.contains("localhost:3306"));
        assert!(url.contains("test_db"));
    }

    #[test]
    fn test_serialize_sink_config() {
        let config = SinkConfig::Console(ConsoleConfig {
            name: "console_sink".to_string(),
        });

        let yaml = serde_yaml::to_string(&config).unwrap();
        println!("{}", yaml);
        assert!(yaml.contains("type: console"));
    }
}
