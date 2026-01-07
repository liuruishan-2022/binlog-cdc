use std::time::Duration;

use serde::{Deserialize, Serialize};

///
/// 放置的都是source的数据配置信息
/// 使用枚举类型来做自动的区分
///
#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type")]
pub enum Source {
    #[serde(rename = "kafka")]
    Kafka(Kafka),
    #[serde(rename = "mysql")]
    Mysql(Mysql),
}

///
/// Kafka作为source的配置
///
#[derive(Deserialize, Serialize, Debug)]
pub struct Kafka {
    name: String,
    bootstrap_server: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Mysql {
    name: String,
    hostname: String,
    port: u32,
    username: String,
    password: String,
    tables: String,
    #[serde(rename = "server-id")]
    server_id: String,

    #[serde(rename = "scan.startup.mode")]
    mode: String,
    #[serde(rename = "scan.startup.specific-offset.file")]
    binlog_filename: Option<String>,
    #[serde(rename = "scan.startup.specific-offset.pos")]
    binlog_offset: Option<u32>,

    #[serde(rename = "scan.startup.timestamp-millis")]
    timestamp_millis: Option<u32>,

    #[serde(rename = "connect.max-retries")]
    connect_max_retries: Option<u32>,
    #[serde(rename = "connect.timeout")]
    connect_timeout: Option<Duration>,
}

impl Mysql {
    pub fn new(
        name: &str,
        hostname: &str,
        port: u32,
        username: &str,
        password: &str,
        tables: &str,
        server_id: &str,
        mode: &str,
    ) -> Self {
        Mysql {
            name: name.to_string(),
            hostname: hostname.to_string(),
            port,
            username: username.to_string(),
            password: password.to_string(),
            tables: tables.to_string(),
            server_id: server_id.to_string(),
            mode: mode.to_string(),
            binlog_filename: None,
            binlog_offset: None,
            timestamp_millis: None,
            connect_max_retries: None,
            connect_timeout: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::source::{Kafka, Source};

    #[test]
    fn test_serialize_source() {
        let kafka = Kafka {
            name: "kafka_source".to_string(),
            bootstrap_server: "localhost:9092".to_string(),
        };

        let kafka = Source::Kafka(kafka);
        let yaml = serde_yaml::to_string(&kafka).unwrap();
        println!("{}", yaml);
    }
}
