//! Configuration module for source-sink
//!
//! Provides YAML-based configuration loading for pipelines, sources, and sinks.

pub mod source;
pub mod sink;
pub mod pipeline;

use std::fs;
use std::path::Path;
use serde::{Deserialize, Serialize};

use crate::config::pipeline::Pipeline;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Route {
    #[serde(rename = "source-table")]
    pub source_table: String,

    #[serde(rename = "sink-table")]
    pub sink_table: String,
}

impl Route {
    pub fn new(source_table: String, sink_table: String) -> Self {
        Self {
            source_table,
            sink_table,
        }
    }

    pub fn source_table(&self) -> &str {
        &self.source_table
    }

    pub fn sink_table(&self) -> &str {
        &self.sink_table
    }

    pub fn source_database(&self) -> Option<&str> {
        self.source_table.split('.').next()
    }

    pub fn source_table_name(&self) -> &str {
        self.source_table
            .split('.')
            .last()
            .unwrap_or(&self.source_table)
    }
}

pub fn load_config<P: AsRef<Path>>(config_path: P) -> Config {
    let content = fs::read_to_string(config_path)
        .expect("Unable to read config file");
    serde_yaml::from_str(&content)
        .expect("Unable to parse config file")
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Config {
    pub source: source::SourceConfig,

    pub sink: sink::SinkConfig,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<Pipeline>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub route: Option<Vec<Route>>,
}

impl Config {
    pub fn new(
        source: source::SourceConfig,
        sink: sink::SinkConfig,
        pipeline: Option<Pipeline>,
        route: Option<Vec<Route>>,
    ) -> Self {
        Self {
            source,
            sink,
            pipeline,
            route,
        }
    }

    pub fn source(&self) -> &source::SourceConfig {
        &self.source
    }

    pub fn sink(&self) -> &sink::SinkConfig {
        &self.sink
    }

    pub fn pipeline(&self) -> Option<&Pipeline> {
        self.pipeline.as_ref()
    }

    pub fn route(&self) -> Option<&Vec<Route>> {
        self.route.as_ref()
    }

    pub fn has_pipeline(&self) -> bool {
        self.pipeline.is_some()
    }

    pub fn has_route(&self) -> bool {
        self.route.is_some() && !self.route.as_ref().unwrap().is_empty()
    }

    pub fn find_sink_table(&self, source_table: &str) -> Option<String> {
        self.route.as_ref()?.iter().find_map(|route| {
            if route.source_table == source_table {
                Some(route.sink_table.clone())
            } else {
                None
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{source, sink};

    #[test]
    fn test_deserialize_config() {
        let yaml_str = r#"
source:
  type: kafka
  name: kafka_source
  properties.bootstrap.servers: localhost:9092
  properties.group.id: test-group
  topic: test-topic
  partition: 0

sink:
  type: mysql
  name: mysql_sink
  hostname: localhost
  port: 3306
  username: root
  password: password
  database: test_db

pipeline:
  name: Test Pipeline
  parallelism: 2
"#;

        let config: Config = serde_yaml::from_str(yaml_str).unwrap();
        println!("{:#?}", config);

        assert!(config.has_pipeline());
        assert_eq!(config.pipeline().unwrap().name(), "Test Pipeline");
        assert!(!config.has_route());
    }

    #[test]
    fn test_deserialize_config_with_route() {
        let yaml_str = r#"
source:
  type: mysql
  name: mysql_source
  connection.url: mysql://root:password@localhost:3306/app_db
  query: SELECT * FROM orders
  poll.interval.secs: 5

sink:
  type: kafka
  name: kafka_sink
  properties.bootstrap.servers: localhost:9092
  topic: output-topic

pipeline:
  name: MySQL to Kafka Pipeline
  parallelism: 1

route:
  - source-table: app_db.orders
    sink-table: kafka_ods_orders
  - source-table: app_db.products
    sink-table: kafka_ods_products
"#;

        let config: Config = serde_yaml::from_str(yaml_str).unwrap();
        println!("{:#?}", config);

        assert!(config.has_route());
        assert_eq!(config.route().unwrap().len(), 2);

        // Test finding sink table
        let sink_table = config.find_sink_table("app_db.orders");
        assert_eq!(sink_table, Some("kafka_ods_orders".to_string()));

        let sink_table = config.find_sink_table("app_db.products");
        assert_eq!(sink_table, Some("kafka_ods_products".to_string()));

        let sink_table = config.find_sink_table("nonexistent");
        assert_eq!(sink_table, None);
    }

    #[test]
    fn test_route_methods() {
        let route = Route::new("app_db.orders".to_string(), "kafka_ods_orders".to_string());
        assert_eq!(route.source_table(), "app_db.orders");
        assert_eq!(route.sink_table(), "kafka_ods_orders");
        assert_eq!(route.source_database(), Some("app_db"));
        assert_eq!(route.source_table_name(), "orders");

        let route2 = Route::new("orders".to_string(), "ods_orders".to_string());
        assert_eq!(route2.source_database(), None);
        assert_eq!(route2.source_table_name(), "orders");
    }

    #[test]
    fn test_serialize_config() {
        let config = Config::new(
            source::SourceConfig::Kafka(source::KafkaSourceConfig {
                name: "kafka_source".to_string(),
                bootstrap_servers: vec!["localhost:9092".to_string()],
                group_id: "test-group".to_string(),
                topic: "test-topic".to_string(),
                partition: 0,
                start_offset: "latest".to_string(),
            }),
            sink::SinkConfig::Console(sink::ConsoleConfig {
                name: "console_sink".to_string(),
            }),
            Some(Pipeline::new("Test Pipeline", 2)),
            None,
        );

        let yaml_str = serde_yaml::to_string(&config).unwrap();
        println!("{}", yaml_str);
    }

    #[test]
    fn test_serialize_config_with_route() {
        let routes = vec![
            Route::new("app_db.orders".to_string(), "kafka_ods_orders".to_string()),
            Route::new("app_db.products".to_string(), "kafka_ods_products".to_string()),
        ];

        let config = Config::new(
            source::SourceConfig::Console(source::ConsoleSourceConfig {
                name: "console_source".to_string(),
            }),
            sink::SinkConfig::Console(sink::ConsoleConfig {
                name: "console_sink".to_string(),
            }),
            Some(Pipeline::new("Test Pipeline", 1)),
            Some(routes),
        );

        let yaml_str = serde_yaml::to_string(&config).unwrap();
        println!("{}", yaml_str);
        assert!(yaml_str.contains("source-table: app_db.orders"));
        assert!(yaml_str.contains("sink-table: kafka_ods_orders"));
    }
}
