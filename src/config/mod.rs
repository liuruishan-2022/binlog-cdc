use serde::{Deserialize, Serialize};

pub mod cdc;
pub mod sink;
pub mod source;

#[derive(Deserialize, Serialize, Debug)]
pub struct CdcConfig {
    source: source::Source,
    sink: sink::Sink,
}

#[cfg(test)]
mod tests {
    use crate::config::{
        CdcConfig,
        sink::{self, Kafka},
        source,
    };
    use serde_yaml;

    #[test]
    fn test_deserialize_config() {
        let yaml_str = r#"source:
  type: mysql
  name: mysql_source
  hostname: localhost
  port: 3306
  username: flink-cdc
  password: flink-cdc@120
  tables: flink_cdc.*
  server-id: 5400-5404
  server-time-zone: UTC
  scan.startup.mode: specific-offset
  scan.startup.specific-offset.file: binlog.000165
  scan.startup.specific-offset.pos: 0

sink:
  type: kafka
  name: Kafka
  properties.bootstrap.servers: 172.16.1.118:9092
  properties.compression.type: lz4
  topic: kafka-default-press"#;
        let config: CdcConfig = serde_yaml::from_str(yaml_str).unwrap();
        println!("{:#?}", config);
    }

    #[test]
    fn test_serialize_config() {
        let config = CdcConfig {
            source: source::Source::Mysql(source::Mysql::new(
                "mysql_source",
                "localhost",
                3306,
                "flink-cdc",
                "flink-cdc@120",
                "flink_cdc.*",
                "5400-5404",
                "specific-offset",
            )),
            sink: sink::Sink::Kafka(Kafka::new("Kafka", "172.16.1.118:9092")),
        };

        let yaml_str = serde_yaml::to_string(&config).unwrap();
        println!("{}", yaml_str);
    }
}
