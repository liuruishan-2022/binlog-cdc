use serde::{Deserialize, Serialize};

///
/// 提供各种类型的sink配置信息
///
#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type")]
pub enum Sink {
    #[serde(rename = "kafka")]
    Kafka(Kafka),
    #[serde(rename = "mysql")]
    Mysql(Mysql),
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Kafka {
    name: String,
    #[serde(rename = "properties.bootstrap.servers")]
    bootstrap_server: String,
}

impl Kafka {
    pub fn new(name: &str, bootstrap_server: &str) -> Self {
        Kafka {
            name: name.to_string(),
            bootstrap_server: bootstrap_server.to_string(),
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Mysql {
    name: String,
    url: String,
}
