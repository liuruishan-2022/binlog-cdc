use std::collections::HashMap;

use regex::Regex;
use serde::{Deserialize, Serialize};
use url::Url;

///
/// 放置配置CDC的配置文件解析的地方，兼容flink cdc 3的yaml配置格式，获取自己需要的配置信息
///
#[derive(Deserialize, Serialize, Debug)]
pub struct FlinkCdc {
    source: Source,
    sink: Sink,
    transform: Option<Vec<Transform>>,
}

impl FlinkCdc {
    ///
    /// 读取配置文件的时候,需要进行一个校验的工作，否则有些参数会填写错误,或者是没有填写
    pub fn read_from(config_path: &str) -> Self {
        let config_file = std::fs::read_to_string(config_path).expect("Unable to read config file");

        let config: FlinkCdc = serde_yaml::from_str(&config_file)
            .expect("Unable to parse config file to pub struct FlinkCDC");
        config.source.validate();
        return config;
    }

    pub fn source_url(&self) -> String {
        self.source.url()
    }

    pub fn source_username(&self) -> &str {
        self.source.username()
    }

    pub fn source_password(&self) -> &str {
        self.source.password()
    }

    pub fn source_server_id(&self) -> u64 {
        self.source.server_id()
    }

    pub fn source_binlog_file(&self) -> String {
        self.source.binlog_filename().to_string()
    }

    pub fn source_binlog_offset(&self) -> u32 {
        self.source.binlog_offset()
    }

    pub fn source_table_include(&self) -> TableInclude {
        self.source.tables_include()
    }

    ///
    /// 获取sink相关的信息配置
    ///
    pub fn sink_name(&self) -> &str {
        self.sink.name()
    }

    pub fn sink_bootstrap_server(&self) -> &str {
        self.sink.bootstrap_server()
    }

    pub fn sink_compression_type(&self) -> &str {
        self.sink.compression_type()
    }

    pub fn sink_topic(&self) -> &str {
        self.sink.topic()
    }

    pub fn sink_batch_size(&self) -> u32 {
        self.sink.batch_size()
    }

    pub fn sink_linger_ms(&self) -> u32 {
        self.sink.linger_ms()
    }

    pub fn transorms(&self) -> Option<&Vec<Transform>> {
        self.transform.as_ref()
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Source {
    #[serde(rename = "type")]
    type_name: String,
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
}

impl Source {
    pub fn hostname(&self) -> &str {
        return self.hostname.as_str();
    }

    pub fn port(&self) -> u32 {
        return self.port;
    }

    pub fn username(&self) -> &str {
        return self.username.as_str();
    }

    pub fn password(&self) -> &str {
        return self.password.as_str();
    }

    pub fn binlog_filename(&self) -> &str {
        return self
            .binlog_filename
            .as_ref()
            .expect("error of fetch scan.startup.specific-offset.file");
    }

    pub fn binlog_offset(&self) -> u32 {
        return self
            .binlog_offset
            .expect("error of fetch scan.startup.specific-offset.pos");
    }

    pub fn timestamp_millis(&self) -> Option<&u32> {
        return self.timestamp_millis.as_ref();
    }

    pub fn tables_include(&self) -> TableInclude {
        return TableInclude::create(&self.tables);
    }

    pub fn server_id(&self) -> u64 {
        return self
            .server_id
            .split_once('-')
            .map(|(prefix, _)| prefix.parse::<u64>())
            .unwrap_or_else(|| self.server_id.parse())
            .expect(format!("parse server-id:{} error", self.server_id.as_str()).as_str());
    }

    ///
    /// 当密码当中含有:#这类特殊字符的时候，sqlx会报错: Configuration(InvalidPort)
    /// 所以需要使用Uri来处理
    pub fn url(&self) -> String {
        let uri = format!("mysql://{}:{}", self.hostname, self.port);
        let mut uri = Url::parse(&uri).unwrap();
        let _ = uri.set_username(self.username());
        let _ = uri.set_password(Some(self.password()));

        return uri.as_str().to_string();
    }

    ///
    /// 需要校验，就是如果配置不同的mode,那么就需要对应配置不同的配置信息
    ///
    pub fn validate(&self) {
        match self.mode.as_str() {
            "specific-offset" => {
                if self.binlog_filename.is_none() || self.binlog_offset.is_none() {
                    panic!(
                        "when scan.startup.mode is specific-offset, must config scan.startup.specific-offset.file and scan.startup.specific-offset.pos"
                    );
                }
            }
            "timestamp" => {
                if self.timestamp_millis.is_none() {
                    panic!(
                        "when scan.startup.mode is timestamp, must config scan.startup.timestamp-millis"
                    );
                }
            }
            _ => {
                panic!("unsupported scan.startup.mode:{}", self.mode.as_str());
            }
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Sink {
    #[serde(rename = "type")]
    type_name: String,
    name: String,
    #[serde(rename = "properties.bootstrap.servers")]
    bootstrap_server: String,
    #[serde(rename = "properties.compression.type")]
    compression_type: String,
    topic: String,

    #[serde(rename = "properties.batch.size")]
    batch_size: Option<u32>,
    #[serde(rename = "properties.linger.ms")]
    linger_ms: Option<u32>,
}

impl Sink {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn bootstrap_server(&self) -> &str {
        &self.bootstrap_server
    }

    pub fn compression_type(&self) -> &str {
        &self.compression_type
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn batch_size(&self) -> u32 {
        self.batch_size.unwrap_or(10000)
    }

    pub fn linger_ms(&self) -> u32 {
        self.linger_ms.unwrap_or(100)
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Transform {
    #[serde(rename = "source-table")]
    source_table: String,
    projection: String,
}

impl Transform {
    pub fn new(source_table: &str, projection: &str) -> Self {
        return Transform {
            source_table: source_table.to_string(),
            projection: projection.to_string(),
        };
    }

    pub fn source_table(&self) -> &str {
        &self.source_table
    }

    pub fn projection(&self) -> &str {
        &self.projection
    }
}

///
/// 下面不是属于flink cdc规范对应的struct了
#[derive(Debug)]
pub struct TableInclude {
    includes: HashMap<String, Vec<Regex>>,
}

impl TableInclude {
    pub fn create(include_tables: &str) -> Self {
        let includes = include_tables
            .split(',')
            .map(|s| s.to_string())
            .collect::<Vec<String>>();

        let includes = includes
            .iter()
            .filter_map(|ele| {
                let mut parts = ele.splitn(2, '.');
                match (parts.next(), parts.next()) {
                    (Some(key), Some(value)) => Some((key.to_string(), value.to_string())),
                    _ => None,
                }
            })
            .fold(HashMap::new(), |mut map, (key, value)| {
                let regexs = map.entry(key.to_string()).or_insert(vec![]);
                let regex = format!("^{}$", value.as_str());
                let regex = Regex::new(&regex)
                    .expect(format!("regex value:{} error!", value.as_str()).as_str());
                regexs.push(regex);
                map
            });

        return TableInclude { includes: includes };
    }

    //
    //提供过滤的判断函数
    pub fn can_include(&self, db_name: &str, table_name: &str) -> bool {
        let value = self.includes.get(db_name);
        return value
            .map(|eles| -> bool {
                let result = eles.iter().any(|ele| ele.is_match(table_name));
                return result;
            })
            .unwrap_or(false);
    }

    pub fn can_exclude(&self, db_name: &str, table_name: &str) -> bool {
        return !self.can_include(db_name, table_name);
    }
}

#[cfg(test)]
mod tests {

    use crate::LocalTimer;

    fn init() {
        tracing_subscriber::fmt().with_timer(LocalTimer).init();
    }
}
