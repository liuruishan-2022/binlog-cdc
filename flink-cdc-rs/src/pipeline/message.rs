use std::fmt::Display;

use mysql_binlog_connector_rust::event::event_data::EventData;
use serde::{Deserialize, Serialize};

use crate::binlog::row::DebeziumFormat;

///
/// 决定采用顶级的enum来处理这种可变的,多变的数据对象信息
///

#[derive(Serialize, Deserialize)]
pub enum PipelineRecord {
    MysqlDebezium(DebeziumFormat),
    MysqlBinlogEvent(MysqlBinlogEventRecord),
    MysqlBinlogStream(DebeziumFormat),
    KafkaDebezium(KafkaDebezium),
    Mysqldump(Mysqldump),
    MysqlBinlogFile(MysqlBinlogFile),
}

impl PipelineRecord {
    pub fn create_mysqldump(data: DebeziumFormat, file: String, table: String) -> Self {
        let dump = Mysqldump::new(data, file, table);
        return PipelineRecord::Mysqldump(dump);
    }

    pub fn create_mysql_binlog_stream(data: DebeziumFormat) -> Self {
        return PipelineRecord::MysqlBinlogStream(data);
    }

    pub fn create_mysql_debezium(data: DebeziumFormat) -> Self {
        return PipelineRecord::MysqlDebezium(data);
    }

    pub fn create_mysql_binlog_event(binlog: String, key: String, event_data: EventData) -> Self {
        return PipelineRecord::MysqlBinlogEvent(MysqlBinlogEventRecord::new(
            binlog, key, event_data,
        ));
    }
}

impl Display for PipelineRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineRecord::MysqlDebezium(data) => write!(f, "mysql debezium:{}", data),
            PipelineRecord::MysqlBinlogEvent(data) => write!(f, "mysql binlog event:{}", data),
            PipelineRecord::MysqlBinlogStream(data) => write!(f, "mysql binlog stream:{}", data),
            PipelineRecord::KafkaDebezium(data) => write!(f, "kafka debezium:{}", data),
            PipelineRecord::Mysqldump(data) => write!(f, "mysqldump:{}", data),
            PipelineRecord::MysqlBinlogFile(data) => write!(f, "mysql binlog file:{}", data),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct MysqlBinlogEventRecord {
    binlog: String,
    key: String,
    event_data: EventData,
}

impl MysqlBinlogEventRecord {
    pub fn new(binlog: String, key: String, event_data: EventData) -> Self {
        MysqlBinlogEventRecord {
            binlog,
            key,
            event_data,
        }
    }

    pub fn binlog(&self) -> &str {
        self.binlog.as_str()
    }

    pub fn key(&self) -> &str {
        self.key.as_str()
    }

    pub fn event_data(&self) -> &EventData {
        &self.event_data
    }

    pub fn into_event_data(self) -> EventData {
        self.event_data
    }
}

impl Display for MysqlBinlogEventRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "binlog:{} key:{} event:{:?}",
            self.binlog, self.key, self.event_data
        )
    }
}

///
/// 定义Kafak的消息结构,但是我们可能定义多种数据结构，因为不清楚从Kafka消费到什么类型的消息
///
#[derive(Serialize, Deserialize)]
pub struct KafkaDebezium {
    data: DebeziumFormat,
    topic: String,
}

impl KafkaDebezium {
    pub fn data(&self) -> &DebeziumFormat {
        &self.data
    }

    pub fn topic(&self) -> &str {
        self.topic.as_str()
    }
}

impl Display for KafkaDebezium {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "data:{} topic:{}", self.data, self.topic)
    }
}

#[derive(Serialize, Deserialize)]
pub struct MysqlBinlogFile {
    data: DebeziumFormat,
    file: String,
    table: String,
    table_id: String,
    database: String,
}

impl Display for MysqlBinlogFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "data:{} file:{} table:{} table_id:{} database:{}",
            self.data, self.file, self.table, self.table_id, self.database
        )
    }
}

#[derive(Serialize, Deserialize)]
pub struct Mysqldump {
    data: DebeziumFormat,
    file: String,
    table: String,
}

impl Mysqldump {
    pub fn new(data: DebeziumFormat, file: String, table: String) -> Self {
        Mysqldump {
            data: data,
            file: file,
            table: table,
        }
    }
}

impl Display for Mysqldump {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "data:{} file:{} talbe:{}",
            self.data, self.file, self.table
        )
    }
}
