use std::fmt::{Display, write};

use serde::{Deserialize, Serialize};

use crate::binlog::row::DebeziumFormat;

///
/// 决定采用顶级的enum来处理这种可变的,多变的数据对象信息
///

#[derive(Serialize, Deserialize)]
pub enum PipelineRecord {
    MysqlBinlogStream(DebeziumFormat),
    KafkaDebezium(KafkaDebezium),
    Mysqldump(Mysqldump),
    MysqlBinlogFile(MysqlBinlogFile),
}

impl Display for PipelineRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pipeline record:{}", self)
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

impl Display for Mysqldump {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "data:{} file:{} talbe:{}",
            self.data, self.file, self.table
        )
    }
}
