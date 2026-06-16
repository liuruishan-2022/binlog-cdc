use std::{fmt::Display, sync::Arc};

use mysql_binlog_connector_rust::event::event_data::EventData;
use serde::{Deserialize, Serialize};

use crate::{binlog::row::DebeziumFormat, mysql::schema::TableMeta};

///
/// 决定采用顶级的enum来处理这种可变的,多变的数据对象信息
///

pub enum PipelineRecord {
    MysqlDebezium(DebeziumFormat),
    MysqlBinlogEvent(MysqlBinlogEventRecord),
    MysqlBinlogStream(DebeziumFormat),
    RocketmqDebezium(RocketmqDebezium),
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

    pub fn create_mysql_binlog_event(
        binlog: String,
        key: String,
        table_meta: Arc<TableMeta>,
        event_data: EventData,
    ) -> Self {
        return PipelineRecord::MysqlBinlogEvent(MysqlBinlogEventRecord::new(
            binlog, key, table_meta, event_data,
        ));
    }
}

impl Display for PipelineRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineRecord::MysqlDebezium(data) => write!(f, "mysql debezium:{}", data),
            PipelineRecord::MysqlBinlogEvent(data) => write!(f, "mysql binlog event:{}", data),
            PipelineRecord::MysqlBinlogStream(data) => write!(f, "mysql binlog stream:{}", data),
            PipelineRecord::RocketmqDebezium(data) => write!(f, "rocketmq debezium:{}", data),
            PipelineRecord::KafkaDebezium(data) => write!(f, "kafka debezium:{}", data),
            PipelineRecord::Mysqldump(data) => write!(f, "mysqldump:{}", data),
            PipelineRecord::MysqlBinlogFile(data) => write!(f, "mysql binlog file:{}", data),
        }
    }
}

pub struct MysqlBinlogEventRecord {
    binlog: String,
    key: String,
    table_meta: Arc<TableMeta>,
    event_data: EventData,
}

impl MysqlBinlogEventRecord {
    pub fn new(
        binlog: String,
        key: String,
        table_meta: Arc<TableMeta>,
        event_data: EventData,
    ) -> Self {
        MysqlBinlogEventRecord {
            binlog,
            key,
            table_meta,
            event_data,
        }
    }

    pub fn binlog(&self) -> &str {
        self.binlog.as_str()
    }

    pub fn key(&self) -> &str {
        self.key.as_str()
    }

    pub fn table_meta(&self) -> Arc<TableMeta> {
        self.table_meta.clone()
    }

    pub fn table_id(&self) -> u64 {
        self.table_meta.table_id()
    }

    pub fn db_name(&self) -> &str {
        self.table_meta.db_name()
    }

    pub fn table_name(&self) -> &str {
        self.table_meta.table_name()
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
#[derive(Serialize, Deserialize)]
pub struct RocketmqDebezium {
    data: DebeziumFormat,
    topic: String,
    msg_id: String,
}

impl RocketmqDebezium {
    pub fn new(data: DebeziumFormat, topic: String, msg_id: String) -> Self {
        Self {
            data,
            topic,
            msg_id,
        }
    }

    pub fn data(&self) -> &DebeziumFormat {
        &self.data
    }

    pub fn into_data(self) -> DebeziumFormat {
        self.data
    }

    pub fn topic(&self) -> &str {
        self.topic.as_str()
    }

    pub fn msg_id(&self) -> &str {
        self.msg_id.as_str()
    }
}

impl Display for RocketmqDebezium {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "data:{} topic:{} msg_id:{}",
            self.data, self.topic, self.msg_id
        )
    }
}

///
#[derive(Serialize, Deserialize)]
pub struct KafkaDebezium {
    data: DebeziumFormat,
    topic: String,
}

impl KafkaDebezium {
    pub fn new(data: DebeziumFormat, topic: String) -> Self {
        Self { data, topic }
    }

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
