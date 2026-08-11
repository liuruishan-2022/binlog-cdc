use std::{fmt::Display, sync::Arc};

use base64::{Engine, engine::general_purpose};
use mysql_binlog_connector_rust::column::column_value::ColumnValue;
use mysql_binlog_connector_rust::event::event_data::EventData;
use mysql_binlog_connector_rust::event::row_event::RowEvent;
use mysql_binlog_connector_rust::event::update_rows_event::UpdateRowsEvent;
use serde_json::Number;
use serde_json::{Map, Value};

use crate::common;
use crate::pipeline::formatter::{DebeziumFormat, MessageKey};
use crate::{mysql::schema::TableMeta, pipeline::formatter::ToDebeziumFormat};
use serde::{Deserialize, Serialize};

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

///
/// 放置解析的部分代码实现
///

impl MysqlBinlogEventRecord {
    pub fn parse_rows(&self, table_meta: &TableMeta) {
        match self.event_data() {
            EventData::WriteRows(event) => {}
            EventData::UpdateRows(event) => {}
            EventData::DeleteRows(event) => {}
            _ => {}
        }
    }

    fn parse_update_rows(&self, table_meta: &TableMeta, event: UpdateRowsEvent) {
        let debezium_formats = event
            .rows
            .into_iter()
            .map(|(before, after)| {
                let before = Some(self.convert_and_parse_row(table_meta, before));
                let after = self.convert_and_parse_row(table_meta, after);
                (before, after)
            })
            .map(|(before, mut after)| {
                let key = self.create_key(table_meta, &after);
                DebeziumFormat::update(
                    before.map(|b| serde_json::json!(b)),
                    serde_json::json!(after),
                    table_meta.db_name(),
                    table_meta.table_name(),
                    key,
                )
            })
            .collect::<Vec<DebeziumFormat>>();
    }

    fn convert_and_parse_row(&self, table_meta: &TableMeta, row: RowEvent) -> Map<String, Value> {
        return row
            .column_values
            .into_iter()
            .enumerate()
            .map(|(index, column)| {
                table_meta.column(index + 1).map(|meta| {
                    let column_name = meta.column_name();
                    let value = self.convert_column_value_to_json(&column);
                    return (column_name.to_string(), value);
                })
            })
            .filter_map(|ele| ele)
            .collect();
    }

    fn convert_column_value_to_json(&self, column_value: &ColumnValue) -> Value {
        match column_value {
            ColumnValue::Tiny(data) => Value::Number(Number::from(*data)),
            ColumnValue::Short(data) => Value::Number(Number::from(*data)),
            ColumnValue::Long(data) => Value::Number(Number::from(*data)),
            ColumnValue::LongLong(data) => Value::Number(Number::from(*data)),
            ColumnValue::Float(data) => Value::Number(Number::from_f64(*data as f64).unwrap()),
            ColumnValue::Double(data) => Value::Number(Number::from_f64(*data).unwrap()),
            ColumnValue::Decimal(data) => Value::String(data.to_string()),
            ColumnValue::Time(data) => Value::String(data.to_string()),
            ColumnValue::Date(data) => Value::String(data.to_string()),
            ColumnValue::DateTime(data) => Value::String(data.to_string()),
            ColumnValue::Timestamp(data) => {
                let time_format = common::format_timestamp(*data);
                Value::String(time_format)
            }
            ColumnValue::Year(data) => Value::Number(Number::from(*data)),
            ColumnValue::String(data) => {
                let data =
                    String::from_utf8(data.clone()).expect("convert data to utf8 string error");
                Value::String(data)
            }
            ColumnValue::Blob(data) => {
                let data = String::from_utf8(data.clone())
                    .unwrap_or_else(|_| general_purpose::STANDARD.encode(data));
                Value::String(data)
            }
            ColumnValue::Bit(data) => Value::Number(Number::from(*data)),
            ColumnValue::Set(data) => Value::Number(Number::from(*data)),
            ColumnValue::Enum(data) => Value::Number(Number::from(*data)),
            ColumnValue::Json(data) => json!(data),
            _ => Value::Null,
        }
    }

    fn create_key(
        &self,
        table_meta: &TableMeta,
        row: &serde_json::Map<String, Value>,
    ) -> MessageKey {
        let column_name = table_meta.primary_column();
        let primary = row.get(column_name).unwrap();
        // 预分配容量为 2（主键 + TableId）
        let mut key = serde_json::Map::with_capacity(2);
        key.insert(column_name.to_string(), primary.clone());
        key.insert(
            "TableId".to_string(),
            serde_json::json!(format!(
                "{}.{}",
                table_meta.db_name(),
                table_meta.table_name()
            )),
        );
        return MessageKey::new(key);
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

impl ToDebeziumFormat for MysqlBinlogEventRecord {
    fn to(&self) -> DebeziumFormat {
        todo!("这块需要参考以前的处理策略")
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
