use base64::{Engine, engine::general_purpose};
use chrono::{Local, TimeZone, offset::LocalResult};
use mysql_binlog_connector_rust::{
    column::column_value::ColumnValue,
    event::{
        delete_rows_event::DeleteRowsEvent, row_event::RowEvent,
        update_rows_event::UpdateRowsEvent, write_rows_event::WriteRowsEvent,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tracing::warn;

use crate::{
    binlog::{Metrics, schema::TableMeta},
    config::cdc::FlinkCdc,
    sink::kafka_sink::{KafkaSink, RskafkaSink},
    transform::parser::ProjectionHandler,
};

///
/// 处理Row相关类型的事件的
///

pub struct RowEventHandler<'a> {
    kafka_sink: KafkaSink,
    metrics: &'a Metrics,
    projection: ProjectionHandler,
    rskafka_sink: RskafkaSink,
}

impl<'a> RowEventHandler<'a> {
    pub async fn build(config: &'a FlinkCdc, metrics: &'a Metrics) -> Self {
        let rskafka_sink = RskafkaSink::create(config).await;
        RowEventHandler {
            kafka_sink: KafkaSink::build(config),
            metrics: metrics,
            projection: ProjectionHandler::create(config.transforms()),
            rskafka_sink: rskafka_sink,
        }
    }

    pub async fn handle_write_event(&self, table_meta: Option<&TableMeta>, event: WriteRowsEvent) {
        if let Some(table_meta) = table_meta {
            let mut rows = self.parse_rows(table_meta, event.rows);
            let debeziums = rows
                .iter_mut()
                .map(|map| {
                    self.projection
                        .eval(&table_meta.qualified_table_name(), map);
                    return map;
                })
                .map(|after| {
                    DebeziumFormat::insert(
                        json!(after),
                        table_meta.db_name(),
                        table_meta.table_name(),
                        self.create_key(table_meta, &after),
                    )
                })
                .collect::<Vec<DebeziumFormat>>();
            self.metrics
                .inc_flink_sink_kafka_message("create", debeziums.len() as u64);
            self.send_to_kafka(debeziums).await;
        } else {
            warn!("table meta is not exist! table id:{}", event.table_id);
        }
    }

    pub async fn handle_update_event(
        &self,
        table_meta: Option<&TableMeta>,
        event: UpdateRowsEvent,
    ) {
        if let Some(table_meta) = table_meta {
            let debezium = event
                .rows
                .into_iter()
                .map(|(before_row, after_row)| {
                    return (
                        self.convert_and_parse_row(table_meta, before_row),
                        self.convert_and_parse_row(table_meta, after_row),
                    );
                })
                .into_iter()
                .map(|(before, mut after)| {
                    self.projection
                        .eval(&table_meta.qualified_table_name(), &mut after);
                    DebeziumFormat::update(
                        json!(before),
                        json!(after),
                        table_meta.db_name(),
                        table_meta.table_name(),
                        self.create_key(table_meta, &before),
                    )
                })
                .collect::<Vec<DebeziumFormat>>();
            self.metrics
                .inc_flink_sink_kafka_message("update", debezium.len() as u64);
            self.send_to_kafka(debezium).await;
        }
    }

    pub async fn handle_delete_event(
        &self,
        table_meta: Option<&TableMeta>,
        event: DeleteRowsEvent,
    ) {
        if let Some(table_meta) = table_meta {
            let debezium = event
                .rows
                .into_iter()
                .map(|row| self.convert_and_parse_row(table_meta, row))
                .into_iter()
                .map(|before| {
                    DebeziumFormat::delete(
                        json!(before),
                        table_meta.db_name(),
                        table_meta.table_name(),
                        self.create_key(table_meta, &before),
                    )
                })
                .collect::<Vec<DebeziumFormat>>();
            self.metrics
                .inc_flink_sink_kafka_message("delete", debezium.len() as u64);
            self.send_to_kafka(debezium).await;
        }
    }

    async fn send_to_kafka(&self, debezium: Vec<DebeziumFormat>) {
        // 根据环境变量 RSKAFKA 决定使用哪个 sink
        // 如果设置了环境变量 RSKAFKA，则使用 rskafka_sink
        // 否则使用传统的 kafka_sink (rdkafka)
        if std::env::var("RSKAFKA").is_ok() {
            self.rskafka_sink.send_messages(debezium).await;
        } else {
            self.kafka_sink.send_batch_messages(debezium).await;
        }
    }

    fn parse_rows(&self, table_meta: &TableMeta, rows: Vec<RowEvent>) -> Vec<Map<String, Value>> {
        return rows
            .into_iter()
            .map(|row| self.convert_and_parse_row(table_meta, row))
            .collect::<Vec<Map<String, Value>>>();
    }

    fn create_key(&self, table_meta: &TableMeta, row: &Map<String, Value>) -> MessageKey {
        let column_name = table_meta.primary_column();
        let primary = row.get(column_name).unwrap();
        let mut key = Map::new();
        key.insert(column_name.to_string(), primary.clone());
        key.insert(
            "TableId".to_string(),
            json!(format!(
                "{}.{}",
                table_meta.db_name(),
                table_meta.table_name()
            )),
        );
        return MessageKey::new(key);
    }

    //
    // 这个需要携带primary key回来
    fn convert_and_parse_row(&self, table_meta: &TableMeta, row: RowEvent) -> Map<String, Value> {
        let mut position: usize = 1;
        let mut row_map = serde_json::Map::new();
        row.column_values.into_iter().for_each(|column_value| {
            if let Some(column) = table_meta.column(position) {
                let column_name = column.column_name();
                let value: Value = match column_value {
                    ColumnValue::Tiny(data) => {
                        json!(data)
                    }
                    ColumnValue::Short(data) => {
                        json!(data)
                    }
                    ColumnValue::Long(data) => {
                        json!(data)
                    }
                    ColumnValue::LongLong(data) => {
                        json!(data)
                    }
                    ColumnValue::Float(data) => {
                        json!(data)
                    }
                    ColumnValue::Double(data) => {
                        json!(data)
                    }
                    ColumnValue::Decimal(data) => {
                        json!(data)
                    }
                    ColumnValue::Time(data) => {
                        json!(data)
                    }
                    ColumnValue::Date(data) => {
                        json!(data)
                    }
                    ColumnValue::DateTime(data) => {
                        json!(data)
                    }
                    ColumnValue::Timestamp(data) => {
                        let time_format = format_timestamp(data);
                        json!(time_format)
                    }
                    ColumnValue::Year(data) => {
                        json!(data)
                    }
                    ColumnValue::String(data) => {
                        let data =
                            String::from_utf8(data).expect("convert data to utf8 string error");
                        json!(data)
                    }
                    ColumnValue::Blob(data) => {
                        let data = String::from_utf8(data.clone())
                            .unwrap_or_else(|_| general_purpose::STANDARD.encode(data));
                        json!(data)
                    }
                    ColumnValue::Bit(data) => {
                        json!(data)
                    }
                    ColumnValue::Set(data) => {
                        json!(data)
                    }
                    ColumnValue::Enum(data) => {
                        json!(data)
                    }
                    ColumnValue::Json(data) => {
                        json!(data)
                    }
                    _ => Value::Null,
                };
                row_map.insert(column_name.to_string(), value);
            }
            position = position + 1;
        });
        return row_map;
    }
}

///
/// Debezium的JSON格式的数据结构对象
///
#[derive(Serialize, Debug, Deserialize)]
pub struct DebeziumFormat {
    before: Option<Value>,
    after: Option<Value>,
    op: String,
    source: DebeziumSource,
    #[serde(skip)]
    key: MessageKey,
}

impl DebeziumFormat {
    pub fn insert(after: Value, db: &str, table: &str, key: MessageKey) -> Self {
        DebeziumFormat {
            before: None,
            after: Some(after),
            op: "c".to_string(),
            source: DebeziumSource {
                db: db.to_string(),
                table: table.to_string(),
            },
            key: key,
        }
    }

    pub fn update(before: Value, after: Value, db: &str, table: &str, key: MessageKey) -> Self {
        DebeziumFormat {
            before: Some(before),
            after: Some(after),
            op: "u".to_string(),
            source: DebeziumSource {
                db: db.to_string(),
                table: table.to_string(),
            },
            key: key,
        }
    }

    pub fn delete(before: Value, db: &str, table: &str, key: MessageKey) -> Self {
        DebeziumFormat {
            before: Some(before),
            after: None,
            op: "d".to_string(),
            source: DebeziumSource {
                db: db.to_string(),
                table: table.to_string(),
            },
            key: key,
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self)
            .expect(format!("serialization debezium to json error!").as_str())
    }

    pub fn keys(&self) -> String {
        self.key.keys_json()
    }

    pub fn op(&self) -> &str {
        self.op.as_str()
    }

    pub fn before_column(&self, column_name: &str) -> Option<&Value> {
        if let Some(before) = &self.before {
            return before.get(column_name);
        }
        return None;
    }

    pub fn after_column(&self, column_name: &str) -> Option<&Value> {
        if let Some(after) = &self.after {
            return after.get(column_name);
        }
        return None;
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct DebeziumSource {
    db: String,
    table: String,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct MessageKey {
    keys: Map<String, Value>,
}

impl MessageKey {
    pub fn new(keys: Map<String, Value>) -> Self {
        MessageKey { keys }
    }

    pub fn keys_json(&self) -> String {
        serde_json::to_string(&self.keys).unwrap()
    }
}

pub fn format_timestamp(timestamp: i64) -> String {
    let millis = timestamp / 1000;
    match Local.timestamp_millis_opt(millis) {
        LocalResult::Single(time) => time.format("%Y-%m-%d %H:%M:%S").to_string(),
        _ => {
            warn!("timestamp is invalid:{}!", timestamp);
            "".to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::LocalTimer;

    use super::*;
    use base64::{Engine, engine::general_purpose};
    use rust_decimal::prelude::*;
    use tracing::info;

    fn init() {
        tracing_subscriber::fmt().with_timer(LocalTimer).init();
    }

    #[test]
    fn test_decimal() {
        init();
        let source = "0.0900";
        let price = Decimal::from_str(source).unwrap();
        info!("查看serde的翻译:{}", serde_json::to_string(&price).unwrap());
    }

    #[test]
    fn test_parse_bytes() {
        init();

        let content: Vec<u8> = vec![
            31, 139, 8, 0, 0, 0, 0, 0, 0, 0, 227, 58, 196, 196, 197, 109, 104, 97, 108, 98, 108, 6,
            132, 6, 70, 66, 51, 152, 132, 166, 50, 61, 110, 152, 144, 92, 144, 152, 88, 252, 184,
            97, 226, 243, 190, 150, 103, 107, 151, 61, 95, 62, 241, 89, 87, 195, 211, 13, 93, 207,
            166, 174, 121, 62, 171, 229, 197, 186, 253, 79, 215, 45, 122, 191, 167, 231, 197, 250,
            237, 79, 247, 239, 125, 62, 117, 41, 156, 81, 94, 94, 174, 151, 148, 152, 153, 82, 170,
            151, 156, 159, 171, 95, 146, 90, 92, 2, 84, 246, 180, 173, 245, 233, 186, 157, 79, 39,
            172, 126, 222, 185, 243, 217, 186, 174, 231, 107, 150, 61, 237, 223, 174, 150, 154,
            155, 159, 149, 249, 98, 225, 138, 103, 205, 173, 239, 247, 52, 190, 92, 5, 52, 173,
            241, 249, 130, 70, 7, 229, 247, 251, 151, 170, 62, 106, 88, 6, 68, 106, 90, 239, 247,
            116, 188, 223, 211, 249, 168, 97, 10, 16, 105, 191, 223, 51, 235, 81, 195, 156, 199,
            13, 93, 143, 27, 186, 223, 239, 153, 175, 255, 184, 161, 9, 104, 250, 163, 134, 25,
            239, 247, 204, 6, 50, 242, 158, 109, 237, 126, 177, 126, 42, 196, 54, 184, 139, 128,
            18, 184, 196, 63, 204, 159, 209, 4, 196, 205, 143, 102, 44, 248, 48, 127, 82, 195, 135,
            249, 19, 247, 1, 241, 206, 167, 93, 243, 158, 79, 104, 123, 214, 183, 244, 233, 190,
            214, 103, 157, 221, 207, 119, 175, 181, 180, 48, 48, 147, 226, 77, 78, 78, 49, 4, 3,
            35, 99, 19, 35, 37, 6, 13, 6, 3, 198, 0, 134, 14, 70, 166, 209, 64, 195, 25, 104, 0,
            106, 204, 177, 183, 96, 2, 0, 0,
        ];

        let result = general_purpose::STANDARD.encode(content);
        info!("decoded content: {:?}", result);
    }

    #[test]
    fn test_format_timestamp() {
        init();
        let timestamp: i64 = 1763976387000000;
        info!("timestamp: {}", format_timestamp(timestamp));
    }
}
