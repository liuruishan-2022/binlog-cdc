use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
};

use base64::{Engine, engine::general_purpose};
use chrono::{Local, TimeZone, offset::LocalResult};
use mysql_binlog_connector_rust::{
    binlog_client::BinlogClient,
    binlog_error::BinlogError,
    column::column_value::ColumnValue,
    event::{
        delete_rows_event::DeleteRowsEvent, event_data::EventData, row_event::RowEvent,
        table_map_event::TableMapEvent, update_rows_event::UpdateRowsEvent,
        write_rows_event::WriteRowsEvent,
    },
};
use serde_json::{Map, Number, Value, json};
use tracing::{info, warn};

use crate::{
    binlog::row::{DebeziumFormat, MessageKey},
    binlog::schema::{TableMeta, TableSchema},
    config::{
        CdcConfig,
        source::{Mysql, Source},
    },
    savepoint::{SavePoints, local::LocalFileSystem},
};

pub struct MysqlSource<'a> {
    source: &'a Mysql,
    channels: Vec<crossbeam_channel::Sender<PipelineRecord>>,
    resolver: RouteResolver,
    current_binlog: String,
    table_schema: TableSchema,
    table_meta_cache: HashMap<String, HashMap<u64, TableMeta>>,
}

impl<'a> MysqlSource<'a> {
    pub async fn create(
        cdc: &'a CdcConfig,
        channels: Vec<crossbeam_channel::Sender<PipelineRecord>>,
        resolver: RouteResolver,
    ) -> Self {
        let source = match cdc.source() {
            Source::Mysql(source) => source,
            _ => panic!("mysql source need mysql config"),
        };
        let table_schema = TableSchema::new(&source.url())
            .await
            .expect("create mysql table schema error");

        Self {
            source,
            channels,
            resolver,
            current_binlog: String::new(),
            table_schema,
            table_meta_cache: HashMap::new(),
        }
    }

    pub async fn read(&mut self) {
        let savepoint = LocalFileSystem::default();
        let binlog_file = savepoint
            .load()
            .unwrap_or_else(|| self.source.binlog_filename());
        let mut stream = self.binlog_stream(binlog_file).await;

        loop {
            match stream.read().await {
                Ok((header, data)) => {
                    info!("read mysql binlog event timestamp:{}", header.timestamp);
                    match data {
                        EventData::Rotate(event) => {
                            info!("read new binlog:{}", event.binlog_filename);
                            self.current_binlog = event.binlog_filename.clone();
                            savepoint.save(&event.binlog_filename);
                        }
                        EventData::TableMap(event) => {
                            self.record_table_meta(event).await;
                        }
                        EventData::WriteRows(event) => {
                            self.handle_write_rows(event);
                        }
                        EventData::UpdateRows(event) => {
                            self.handle_update_rows(event);
                        }
                        EventData::DeleteRows(event) => {
                            self.handle_delete_rows(event);
                        }
                        _ => {}
                    }
                }
                Err(BinlogError::IoError(err)) => {
                    warn!("read binlog io error:{:?}", err);
                    break;
                }
                Err(BinlogError::UnexpectedData(err)) => {
                    warn!("read binlog unexpected error:{}", err);
                    break;
                }
                Err(err) => {
                    warn!("read mysql binlog error:{:?}", err);
                    break;
                }
            }
        }
    }

    async fn binlog_stream(
        &self,
        binlog_file: String,
    ) -> mysql_binlog_connector_rust::binlog_stream::BinlogStream {
        let mut client = BinlogClient {
            url: self.source.url(),
            server_id: self.source.server_id(),
            binlog_filename: binlog_file,
            binlog_position: self.source.binlog_offset(),
            gtid_enabled: false,
            gtid_set: String::new(),
            heartbeat_interval_secs: 10,
            timeout_secs: self.source.connect_timeout().as_secs(),
            keepalive_idle_secs: 60,
            keepalive_interval_secs: 60,
        };

        client
            .connect()
            .await
            .expect("connect to mysql read binlog file error")
    }

    async fn record_table_meta(&mut self, event: TableMapEvent) {
        if self.can_exclude(&event.database_name, &event.table_name) {
            return;
        }

        let cache = self
            .table_meta_cache
            .entry(self.current_binlog.clone())
            .or_default();
        if cache.contains_key(&event.table_id) {
            return;
        }

        info!("cache binlog table meta information:{}", event.table_id);
        if let Some(meta) = self
            .table_schema
            .desc_table(event.table_id, &event.database_name, &event.table_name)
            .await
        {
            cache.insert(event.table_id, meta);
        } else {
            warn!(
                "failed to get table meta for {}.{}",
                event.database_name, event.table_name
            );
        }
    }

    fn handle_write_rows(&self, event: WriteRowsEvent) {
        if let Some(table_meta) = self.table_meta(event.table_id) {
            for debezium in MysqlRowEventHandler::parse_write_rows(table_meta, event) {
                self.send_debezium(debezium);
            }
        }
    }

    fn handle_update_rows(&self, event: UpdateRowsEvent) {
        if let Some(table_meta) = self.table_meta(event.table_id) {
            for debezium in MysqlRowEventHandler::parse_update_rows(table_meta, event) {
                self.send_debezium(debezium);
            }
        }
    }

    fn handle_delete_rows(&self, event: DeleteRowsEvent) {
        if let Some(table_meta) = self.table_meta(event.table_id) {
            for debezium in MysqlRowEventHandler::parse_delete_rows(table_meta, event) {
                self.send_debezium(debezium);
            }
        }
    }

    fn table_meta(&self, table_id: u64) -> Option<&TableMeta> {
        self.table_meta_cache
            .get(&self.current_binlog)
            .and_then(|cache| cache.get(&table_id))
    }

    fn send_debezium(&self, debezium: DebeziumFormat) {
        if self.channels.is_empty() {
            warn!("mysql source has no channel sender");
            return;
        }

        let key = debezium.keys();
        let meta = SourceMeta::mysql(
            debezium.source_database().unwrap_or_default(),
            debezium.source_table().unwrap_or_default(),
        );
        let record = resolve_record(PipelineRecord::new(debezium, meta), &self.resolver);
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let index = hasher.finish() as usize % self.channels.len();

        if let Err(err) = self.channels[index].send(record) {
            warn!("send mysql debezium to channel error:{:?}", err);
        }
    }

    fn can_exclude(&self, database_name: &str, table_name: &str) -> bool {
        let tables = self.source.tables();
        if tables == "*" || tables == "*.*" {
            return false;
        }

        tables.split(',').all(|pattern| {
            let pattern = pattern.trim();
            if let Some((db, table)) = pattern.split_once('.') {
                let db_match = db == "*" || db == database_name;
                let table_match = table == "*" || table == table_name;
                !(db_match && table_match)
            } else {
                pattern != table_name
            }
        })
    }
}

struct MysqlRowEventHandler;

impl MysqlRowEventHandler {
    fn parse_write_rows(table_meta: &TableMeta, event: WriteRowsEvent) -> Vec<DebeziumFormat> {
        event
            .rows
            .into_iter()
            .map(|row| Self::convert_and_parse_row(table_meta, row))
            .map(|after| {
                DebeziumFormat::insert(
                    json!(after),
                    table_meta.db_name(),
                    table_meta.table_name(),
                    Self::create_key(table_meta, &after),
                )
            })
            .collect()
    }

    fn parse_update_rows(table_meta: &TableMeta, event: UpdateRowsEvent) -> Vec<DebeziumFormat> {
        event
            .rows
            .into_iter()
            .map(|(before_row, after_row)| {
                (
                    Self::convert_and_parse_row(table_meta, before_row),
                    Self::convert_and_parse_row(table_meta, after_row),
                )
            })
            .map(|(before, after)| {
                DebeziumFormat::update(
                    Some(json!(before)),
                    json!(after),
                    table_meta.db_name(),
                    table_meta.table_name(),
                    Self::create_key(table_meta, &after),
                )
            })
            .collect()
    }

    fn parse_delete_rows(table_meta: &TableMeta, event: DeleteRowsEvent) -> Vec<DebeziumFormat> {
        event
            .rows
            .into_iter()
            .map(|row| Self::convert_and_parse_row(table_meta, row))
            .map(|before| {
                DebeziumFormat::delete(
                    json!(before),
                    table_meta.db_name(),
                    table_meta.table_name(),
                    Self::create_key(table_meta, &before),
                )
            })
            .collect()
    }

    fn create_key(table_meta: &TableMeta, row: &Map<String, Value>) -> MessageKey {
        let column_name = table_meta.primary_column();
        let primary = row.get(column_name).unwrap_or(&Value::Null);
        let mut key = Map::with_capacity(2);
        key.insert(column_name.to_string(), primary.clone());
        key.insert(
            "TableId".to_string(),
            json!(format!(
                "{}.{}",
                table_meta.db_name(),
                table_meta.table_name()
            )),
        );
        MessageKey::new(key)
    }

    fn convert_and_parse_row(table_meta: &TableMeta, row: RowEvent) -> Map<String, Value> {
        let mut position: usize = 1;
        let mut row_map = Map::with_capacity(row.column_values.len());
        row.column_values.into_iter().for_each(|column_value| {
            if let Some(column) = table_meta.column(position) {
                row_map.insert(
                    column.column_name().to_string(),
                    Self::convert_column_value_to_json(&column_value),
                );
            }
            position += 1;
        });
        row_map
    }

    fn convert_column_value_to_json(column_value: &ColumnValue) -> Value {
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
            ColumnValue::Timestamp(data) => Value::String(format_timestamp(*data)),
            ColumnValue::Year(data) => Value::Number(Number::from(*data)),
            ColumnValue::String(data) => Value::String(
                String::from_utf8(data.clone()).expect("convert data to utf8 string error"),
            ),
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
}

fn format_timestamp(timestamp: i64) -> String {
    let millis = timestamp / 1000;
    match Local.timestamp_millis_opt(millis) {
        LocalResult::Single(time) => time.format("%Y-%m-%d %H:%M:%S").to_string(),
        _ => {
            warn!("timestamp is invalid:{}", timestamp);
            String::new()
        }
    }
}
