use std::sync::Arc;

use base64::{Engine, engine::general_purpose};
use chrono::{Local, TimeZone, offset::LocalResult};
use crossbeam_channel::{Receiver, Sender};
use mysql_binlog_connector_rust::{
    binlog_client::BinlogClient,
    binlog_error::BinlogError,
    column::column_value::ColumnValue,
    event::{
        delete_rows_event::DeleteRowsEvent, event_data::EventData, row_event::RowEvent,
        update_rows_event::UpdateRowsEvent, write_rows_event::WriteRowsEvent,
    },
};
use prometheus_client::registry::Registry;
use serde_json::{Map, Value, json};
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::{
    binlog::row::{DebeziumFormat, MessageKey},
    binlog::{BinlogEventData, Metrics, event_channel::BinlogTableMetaHandler},
    common::CdcError,
    config::cdc::FlinkCdc,
    savepoint::{SavePoints, local::LocalFileSystem},
    transform::parser::ProjectionHandler,
};

///
/// 使用oop的思想来做mysql binlog的dump工作
///
/// 首先这个地方是binlog的dump的起点,然后我们想做的功能如下:
///
/// 1. 首先是启动binlog的dump功能,然后执行mysql的binlog的操作
/// 2. 接着就是处理各种binlog event事件
/// 3. 对于table map event就同步串行处理,如果是数据事件,则发送到一个新的Channel中
/// 4. 额外启动一个线程去消费这个channel
///
/// 所以,我们暂时先把这些逻辑都放置到Dumper这个struct中
///
/// 经过控制变量的方法进行对比,chanel模式比之前的模式速度快:10倍左右
/// 当然channel模式更耗费CPU,大概需要耗费5-6个C,不过这也是提升速度的唯一方案
///
/// 按照primary key执行partition的时候,大概是10s一个binlog的解析和分发，
/// 和同样的数据量的之前的方案对比,之前大概是:55s的样子,也就是说快了5.5倍的性能
/// 这个和刚才的6倍差距是需要在binlog的线程根据primary key计算partition

pub struct Dumper {
    current_binlog: String,
    table_meta_handler: Arc<BinlogTableMetaHandler>,
    parallelism: u32,
    capacity: u32,
}

impl Dumper {
    pub async fn new(config: &FlinkCdc) -> Result<Self, CdcError> {
        let table_meta_handler = BinlogTableMetaHandler::new(&config.source_url()).await?;
        Ok(Dumper {
            current_binlog: String::from(""),
            table_meta_handler: Arc::new(table_meta_handler),
            parallelism: config.pipeline_parallelism(),
            capacity: config.pipeline_capacity(),
        })
    }

    pub async fn start(
        &mut self,
        registry: Arc<Mutex<Registry>>,
        config: &FlinkCdc,
    ) -> Result<i32, CdcError> {
        let metrics = metrics(registry).await;
        let savepoint = LocalFileSystem::default();
        let binlog_file = savepoint.load().unwrap_or(config.source_binlog_file());

        let (senders, receivers) = self.channels();
        self.start_receivers(receivers, config);

        let mut stream = self.binlog_stream(config, binlog_file).await;

        loop {
            let result = stream.read().await;
            match result {
                Ok((header, data)) => {
                    //处理binlog event事件对象并且做监控
                    metrics.stat_binlog_event_timestamp(header.timestamp);
                    match data {
                        EventData::Rotate(event) => {
                            info!("read new binlog:{}", event.binlog_filename);
                            self.set_binlog_filename(&event.binlog_filename);
                            savepoint.save(&event.binlog_filename);
                            metrics.inc_flink_mysql_cdc("rotate");
                        }
                        EventData::TableMap(event) => {
                            metrics.inc_flink_mysql_cdc("table-map");
                            self.table_meta_handler
                                .record_table_meta(&self.current_binlog, event)
                                .await;
                        }
                        EventData::WriteRows(event) => {
                            metrics.inc_flink_mysql_cdc("write-rows");
                            self.partition_write_event(event, &self.current_binlog, &senders);
                        }
                        EventData::DeleteRows(event) => {
                            metrics.inc_flink_mysql_cdc("delete-rows");
                            self.partition_delete_event(event, &self.current_binlog, &senders);
                        }
                        EventData::UpdateRows(event) => {
                            metrics.inc_flink_mysql_cdc("update-rows");
                            self.partition_update_event(event, &self.current_binlog, &senders);
                        }
                        EventData::NotSupported => {
                            metrics.inc_flink_mysql_cdc("not-supported");
                        }
                        EventData::FormatDescription(_event) => {
                            metrics.inc_flink_mysql_cdc("format-description");
                        }
                        EventData::PreviousGtids(_event) => {
                            metrics.inc_flink_mysql_cdc("previous-gtids");
                        }
                        EventData::Gtid(_event) => {
                            metrics.inc_flink_mysql_cdc("gtid");
                        }
                        EventData::Query(_event) => {
                            metrics.inc_flink_mysql_cdc("query");
                        }
                        EventData::Xid(_event) => {
                            metrics.inc_flink_mysql_cdc("xid");
                        }
                        EventData::XaPrepare(_event) => {
                            metrics.inc_flink_mysql_cdc("xa-prepare");
                        }
                        EventData::TransactionPayload(_event) => {
                            metrics.inc_flink_mysql_cdc("transaction-payload");
                        }
                        EventData::RowsQuery(_event) => {
                            metrics.inc_flink_mysql_cdc("rows-query");
                        }
                        EventData::HeartBeat => {
                            metrics.inc_flink_mysql_cdc("heart-beat");
                        }
                    }
                }
                //
                // 目前生产省出现了如下的错误,需要进行重新建立连接的操作
                // 1. IoError(Error { kind: InvalidData, message: "stream did not contain valid UTF-8" })
                // 处理方案就是: 对BinlogError::IoError(err) 进行一个抓取处理
                // 2. UnexpectedData("Read binlog header timeout after 10s while waiting for packet header")
                // 目前发现生产有这个问题: 处理这个问题
                Err(BinlogError::IoError(err)) => {
                    //在Mysql重启的时候,无法做到重新连接的操作
                    warn!(
                        "read binlog error:{:?} we will retry 3 times to reconnection!",
                        err
                    );
                    return Err(CdcError::BinlogIo("Binlog IoError".to_string()));
                }
                Err(BinlogError::UnexpectedData(err)) => {
                    warn!(
                        "read binlog error:{} we will retry 3 times to reconnection!",
                        err
                    );
                    return Err(CdcError::BinlogUnexpected(err));
                }
                Err(err) => {
                    warn!("read mysql binlog error:{:?}", err);
                    return Err(CdcError::Other("Binlog error".to_string()));
                }
            }
        }
    }

    fn partition_write_event(
        &self,
        event: WriteRowsEvent,
        binlog: &str,
        senders: &Vec<Sender<BinlogEventData>>,
    ) {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let sender_count = senders.len();

        if let Some(table_meta) = self.table_meta_handler.table_schema(binlog, event.table_id) {
            event.rows.into_iter().for_each(|row| {
                if let Some(primary_key) = row
                    .column_values
                    .get(table_meta.primary_key_position() as usize)
                {
                    let key = BinlogRowEventHandler::convert_column_value_to_json(primary_key)
                        .to_string();
                    let mut hasher = DefaultHasher::new();
                    key.hash(&mut hasher);
                    let hash_code = hasher.finish();
                    let sender = senders
                        .get(hash_code as usize % sender_count)
                        .expect("fetch sender error");

                    let partition_event = WriteRowsEvent {
                        table_id: event.table_id,
                        included_columns: vec![],
                        rows: vec![row],
                    };

                    let binlog_event = BinlogEventData::new(
                        binlog.to_string(),
                        EventData::WriteRows(partition_event),
                    );
                    let _ = sender.send(binlog_event);
                }
            });
        }
    }

    fn partition_delete_event(
        &self,
        event: DeleteRowsEvent,
        binlog: &str,
        senders: &Vec<Sender<BinlogEventData>>,
    ) {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let sender_count = senders.len();

        if let Some(table_meta) = self.table_meta_handler.table_schema(binlog, event.table_id) {
            event.rows.into_iter().for_each(|row| {
                if let Some(primary_key) = row
                    .column_values
                    .get(table_meta.primary_key_position() as usize)
                {
                    let key = BinlogRowEventHandler::convert_column_value_to_json(primary_key)
                        .to_string();
                    let mut hasher = DefaultHasher::new();
                    key.hash(&mut hasher);
                    let hash_code = hasher.finish();
                    let sender = senders
                        .get(hash_code as usize % sender_count)
                        .expect("fetch sender error");

                    let partition_event = DeleteRowsEvent {
                        table_id: event.table_id,
                        included_columns: vec![],
                        rows: vec![row],
                    };

                    let binlog_event = BinlogEventData::new(
                        binlog.to_string(),
                        EventData::DeleteRows(partition_event),
                    );
                    let _ = sender.send(binlog_event);
                }
            });
        }
    }

    fn partition_update_event(
        &self,
        event: UpdateRowsEvent,
        binlog: &str,
        senders: &Vec<Sender<BinlogEventData>>,
    ) {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let sender_count = senders.len();

        if let Some(table_meta) = self.table_meta_handler.table_schema(binlog, event.table_id) {
            event.rows.into_iter().for_each(|(before, after)| {
                // 使用 before 行的 primary_key 来确定路由
                if let Some(primary_key) = before
                    .column_values
                    .get(table_meta.primary_key_position() as usize)
                {
                    let key = BinlogRowEventHandler::convert_column_value_to_json(primary_key)
                        .to_string();
                    let mut hasher = DefaultHasher::new();
                    key.hash(&mut hasher);
                    let hash_code = hasher.finish();
                    let sender = senders
                        .get(hash_code as usize % sender_count)
                        .expect("fetch sender error");

                    let partition_event = UpdateRowsEvent {
                        table_id: event.table_id,
                        included_columns_before: vec![],
                        included_columns_after: vec![],
                        rows: vec![(before, after)],
                    };

                    let binlog_event = BinlogEventData::new(
                        binlog.to_string(),
                        EventData::UpdateRows(partition_event),
                    );
                    let _ = sender.send(binlog_event);
                }
            });
        }
    }

    fn start_receivers(&self, receivers: Vec<Receiver<BinlogEventData>>, config: &FlinkCdc) {
        let table_handler = Arc::clone(&self.table_meta_handler);
        receivers.into_iter().for_each(|receiver| {
            let table_handler = Arc::clone(&table_handler);
            let row_handler = BinlogRowEventHandler::new(config);
            tokio::spawn(async move {
                for binlog_event in receiver.iter() {
                    match binlog_event.event_data {
                        EventData::WriteRows(event) => {
                            let table_meta =
                                table_handler.table_schema(&binlog_event.binlog, event.table_id);
                            if let Some(table_meta) = table_meta {
                                let _rows = row_handler.parse_write_rows(&table_meta, event);
                            }
                        }
                        EventData::UpdateRows(event) => {
                            let table_meta =
                                table_handler.table_schema(&binlog_event.binlog, event.table_id);
                            if let Some(table_meta) = table_meta {
                                let _rows = row_handler.parse_update_rows(&table_meta, event);
                            }
                        }
                        EventData::DeleteRows(event) => {
                            let table_meta =
                                table_handler.table_schema(&binlog_event.binlog, event.table_id);
                            if let Some(table_meta) = table_meta {
                                let _rows = row_handler.parse_delete_rows(&table_meta, event);
                            }
                        }
                        _ => {}
                    }
                }
                info!("Receiver channel closed, exiting receiver task");
            });
        });
    }

    fn channels(&self) -> (Vec<Sender<BinlogEventData>>, Vec<Receiver<BinlogEventData>>) {
        let (senders, receivers): (Vec<_>, Vec<_>) = (1..=self.parallelism)
            .into_iter()
            .map(|_| crossbeam_channel::bounded(self.capacity as usize))
            .unzip();
        (senders, receivers)
    }

    fn random_sender<'a>(
        &self,
        senders: &'a [Sender<BinlogEventData>],
    ) -> &'a Sender<BinlogEventData> {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0..senders.len());
        &senders[index]
    }

    async fn binlog_stream(
        &self,
        config: &FlinkCdc,
        binlog_file: String,
    ) -> mysql_binlog_connector_rust::binlog_stream::BinlogStream {
        let mut client = BinlogClient {
            url: config.source_url(),
            server_id: config.source_server_id(),
            binlog_filename: binlog_file,
            binlog_position: config.source_binlog_offset(),
            gtid_enabled: false,
            gtid_set: "rust-123".to_string(),
            heartbeat_interval_secs: 10,
            timeout_secs: config.source_keep_alive_interval().as_secs(),
            keepalive_idle_secs: 60,
            keepalive_interval_secs: 60,
        };

        client
            .connect()
            .await
            .expect("connect to mysql read binlog file error!")
    }

    fn set_binlog_filename(&mut self, binlog_name: &str) {
        self.current_binlog = binlog_name.to_string();
    }

    fn current_binlog_filename(&self) -> String {
        self.current_binlog.clone()
    }
}

async fn metrics(registry: Arc<Mutex<Registry>>) -> Metrics {
    let mut registry = registry.lock().await;

    let metrics = Metrics::default();
    metrics.register(&mut registry);
    return metrics;
}

///
/// 处理 Binlog Row 事件的 Handler
/// 不包含 Kafka 和 Metrics 相关逻辑
///

pub struct BinlogRowEventHandler {
    projection: ProjectionHandler,
}

impl BinlogRowEventHandler {
    pub fn new(config: &FlinkCdc) -> Self {
        BinlogRowEventHandler {
            projection: ProjectionHandler::create(config.transforms()),
        }
    }

    ///
    ///
    /// 解析并返回 Debezium 格式的数据
    ///
    pub fn parse_write_rows(
        &self,
        table_meta: &crate::binlog::schema::TableMeta,
        event: mysql_binlog_connector_rust::event::write_rows_event::WriteRowsEvent,
    ) -> Vec<DebeziumFormat> {
        let mut rows = event
            .rows
            .into_iter()
            .map(|row| Self::convert_and_parse_row(table_meta, row))
            .collect::<Vec<_>>();

        rows.iter_mut().for_each(|map| {
            self.projection
                .eval(&table_meta.qualified_table_name(), map);
        });

        rows.into_iter()
            .map(|after| {
                DebeziumFormat::insert(
                    serde_json::json!(after),
                    table_meta.db_name(),
                    table_meta.table_name(),
                    self.create_key(table_meta, &after),
                )
            })
            .collect()
    }

    pub fn parse_update_rows(
        &self,
        table_meta: &crate::binlog::schema::TableMeta,
        event: mysql_binlog_connector_rust::event::update_rows_event::UpdateRowsEvent,
    ) -> Vec<DebeziumFormat> {
        event
            .rows
            .into_iter()
            .map(|(before_row, after_row)| {
                (
                    Self::convert_and_parse_row(table_meta, before_row),
                    Self::convert_and_parse_row(table_meta, after_row),
                )
            })
            .map(|(before, mut after)| {
                self.projection
                    .eval(&table_meta.qualified_table_name(), &mut after);
                DebeziumFormat::update(
                    serde_json::json!(before),
                    serde_json::json!(after),
                    table_meta.db_name(),
                    table_meta.table_name(),
                    self.create_key(table_meta, &before),
                )
            })
            .collect()
    }

    pub fn parse_delete_rows(
        &self,
        table_meta: &crate::binlog::schema::TableMeta,
        event: mysql_binlog_connector_rust::event::delete_rows_event::DeleteRowsEvent,
    ) -> Vec<DebeziumFormat> {
        event
            .rows
            .into_iter()
            .map(|row| Self::convert_and_parse_row(table_meta, row))
            .map(|before| {
                DebeziumFormat::delete(
                    serde_json::json!(before),
                    table_meta.db_name(),
                    table_meta.table_name(),
                    self.create_key(table_meta, &before),
                )
            })
            .collect()
    }

    fn create_key(
        &self,
        table_meta: &crate::binlog::schema::TableMeta,
        row: &serde_json::Map<String, Value>,
    ) -> MessageKey {
        let column_name = table_meta.primary_column();
        let primary = row.get(column_name).unwrap();
        let mut key = serde_json::Map::new();
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

    fn convert_and_parse_row(
        table_meta: &crate::binlog::schema::TableMeta,
        row: RowEvent,
    ) -> Map<String, Value> {
        let mut position: usize = 1;
        let mut row_map = serde_json::Map::new();
        row.column_values.into_iter().for_each(|column_value| {
            if let Some(column) = table_meta.column(position) {
                let column_name = column.column_name();
                let value = Self::convert_column_value_to_json(&column_value);
                row_map.insert(column_name.to_string(), value);
            }
            position = position + 1;
        });
        row_map
    }

    fn convert_column_value_to_json(column_value: &ColumnValue) -> Value {
        match column_value {
            ColumnValue::Tiny(data) => json!(data),
            ColumnValue::Short(data) => json!(data),
            ColumnValue::Long(data) => json!(data),
            ColumnValue::LongLong(data) => json!(data),
            ColumnValue::Float(data) => json!(data),
            ColumnValue::Double(data) => json!(data),
            ColumnValue::Decimal(data) => json!(data),
            ColumnValue::Time(data) => json!(data),
            ColumnValue::Date(data) => json!(data),
            ColumnValue::DateTime(data) => json!(data),
            ColumnValue::Timestamp(data) => {
                let time_format = format_timestamp(*data);
                json!(time_format)
            }
            ColumnValue::Year(data) => json!(data),
            ColumnValue::String(data) => {
                let data =
                    String::from_utf8(data.clone()).expect("convert data to utf8 string error");
                json!(data)
            }
            ColumnValue::Blob(data) => {
                let data = String::from_utf8(data.clone())
                    .unwrap_or_else(|_| general_purpose::STANDARD.encode(data));
                json!(data)
            }
            ColumnValue::Bit(data) => json!(data),
            ColumnValue::Set(data) => json!(data),
            ColumnValue::Enum(data) => json!(data),
            ColumnValue::Json(data) => json!(data),
            _ => Value::Null,
        }
    }
}

///
/// 格式化时间戳
///

fn format_timestamp(timestamp: i64) -> String {
    let millis = timestamp / 1000;
    match Local.timestamp_millis_opt(millis) {
        LocalResult::Single(time) => time.format("%Y-%m-%d %H:%M:%S").to_string(),
        _ => {
            warn!("timestamp is invalid:{}!", timestamp);
            "".to_string()
        }
    }
}
