use std::{error::Error, sync::Arc, time};

use mysql_binlog_connector_rust::{
    binlog_client::BinlogClient, binlog_error::BinlogError, event::event_data::EventData,
};
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, gauge::Gauge},
    registry::Registry,
};
use tokio::{sync::Mutex, time::sleep};
use tracing::{info, warn};

use crate::{
    common::CdcError,
    config::cdc::FlinkCdc,
    savepoint::{SavePoints, local::LocalFileSystem},
};

pub mod event_handler;
pub mod row;
pub mod schema;

///
/// 监听binlog文件，以及解析binlong文件的内容
pub async fn dump_and_parse(
    registry: Arc<Mutex<Registry>>,
    config: &FlinkCdc,
) -> Result<i32, CdcError> {
    let metrics = metrics(registry).await;
    let savepoint = LocalFileSystem::default();
    let mut event_handler = event_handler::EventHandler::new(&config, &metrics).await;

    let binlog_file = savepoint.load().unwrap_or(config.source_binlog_file());

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

    let mut stream = client
        .connect()
        .await
        .expect("connect to mysql read binlog file error!");

    loop {
        let result = stream.read().await;
        match result {
            Ok((header, data)) => {
                //处理binlog event事件对象并且做监控
                metrics.stat_binlog_event_timestamp(header.timestamp);
                match data {
                    EventData::Rotate(event) => {
                        info!("read new binlog:{}", event.binlog_filename);
                        event_handler.handle_rotate_event(&event);
                        savepoint.save(&event.binlog_filename);
                        metrics.inc_flink_mysql_cdc("rotate");
                    }
                    EventData::TableMap(event) => {
                        event_handler.handle_table_map_event(&event).await;
                        metrics.inc_flink_mysql_cdc("table-map");
                    }
                    EventData::WriteRows(event) => {
                        event_handler.handle_write_rows_event(event).await;
                        metrics.inc_flink_mysql_cdc("write-rows");
                    }
                    EventData::DeleteRows(event) => {
                        event_handler.handle_delete_rows_event(event).await;
                        metrics.inc_flink_mysql_cdc("delete-rows");
                    }
                    EventData::UpdateRows(event) => {
                        event_handler.handle_update_rows_event(event).await;
                        metrics.inc_flink_mysql_cdc("update-rows");
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

                for index in 1..=config.source_connect_retry_times() {
                    info!("retry to reconnection mysql times:{}!", index);

                    sleep(config.source_connect_timeout()).await;
                    match client.connect().await {
                        Ok(re_stream) => {
                            info!("reconnection mysql success!");
                            stream = re_stream;
                            break;
                        }
                        Err(err) => {
                            warn!("reconnection mysql failed, error:{:?}!", err);
                        }
                    }
                }

                return CdcError::BinlogIo(err);
            }
            Err(BinlogError::UnexpectedData(err)) => {
                warn!(
                    "read binlog error:{} we will retry 3 times to reconnection!",
                    err
                );
                for index in 1..=config.source_connect_retry_times() {
                    info!("retry to reconnection mysql times:{}!", index);

                    sleep(config.source_connect_timeout()).await;
                    match client.connect().await {
                        Ok(re_stream) => {
                            info!("reconnection mysql success!");
                            stream = re_stream;
                            break;
                        }
                        Err(err) => {
                            warn!("reconnection mysql failed, error:{:?}!", err);
                        }
                    }
                }
                return CdcError::BinlogUnexpected(err);
            }
            Err(err) => {
                panic!("read mysql binlog error:{:?}", err);
                return CdcError::Other("Binlog error".to_string());
            }
        }
    }
}

///
/// 有关监控的一些配置信息
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct EventLabel {
    type_name: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct DescTableLabel {
    db_name: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct KafkaLabel {
    event: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct BinlogEventLabel {}

///
/// 整个服务的所有指标都定义在这里了
#[derive(Debug)]
pub struct Metrics {
    ///
    /// 记录mysql binlog cdc event的个数(按照不同的类型进行区分)
    flink_mysql_cdc: Family<EventLabel, Counter>,

    ///
    /// 监控table-map-event真实请求mysql的个数
    flink_mysql_desc_table: Family<DescTableLabel, Counter>,

    ///
    /// 监控发送到kafka的消息个数
    flink_sink_kafka_message: Family<KafkaLabel, Counter>,

    ///
    /// 记录读取到的binlog event的事件戳
    flink_mysql_binlog_event_timestamp: Family<BinlogEventLabel, Gauge<i64>>,
}

impl Metrics {
    pub fn default() -> Self {
        Metrics {
            flink_mysql_cdc: Family::default(),
            flink_mysql_desc_table: Family::default(),
            flink_sink_kafka_message: Family::default(),
            flink_mysql_binlog_event_timestamp: Family::default(),
        }
    }

    pub fn register(&self, registry: &mut Registry) {
        registry.register(
            "flink_mysql_cdc",
            "flink mysql cdc event count",
            self.flink_mysql_cdc.clone(),
        );
        registry.register(
            "flink_mysql_desc_table",
            "flink mysql desc table command total count",
            self.flink_mysql_desc_table.clone(),
        );
        registry.register(
            "flink_sink_kafka_message",
            "flink sink kafka message send count total",
            self.flink_sink_kafka_message.clone(),
        );
        registry.register(
            "flink_mysql_binlog_event_timestamp",
            "flink mysql binlog event timestamp",
            self.flink_mysql_binlog_event_timestamp.clone(),
        );
    }

    pub fn inc_flink_mysql_cdc(&self, type_name: &str) {
        let labels = EventLabel {
            type_name: type_name.to_string(),
        };
        self.flink_mysql_cdc.get_or_create(&labels).inc();
    }

    pub fn inc_flink_mysql_desc_table(&self, db_name: &str) {
        self.flink_mysql_desc_table
            .get_or_create(&DescTableLabel {
                db_name: db_name.to_string(),
            })
            .inc();
    }

    pub fn inc_flink_sink_kafka_message(&self, event: &str, count: u64) {
        self.flink_sink_kafka_message
            .get_or_create(&KafkaLabel {
                event: event.to_string(),
            })
            .inc_by(count);
    }

    pub fn stat_binlog_event_timestamp(&self, timestamp: u32) {
        self.flink_mysql_binlog_event_timestamp
            .get_or_create(&BinlogEventLabel {})
            .set(timestamp as i64);
    }
}

async fn metrics(registry: Arc<Mutex<Registry>>) -> Metrics {
    let mut registry = registry.lock().await;

    let metrics = Metrics::default();
    metrics.register(&mut registry);
    return metrics;
}
