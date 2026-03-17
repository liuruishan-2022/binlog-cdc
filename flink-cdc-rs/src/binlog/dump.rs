use std::sync::Arc;

use mysql_binlog_connector_rust::{
    binlog_client::BinlogClient, binlog_error::BinlogError, event::event_data::EventData,
};
use prometheus_client::registry::Registry;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::{
    binlog::{Metrics, event_handler},
    common::CdcError,
    config::cdc::FlinkCdc,
    savepoint::SavePoints,
    savepoint::local::LocalFileSystem,
};

///
/// 使用oop的思想来做mysql binlog的dump工作
///

pub struct Dumper {}

impl Dumper {
    pub fn new() -> Self {
        Dumper {}
    }

    pub async fn dump_and_parse(
        registry: Arc<Mutex<Registry>>,
        config: &FlinkCdc,
    ) -> Result<i32, CdcError> {
        let metrics = metrics(registry).await;
        let savepoint = LocalFileSystem::default();
        let mut event_handler = event_handler::EventHandler::new(&config, &metrics).await?;

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
                            event_handler.handle_table_map_event(event).await;
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
}

async fn metrics(registry: Arc<Mutex<Registry>>) -> Metrics {
    let mut registry = registry.lock().await;

    let metrics = Metrics::default();
    metrics.register(&mut registry);
    return metrics;
}
