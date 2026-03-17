use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender};
use mysql_binlog_connector_rust::{
    binlog_client::BinlogClient, binlog_error::BinlogError, event::event_data::EventData,
};
use prometheus_client::registry::Registry;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::{
    binlog::{BinlogEventData, Metrics, event_handler},
    common::CdcError,
    config::cdc::FlinkCdc,
    savepoint::{SavePoints, local::LocalFileSystem},
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

pub struct Dumper {}

impl Dumper {
    pub fn new() -> Self {
        Dumper {}
    }

    pub async fn start(
        &self,
        registry: Arc<Mutex<Registry>>,
        config: &FlinkCdc,
    ) -> Result<i32, CdcError> {
        let metrics = metrics(registry).await;
        let savepoint = LocalFileSystem::default();
        let binlog_file = savepoint.load().unwrap_or(config.source_binlog_file());

        let (senders, receivers) = self.channels();
        self.start_receivers(receivers);

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
                            savepoint.save(&event.binlog_filename);
                            metrics.inc_flink_mysql_cdc("rotate");
                        }
                        EventData::TableMap(event) => {
                            metrics.inc_flink_mysql_cdc("table-map");
                        }
                        EventData::WriteRows(event) => {
                            metrics.inc_flink_mysql_cdc("write-rows");
                            let event =
                                BinlogEventData::new("".to_string(), EventData::WriteRows(event));
                            let sender = self.random_sender(&senders);
                            let _ = sender.send(event);
                        }
                        EventData::DeleteRows(event) => {
                            metrics.inc_flink_mysql_cdc("delete-rows");
                            let event =
                                BinlogEventData::new("".to_string(), EventData::DeleteRows(event));
                            let sender = self.random_sender(&senders);
                            let _ = sender.send(event);
                        }
                        EventData::UpdateRows(event) => {
                            metrics.inc_flink_mysql_cdc("update-rows");
                            let event =
                                BinlogEventData::new("".to_string(), EventData::UpdateRows(event));
                            let sender = self.random_sender(&senders);
                            let _ = sender.send(event);
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

    fn start_receivers(&self, receivers: Vec<Receiver<BinlogEventData>>) {
        receivers.into_iter().for_each(|receiver| {
            tokio::spawn(async move {
                for event in receiver.iter() {
                    info!(
                        "Received binlog event: binlog={}, event_type={:?}",
                        event.binlog,
                        std::mem::discriminant(&event.event_data)
                    );
                }
                info!("Receiver channel closed, exiting receiver task");
            });
        });
    }

    fn channels(&self) -> (Vec<Sender<BinlogEventData>>, Vec<Receiver<BinlogEventData>>) {
        let (senders, receivers): (Vec<_>, Vec<_>) = (1..=3)
            .into_iter()
            .map(|_| crossbeam_channel::bounded(10000))
            .unzip();
        (senders, receivers)
    }

    fn random_sender(&self, senders: &[Sender<BinlogEventData>]) -> &Sender<BinlogEventData> {
        use rand::seq::SliceRandom;
        senders.choose(&mut rand::thread_rng()).unwrap()
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
}

async fn metrics(registry: Arc<Mutex<Registry>>) -> Metrics {
    let mut registry = registry.lock().await;

    let metrics = Metrics::default();
    metrics.register(&mut registry);
    return metrics;
}
