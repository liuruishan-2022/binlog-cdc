use crossbeam_channel::{Receiver, Sender};
use tracing::info;

use crate::{
    binlog::row::DebeziumFormat,
    config::{CdcConfig, sink::Sink},
    sink::{console::ConsoleSink, kafka::RskafkaSink},
    source::mysqldump::MysqldumpSource,
};

/// 主要是放置数据处理的Pipeline的逻辑
/// 类似于流水线的思想去做
///

pub async fn pipeline(cdc: &CdcConfig) {
    let (senders, receivers) = channels();
    let sink_handles = match cdc.sink() {
        Sink::Console(_) => ConsoleSink::create(receivers).start(),
        Sink::Kafka(kafka) => RskafkaSink::create_with_channels(kafka, receivers)
            .await
            .start(),
        Sink::Mysql(_) => {
            panic!("mysql sink is not supported by pipeline yet");
        }
    };
    info!("sink receiver workers已启动, count={}", sink_handles.len());

    let mut source = MysqldumpSource::create(cdc, senders);
    info!("source开始读取");
    source.read().await;
    info!("source读取完成, 准备关闭sender");
    drop(source);

    for handle in sink_handles {
        handle.await.expect("sink task failed");
    }
    info!("pipeline执行完成");
}

fn channels() -> (Vec<Sender<DebeziumFormat>>, Vec<Receiver<DebeziumFormat>>) {
    (1..=6)
        .map(|_index| crossbeam_channel::bounded(1000))
        .unzip()
}
