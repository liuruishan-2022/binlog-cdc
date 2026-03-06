use futures_util::TryStreamExt;
use rdkafka::{
    ClientConfig, Message,
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
};
use tracing::{info, warn};

use crate::{
    binlog::row::DebeziumFormat, config::CdcConfig, config::source::Kafka,
    sink::SinkStream,
};

///
/// 放置Kafka作为数据源的处理代码
///

pub struct KafkaSource<'a, T>
where
    T: SinkStream,
{
    sink: T,
    source_config: &'a Kafka,
    cdc_config: &'a CdcConfig,
}

impl<'a, T> KafkaSource<'a, T>
where
    T: SinkStream,
{
    const BOOTSTRAP_SERVERS: &'static str = "bootstrap.servers";
    const GROUP_ID: &'static str = "group.id";
    const AUTO_OFFSET_RESET: &'static str = "auto.offset.reset";
    const EARLIEST: &'static str = "earliest";
    const MESSAGE_TIMEOUT_MS: &'static str = "message.timeout.ms";
    const SESSION_TIMEOUT_MS: &'static str = "session.timeout.ms";
    const BATCH_SIZE: &'static str = "batch.size";
    const COMPRESSION_TYPE: &'static str = "compression.type";
    const HEARTBEAT_INTERVAL_MS: &'static str = "heartbeat.interval.ms";
    const LINKGER_MS: &'static str = "linger.ms";
    pub fn new(sink: T, source_config: &'a Kafka, cdc_config: &'a CdcConfig) -> Self {
        KafkaSource {
            sink,
            source_config,
            cdc_config,
        }
    }

    pub async fn start(&self) {
        let consumer = self.build_consumer().expect("build consumer error!");
        let stream = consumer.stream().try_for_each(|message| async move {
            info!("消费到消息内容");

            let debezium =
                serde_json::from_slice::<DebeziumFormat>(&message.payload().unwrap()).unwrap();
            self.sink.process(&debezium, message.topic()).await;
            Ok(())
        });
        stream.await.expect("stream kafka message error!");
        warn!("kafka source stop!");
    }

    fn build_consumer(&self) -> Result<StreamConsumer, KafkaError> {
        let consumer = ClientConfig::new()
            .set(
                Self::BOOTSTRAP_SERVERS,
                self.source_config.bootstrap_server(),
            )
            .set(Self::GROUP_ID, self.source_config.group_id())
            .set(Self::AUTO_OFFSET_RESET, Self::EARLIEST)
            .set(Self::HEARTBEAT_INTERVAL_MS, "3000")
            .set(Self::SESSION_TIMEOUT_MS, "45000")
            .create::<StreamConsumer>()?;
        let topics = self
            .cdc_config
            .route_sources()
            .expect("no config topic for kafka source!");
        consumer.subscribe(&topics)?;
        return Ok(consumer);
    }
}
