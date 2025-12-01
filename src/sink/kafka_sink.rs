use std::{sync::Arc, time::Duration};

use rdkafka::{
    ClientConfig,
    error::KafkaError,
    producer::{FutureProducer, FutureRecord},
    types::RDKafkaErrorCode,
};
use tracing::{info, warn};

use crate::{binlog::row::DebeziumFormat, config::cdc::FlinkCdc};

///
/// 按照Flink CDC的思想，有source和sink的类型，目前我们只支持sink为Kafka的类型
/// 并且只支持投递一个配置的topic的地址
///

const BOOTSTRAP_SERVERS: &'static str = "bootstrap.servers";
const MESSAGE_TIMEOUT_MS: &'static str = "message.timeout.ms";
const BATCH_SIZE: &'static str = "batch.size";
const COMPRESSION_TYPE: &'static str = "compression.type";
const LINGER_MS: &'static str = "linger.ms";

pub struct KafkaSink {
    producer: FutureProducer,
    topic: Arc<String>,
}

impl KafkaSink {
    pub fn build(config: &FlinkCdc) -> Self {
        let producer = ClientConfig::new()
        .set(BOOTSTRAP_SERVERS, config.sink_bootstrap_server())
        .set(MESSAGE_TIMEOUT_MS, "5000")
        .set(BATCH_SIZE, "10485760")
        .set(COMPRESSION_TYPE, config.sink_compression_type())
        .set(LINGER_MS, config.sink_linger_ms().to_string())
        .create::<FutureProducer>()
        .expect(format!("Sink Kafka producer creation failed of bootstrap.servers:{} compression-type:{}",config.sink_bootstrap_server(),config.sink_compression_type()).as_str());

        KafkaSink {
            producer: producer,
            topic: Arc::new(config.sink_topic().to_string()),
        }
    }

    pub async fn send_batch_messages(&self, messages: Vec<DebeziumFormat>) {
        for message in messages {
            let body = message.to_json();
            let key = message.keys();
            let message = FutureRecord::to(self.topic.as_str())
                .key(key.as_str())
                .payload(&body);
            match self.producer.send_result(message) {
                Ok(_) => {
                    info!("send message ok,keys:{}!", key);
                }
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), record)) => {
                    let retry_result = self.producer.send(record, Duration::from_secs(3)).await;
                    match retry_result {
                        Ok(_) => {
                            info!("retry send message ok,keys:{}!", key);
                        }
                        Err(err) => {
                            warn!("retry send message error,keys:{},error:{:?}", key, err);
                        }
                    }
                }
                Err(err) => {
                    warn!("send message error,keys:{}!{:?}", key, err);
                }
            }
        }
    }
}
