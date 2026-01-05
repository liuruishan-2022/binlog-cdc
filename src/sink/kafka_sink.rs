use std::{
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
    time::Duration,
};

use rdkafka::{
    ClientConfig,
    error::KafkaError,
    producer::{FutureProducer, FutureRecord},
    types::RDKafkaErrorCode,
};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::{info, warn};

use crate::{binlog::row::DebeziumFormat, config::cdc::FlinkCdc, sink::SinkStream};

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

impl SinkStream for KafkaSink {
    async fn handle_messages(&self, messages: Vec<DebeziumFormat>) {
        self.send_batch_messages(messages).await;
    }
}

///
/// 多线程发送给Kafka的实现
/// 之前发现一个问题:就是解析binlog,然后发送到Kafka的过程是在一个线程中执行的,所以
/// 导致解析Binlog和发送Kafka相互争抢CPU,从而影响了整体的吞吐量
///
/// It is two threads to send messages to Kafka.
///
/// 为此我们决定使用channel来模拟一个单个生产者,但是多个消费者的模式
///

type Senders = Vec<Sender<DebeziumFormat>>;

pub struct SpmcKafkaSink {
    senders: Senders,
}

impl SpmcKafkaSink {
    pub fn build(config: &FlinkCdc) -> Self {
        type Channels = (Vec<Sender<DebeziumFormat>>, Vec<Receiver<DebeziumFormat>>);
        let (senders, receivers): Channels = (1..=config.pipeline_parallelism())
            .into_iter()
            .map(|_| mpsc::channel(10000))
            .unzip();

        let topic = Arc::new(config.sink_topic().to_string());

        for mut receiver in receivers {
            let producer = SpmcKafkaSink::create_producer(config);
            let target_topic = topic.clone();
            tokio::spawn(async move {
                while let Some(message) = receiver.recv().await {
                    let body = message.to_json();
                    let key = message.keys();
                    let message = FutureRecord::to(target_topic.as_str())
                        .key(key.as_str())
                        .payload(&body);

                    match producer.send_result(message) {
                        Ok(_) => {
                            info!("send use mpsc channels ok!")
                        }
                        Err(err) => {
                            warn!("send use mpsc channels error:{:?}", err);
                        }
                    }
                }
            });
        }
        SpmcKafkaSink { senders: senders }
    }

    fn create_producer(config: &FlinkCdc) -> FutureProducer {
        return ClientConfig::new()
        .set(BOOTSTRAP_SERVERS, config.sink_bootstrap_server())
        .set(MESSAGE_TIMEOUT_MS, "5000")
        .set(BATCH_SIZE, "10485760")
        .set(COMPRESSION_TYPE, config.sink_compression_type())
        .set(LINGER_MS, config.sink_linger_ms().to_string())
        .create::<FutureProducer>()
        .expect(format!("Sink Kafka producer creation failed of bootstrap.servers:{} compression-type:{}",config.sink_bootstrap_server(),config.sink_compression_type()).as_str());
    }
}

impl SinkStream for SpmcKafkaSink {
    async fn handle_messages(&self, messages: Vec<DebeziumFormat>) {
        let mut hasher = DefaultHasher::new();
        for msg in messages {
            msg.keys().hash(&mut hasher);
            let hash = hasher.finish();
            let index = hash % self.senders.len() as u64;
            match self.senders[index as usize].send(msg).await {
                Ok(_) => {}
                Err(err) => {
                    warn!("send message error:{:?}", err);
                }
            }
        }
    }
}
