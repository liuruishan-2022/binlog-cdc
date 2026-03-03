//! Kafka-based source implementation
//!
//! Provides high-performance Kafka consumer using rskafka.

use crate::config::Config;
use crate::config::source::{KafkaSourceConfig, SourceConfig};
use crate::source::Source;
use rskafka::client::partition::{PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use rskafka::topic::Topic;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use tracing::info;

pub struct TopicClient {
    pub client: Arc<PartitionClient>,
    pub current_offset: Arc<AtomicI64>,
}

type TopicClients = HashMap<String, TopicClient>;
type TopicMetadata = HashMap<String, Topic>;

/// Kafka source implementation with multi-topic support
pub struct KafkaSource {
    client: Client,
    topic_metadatas: TopicMetadata,
    partition_clients: TopicClients,
    config: Arc<Config>,
    kafka_config: KafkaSourceConfig,
}

impl KafkaSource {
    pub async fn create(config: Arc<Config>) -> Self {
        let kafka_config = if let SourceConfig::Kafka(source) = config.source() {
            source.clone()
        } else {
            panic!("Invalid source config for KafkaSource");
        };
        let connections = kafka_config.bootstrap_servers();
        let client = ClientBuilder::new(connections)
            .build()
            .await
            .expect("Failed to create kafka client");
        let topic_metadatas = KafkaSource::load_metadata(&client).await;

        KafkaSource {
            client,
            topic_metadatas,
            partition_clients: HashMap::new(),
            config: config,
            kafka_config: kafka_config,
        }
    }

    async fn load_metadata(client: &Client) -> HashMap<String, Topic> {
        let topics = client
            .list_topics()
            .await
            .expect("failed to load topics metadata...");
        topics.into_iter().fold(HashMap::new(), |mut acc, topic| {
            acc.entry(topic.name.clone()).or_insert(topic);
            acc
        })
    }

    pub async fn consume(&mut self) {
        if let Some(routes) = self.config.route() {
            let mut tasks = Vec::new();

            for route in routes {
                let topic_name = route.source_table().to_string();
                let topic_metadata = self
                    .topic_metadatas
                    .get(&topic_name)
                    .expect("fetch topic metadata error...")
                    .clone();

                for partition in topic_metadata.partitions.iter() {
                    let partition_id = partition.clone();
                    let topic_name_clone = topic_name.clone();

                    // 在主线程中为每个 partition 创建 partition_client
                    let partition_client = match self
                        .client
                        .partition_client(
                            topic_name.clone(),
                            partition_id,
                            UnknownTopicHandling::Retry,
                        )
                        .await
                    {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::error!(
                                "Failed to create partition client for topic {}, partition {}: {:?}",
                                topic_name,
                                partition_id,
                                e
                            );
                            continue;
                        }
                    };

                    // 启动任务，将 partition_client 包装在 Arc 中以便共享
                    let partition_client = Arc::new(partition_client);
                    let task = tokio::spawn(async move {
                        Self::consume_partition(
                            partition_client,
                            topic_name_clone,
                            partition_id,
                        )
                        .await;
                    });

                    tasks.push(task);
                }
            }

            // 等待所有任务（实际上它们会一直运行直到程序退出）
            for task in tasks {
                task.await.ok();
            }
        }
    }

    async fn consume_partition(
        partition_client: Arc<PartitionClient>,
        topic_name: String,
        partition_id: i32,
    ) {
        let start_offset = match partition_client
            .get_offset(rskafka::client::partition::OffsetAt::Earliest)
            .await
        {
            Ok(offset) => offset,
            Err(e) => {
                tracing::error!(
                    "Failed to get offset for topic {}, partition {}: {:?}",
                    topic_name,
                    partition_id,
                    e
                );
                return;
            }
        };

        let mut current_offset = start_offset;

        info!(
            "Starting consumption: topic={}, partition={}, start_offset={}",
            topic_name, partition_id, start_offset
        );

        loop {
            match partition_client
                .fetch_records(current_offset, 0..10000, 1000)
                .await
            {
                Ok((records, _high_watermark)) => {
                    for record in records {
                        info!(
                            "Consumed from {}:{} - offset: {}",
                            topic_name, partition_id, record.offset
                        );
                        current_offset = record.offset + 1;
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to fetch records from {}:{}: {:?}",
                        topic_name,
                        partition_id,
                        e
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }
}

impl std::fmt::Display for KafkaSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KafkaSource:{:?}", self.topic_metadatas)
    }
}

///
/// 实现Source这个trait,提供Kafka的基本的操作
impl Source for KafkaSource {
    async fn consumer(&mut self) {
        self.consume().await;
    }
}

#[cfg(test)]
mod tests {
    use tracing_subscriber::fmt;

    fn init() {
        fmt()
            .with_line_number(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .init();
    }
}
