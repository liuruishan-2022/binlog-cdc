//! Kafka-based source implementation
//!
//! Provides high-performance Kafka consumer using rskafka.

use crate::source::Source;
use rskafka::client::partition::{PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use rskafka::topic::Topic;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

#[derive(Debug, Clone)]
pub struct KafkaSourceConfig {
    pub brokers: Vec<String>,
    pub group_id: Option<String>,
    pub start_offset: String,
    pub partition: i32,
}

impl Default for KafkaSourceConfig {
    fn default() -> Self {
        Self {
            brokers: vec!["localhost:9092".to_string()],
            group_id: None,
            start_offset: "latest".to_string(),
            partition: 3,
        }
    }
}

pub struct TopicClient {
    pub client: Arc<PartitionClient>,
    pub current_offset: Arc<AtomicI64>,
}

type TopicClients = HashMap<String, TopicClient>;

/// Kafka source implementation with multi-topic support
pub struct KafkaSource {
    client: Client,
    topic_metadatas: HashMap<String, Topic>,
    partition_clients: Option<TopicClients>,
}

impl KafkaSource {
    pub async fn create(connections: &str) -> Self {
        let connections = connections
            .split(',')
            .map(|s| s.trim().to_string())
            .collect::<Vec<String>>();
        let client = ClientBuilder::new(connections)
            .build()
            .await
            .expect("Failed to create kafka client");
        let topic_metadatas = KafkaSource::load_metadata(&client).await;

        return KafkaSource {
            client: client,
            topic_metadatas: topic_metadatas,
            partition_clients: None,
        };
    }

    async fn load_metadata(client: &Client) -> HashMap<String, Topic> {
        let topics = client
            .list_topics()
            .await
            .expect("failed to load topics metadata...");
        return topics.into_iter().fold(HashMap::new(), |mut acc, topic| {
            acc.entry(topic.name.clone()).or_insert(topic);
            acc
        });
    }

    async fn consumer(&self) {
        let partition_client = self
            .client
            .partition_client("kafka-press", 0, UnknownTopicHandling::Retry)
            .await
            .expect("create partition client error...");
        partition_client.fetch_records(offset, bytes, max_wait_ms)
    }
}

impl Display for KafkaSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KafkaSource:{:?}", self.topic_metadatas)
    }
}

///
/// 实现Source这个trait,提供Kafka的基本的操作
impl Source for KafkaSource {}

#[cfg(test)]
mod tests {
    use tracing::info;
    use tracing_subscriber::fmt;

    use crate::source::kafka::KafkaSource;

    fn init() {
        fmt()
            .with_line_number(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .init();
    }

    #[tokio::test]
    async fn test_load_metadata() {
        init();
        let connections = "172.16.1.135:9092,172.16.1.118:9092,172.16.1.149:9092";
        let kafka = KafkaSource::create(connections).await;
        info!("KafkaSource metadata: {}", kafka);
    }
}
