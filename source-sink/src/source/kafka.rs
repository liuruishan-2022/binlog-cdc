//! Kafka-based source implementation
//!
//! Provides high-performance Kafka consumer using rskafka.

use crate::common::DebeziumFormat;
use crate::config::Config;
use crate::pipeline::message::{PipelineMessage, RouteInfo};
use crate::source::Source;
use async_trait::async_trait;
use rskafka::client::ClientBuilder;
use rskafka::client::partition::PartitionClient;
use rskafka::client::partition::UnknownTopicHandling;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

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

/// Kafka source implementation with multi-topic support
pub struct KafkaSource {
    config: KafkaSourceConfig,
    running: Arc<AtomicBool>,
    sender: Option<mpsc::Sender<PipelineMessage>>,
    // Map topic name to its partition client and current offset
    topic_clients: Arc<Mutex<HashMap<String, TopicClient>>>,
    kafka_client: Option<Arc<rskafka::client::Client>>,
}

impl KafkaSource {
    pub fn new(brokers: Vec<String>) -> Self {
        Self::with_config(KafkaSourceConfig {
            brokers,
            ..Default::default()
        })
    }

    pub fn with_config(config: KafkaSourceConfig) -> Self {
        Self {
            config,
            running: Arc::new(AtomicBool::new(false)),
            sender: None,
            topic_clients: Arc::new(Mutex::new(HashMap::new())),
            kafka_client: None,
        }
    }

    fn is_running_inner(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    async fn consume_messages(&self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Connecting to Kafka brokers: {:?}", self.config.brokers);

        // Build client
        let client = ClientBuilder::new(self.config.brokers.clone())
            .build()
            .await?;
        let client = Arc::new(client);
        info!("Kafka client created successfully");

        // List all available topics from Kafka cluster
        let topics = client.list_topics().await?;
        info!("Found {} topics in Kafka cluster", topics.len());

        let mut topics_to_consume = Vec::new();
        let partition = self.config.partition;

        for topic_metadata in &topics {
            let topic_name = topic_metadata.name.clone();
            info!("Found topic: {} with {} partitions",
                topic_name,
                topic_metadata.partitions.len()
            );

            // Check if the specified partition exists
            let has_partition = topic_metadata.partitions
                .iter()
                .any(|&p| p == partition);

            if has_partition {
                topics_to_consume.push(topic_name);
            } else {
                warn!(
                    "Topic {} does not have partition {}, skipping",
                    topic_name, partition
                );
            }
        }

        if topics_to_consume.is_empty() {
            warn!("No topics available with partition {}", partition);
            return Ok(());
        }

        info!("Will consume from {} topics: {:?}", topics_to_consume.len(), topics_to_consume);

        let running = self.running.clone();
        let sender = self.sender.clone().unwrap();
        let partition = self.config.partition;

        // Spawn consumer tasks for each topic
        for topic in topics_to_consume {
            let topic_clone: String = topic.clone();
            let sender_clone = sender.clone();
            let running_clone = running.clone();
            let client_clone = client.clone();

            tokio::spawn(async move {
                info!("Starting consumer for topic: {}", topic_clone);

                // Create partition client
                let partition_client: PartitionClient = match client_clone
                    .partition_client(topic_clone.clone(), partition, UnknownTopicHandling::Retry)
                    .await
                {
                    Ok(pc) => pc,
                    Err(e) => {
                        error!(
                            "Failed to create partition client for {}: {}",
                            topic_clone, e
                        );
                        return;
                    }
                };

                let start_offset = 0i64;
                let mut current_offset = start_offset;

                info!(
                    "Starting consumption from topic: {}, partition: {}, offset: {}",
                    topic_clone, partition, start_offset
                );

                while running_clone.load(Ordering::Relaxed) {
                    match partition_client
                        .fetch_records(current_offset, 1..100_000, 1000)
                        .await
                    {
                        Ok((records, high_watermark)) => {
                            let record_count = records.len();
                            debug!(
                                "Fetched {} records from {}, high_watermark: {}",
                                record_count, topic_clone, high_watermark
                            );

                            for record in &records {
                                let offset = record.offset;

                                info!(
                                    "Kafka message: topic={}, partition={}, offset={}",
                                    topic_clone, partition, offset
                                );

                                let key_len = record
                                    .record
                                    .key
                                    .as_ref()
                                    .map(|k: &Vec<u8>| k.len())
                                    .unwrap_or(0);
                                let value_len = record
                                    .record
                                    .value
                                    .as_ref()
                                    .map(|v: &Vec<u8>| v.len())
                                    .unwrap_or(0);

                                debug!("Key length: {}, Payload length: {}", key_len, value_len);

                                // Try to parse as Debezium format
                                if let Some(payload) = &record.record.value {
                                    if let Ok(debezium) =
                                        serde_json::from_slice::<DebeziumFormat>(payload)
                                    {
                                        // Use source_table from debezium as the topic name
                                        let source_table = format!(
                                            "{}.{}",
                                            debezium.source.db, debezium.source.table
                                        );

                                        let route =
                                            RouteInfo::new(source_table.clone(), source_table);
                                        let msg =
                                            PipelineMessage::with_single_route(debezium, route);

                                        if let Err(e) = sender_clone.try_send(msg) {
                                            error!("Failed to send message to pipeline: {}", e);
                                        } else {
                                            debug!("Message sent to pipeline successfully");
                                        }
                                    } else {
                                        warn!(
                                            "Received non-Debezium message from {}, skipping",
                                            topic_clone
                                        );
                                    }
                                } else {
                                    warn!(
                                        "Received message with null payload from {}, skipping",
                                        topic_clone
                                    );
                                }

                                current_offset = offset + 1;
                            }

                            // Update to high watermark if no new records
                            if record_count == 0 && current_offset < high_watermark {
                                current_offset = high_watermark;
                            }
                        }
                        Err(e) => {
                            error!("Error fetching records from {}: {}", topic_clone, e);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }

                    // Small delay to avoid tight loop when no new messages
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }

                info!("Kafka consumer loop stopped for topic: {}", topic_clone);
            });
        }

        Ok(())
    }
}

#[async_trait]
impl Source for KafkaSource {
    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.is_running_inner() {
            info!("Kafka source is already running");
            return Ok(());
        }

        info!("Starting Kafka source (without config)");

        if self.sender.is_none() {
            return Err("Sender not set. Call set_sender() before start()".into());
        }

        self.running.store(true, Ordering::Relaxed);

        info!(
            "Kafka source started successfully (use start_with_config to enable route-based consumption)"
        );
        Ok(())
    }

    async fn start_with_config(
        &mut self,
        config: &Config,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.is_running_inner() {
            info!("Kafka source is already running");
            return Ok(());
        }

        info!("Starting Kafka source with config");

        if self.sender.is_none() {
            return Err("Sender not set. Call set_sender() before start()".into());
        }

        self.running.store(true, Ordering::Relaxed);

        // Start consuming messages from routes
        self.consume_messages().await?;

        info!("Kafka source started successfully with route-based topics");
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.is_running_inner() {
            info!("Kafka source is not running");
            return Ok(());
        }

        info!("Stopping Kafka source");
        self.running.store(false, Ordering::Relaxed);

        // Allow time for consumer to stop gracefully
        tokio::time::sleep(Duration::from_millis(500)).await;

        info!("Kafka source stopped");
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.is_running_inner()
    }

    fn set_sender(&mut self, sender: mpsc::Sender<PipelineMessage>) {
        self.sender = Some(sender);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = KafkaSourceConfig::default();
        assert_eq!(config.brokers, vec!["localhost:9092"]);
        assert_eq!(config.partition, 3);
        assert_eq!(config.start_offset, "latest");
    }

    #[test]
    fn test_kafka_source_creation() {
        let source = KafkaSource::new(vec!["localhost:9092".to_string()]);
        assert!(!source.is_running());
    }
}
