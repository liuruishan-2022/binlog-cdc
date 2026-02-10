//! Kafka-based source implementation
//!
//! Provides high-performance Kafka consumer using rskafka.

use crate::source::Source;
use crate::pipeline::message::{PipelineMessage, RouteInfo};
use crate::common::DebeziumFormat;
use async_trait::async_trait;
use rskafka::client::ClientBuilder;
use rskafka::client::partition::UnknownTopicHandling;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone)]
pub struct KafkaSourceConfig {
    pub brokers: Vec<String>,
    pub topic: String,
    pub partition: i32,
    pub group_id: Option<String>,
    pub start_offset: String,
}

impl Default for KafkaSourceConfig {
    fn default() -> Self {
        Self {
            brokers: vec!["localhost:9092".to_string()],
            topic: "default-topic".to_string(),
            partition: 0,
            group_id: None,
            start_offset: "latest".to_string(),
        }
    }
}

/// Kafka source implementation
pub struct KafkaSource {
    config: KafkaSourceConfig,
    running: Arc<AtomicBool>,
    sender: Option<mpsc::Sender<PipelineMessage>>,
}

impl KafkaSource {
    pub fn new(brokers: Vec<String>, topic: String) -> Self {
        Self::with_config(KafkaSourceConfig {
            brokers,
            topic,
            ..Default::default()
        })
    }

    pub fn with_config(config: KafkaSourceConfig) -> Self {
        Self {
            config,
            running: Arc::new(AtomicBool::new(false)),
            sender: None,
        }
    }

    fn is_running_inner(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    async fn consume_messages(&self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Connecting to Kafka brokers: {:?}", self.config.brokers);

        // Build client
        let client = ClientBuilder::new(self.config.brokers.clone()).build().await?;
        info!("Kafka client created successfully");

        // Get partition client
        let partition_client = client
            .partition_client(
                self.config.topic.clone(),
                self.config.partition,
                UnknownTopicHandling::Retry,
            )
            .await?;

        info!("Got partition client for {}-{}", self.config.topic, self.config.partition);

        // Determine start offset - for "latest" we'll start from 0 and let it fetch latest
        let start_offset = if self.config.start_offset == "earliest" {
            0
        } else {
            0
        };

        info!("Starting consumption from offset: {}", start_offset);

        let running = self.running.clone();
        let sender = self.sender.clone().unwrap();
        let topic = self.config.topic.clone();
        let partition = self.config.partition;

        tokio::spawn(async move {
            let mut current_offset = start_offset;

            while running.load(Ordering::Relaxed) {
                match partition_client
                    .fetch_records(current_offset, 1..100_000, 1000)
                    .await
                {
                    Ok((records, high_watermark)) => {
                        let record_count = records.len();
                        debug!("Fetched {} records, high_watermark: {}", record_count, high_watermark);

                        for record in &records {
                            let offset = record.offset;

                            info!("Kafka message: topic={}, partition={}, offset={}",
                                topic, partition, offset);

                            let key_len = record.record.key.as_ref().map(|k| k.len()).unwrap_or(0);
                            let value_len = record.record.value.as_ref().map(|v| v.len()).unwrap_or(0);
                            let headers_count = record.record.headers.len();

                            debug!("Key length: {}, Payload length: {}, Headers: {}",
                                key_len, value_len, headers_count);

                            // Try to parse as Debezium format
                            if let Some(payload) = &record.record.value {
                                if let Ok(debezium) = serde_json::from_slice::<DebeziumFormat>(payload) {
                                    let source_table = format!("{}.{}", debezium.source.db, debezium.source.table);

                                    // Use route config if available, otherwise use same table
                                    let route = RouteInfo::new(source_table.clone(), source_table);
                                    let msg = PipelineMessage::with_single_route(debezium, route);

                                    if let Err(e) = sender.try_send(msg) {
                                        error!("Failed to send message to pipeline: {}", e);
                                    } else {
                                        debug!("Message sent to pipeline successfully");
                                    }
                                } else {
                                    warn!("Received non-Debezium message, skipping. Payload: {}",
                                        String::from_utf8_lossy(payload));
                                }
                            } else {
                                warn!("Received message with null payload, skipping");
                            }

                            current_offset = offset + 1;
                        }

                        // Update to high watermark if no new records
                        if record_count == 0 && current_offset < high_watermark {
                            current_offset = high_watermark;
                        }
                    }
                    Err(e) => {
                        error!("Error fetching records from Kafka: {}", e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }

                // Small delay to avoid tight loop when no new messages
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            info!("Kafka consumer loop stopped");
        });

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

        info!("Starting Kafka source for topic: {}", self.config.topic);

        if self.sender.is_none() {
            return Err("Sender not set. Call set_sender() before start()".into());
        }

        self.running.store(true, Ordering::Relaxed);

        // Start consuming messages in background
        self.consume_messages().await?;

        info!("Kafka source started successfully");
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
        assert_eq!(config.topic, "default-topic");
        assert_eq!(config.partition, 0);
        assert_eq!(config.start_offset, "latest");
    }

    #[test]
    fn test_kafka_source_creation() {
        let source = KafkaSource::new(
            vec!["localhost:9092".to_string()],
            "test-topic".to_string(),
        );
        assert!(!source.is_running());
    }
}
