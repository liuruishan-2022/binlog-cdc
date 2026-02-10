//! Kafka-based source implementation
//!
//! Provides high-performance Kafka consumer using rskafka.

use crate::source::Source;
use async_trait::async_trait;
use rskafka::client::Client;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::{debug, error, info};

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
    client: Option<Client>,
    running: Arc<AtomicBool>,
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
            client: None,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    fn is_running_inner(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    async fn connect(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!(
            "Connecting to Kafka brokers: {:?}",
            self.config.brokers
        );

        let client = Client::new_with_opts(
            self.config.brokers.clone(),
            Default::default(),
        )
        .await?;

        self.client = Some(client);
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

        self.connect().await?;
        self.running.store(true, Ordering::Relaxed);

        let client = self.client.as_ref().ok_or("Client not initialized")?;
        let running = self.running.clone();
        let topic = self.config.topic.clone();
        let partition = self.config.partition;

        // Spawn consumer task
        tokio::spawn(async move {
            info!(
                "Starting consumer for topic: {}, partition: {}",
                topic, partition
            );

            // Use record consumer builder directly
            let mut consumer = match client
                .record_consumer(&topic, partition)
                .await
            {
                Ok(builder) => {
                    match builder
                        .with_start_offset(rskafka::client::consumer::OffsetAt::AtLatest)
                        .build()
                        .await
                    {
                        Ok(c) => c,
                        Err(e) => {
                            error!("Failed to build consumer: {}", e);
                            return;
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to create record consumer: {}", e);
                    return;
                }
            };

            while running.load(Ordering::Relaxed) {
                match consumer.recv().await {
                    Ok(record) => {
                        debug!(
                            "Received record: topic={}, partition={}, offset={}",
                            topic, partition, record.offset
                        );
                        // Process record here
                    }
                    Err(e) => {
                        error!("Error receiving record: {}", e);
                        if !running.load(Ordering::Relaxed) {
                            break;
                        }
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                }
            }

            info!("Consumer stopped for topic: {}", topic);
        });

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
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        info!("Kafka source stopped");
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.is_running_inner()
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
