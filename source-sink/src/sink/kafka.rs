//! Kafka-based sink implementation
//!
//! Provides high-performance Kafka producer using rskafka.

use crate::sink::Sink;
use async_trait::async_trait;
use rskafka::client::partition::PartitionClient;
use rskafka::client::Client;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::{debug, info};

#[derive(Debug, Clone)]
pub struct KafkaSinkConfig {
    pub brokers: Vec<String>,

    pub topic: String,

    pub partition: i32,

    pub required_acks: i16,

    pub ack_timeout_ms: i32,

    pub max_buffer_time: std::time::Duration,
}

impl Default for KafkaSinkConfig {
    fn default() -> Self {
        Self {
            brokers: vec!["localhost:9092".to_string()],
            topic: "default-topic".to_string(),
            partition: -1,
            required_acks: 1,
            ack_timeout_ms: 5000,
            max_buffer_time: std::time::Duration::from_millis(10),
        }
    }
}

/// Kafka sink implementation
pub struct KafkaSink {
    config: KafkaSinkConfig,
    client: Option<Client>,
    partition_client: Option<PartitionClient>,
    running: Arc<AtomicBool>,
}

impl KafkaSink {
    pub fn new(brokers: Vec<String>, topic: String) -> Self {
        Self::with_config(KafkaSinkConfig {
            brokers,
            topic,
            ..Default::default()
        })
    }

    pub fn with_config(config: KafkaSinkConfig) -> Self {
        Self {
            config,
            client: None,
            partition_client: None,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    fn is_running_inner(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    async fn connect(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Connecting to Kafka brokers: {:?}", self.config.brokers);

        let client = Client::new_with_opts(
            self.config.brokers.clone(),
            Default::default(),
        )
        .await?;

        self.client = Some(client);
        Ok(())
    }

    async fn ensure_partition_client(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.partition_client.is_none() {
            let client = self.client.as_ref().ok_or("Client not initialized")?;

            let partition = if self.config.partition < 0 {
                0
            } else {
                self.config.partition
            };

            let partition_client = client.partition_client(
                &self.config.topic,
                partition,
                rskafka::client::partition::UnknownTopicHandling::Retry,
            ).await?;

            self.partition_client = Some(partition_client);
        }
        Ok(())
    }
}

#[async_trait]
impl Sink for KafkaSink {
    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.is_running_inner() {
            info!("Kafka sink is already running");
            return Ok(());
        }

        info!("Starting Kafka sink for topic: {}", self.config.topic);

        self.connect().await?;
        self.running.store(true, Ordering::Relaxed);

        info!("Kafka sink started successfully");
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.is_running_inner() {
            info!("Kafka sink is not running");
            return Ok(());
        }

        info!("Stopping Kafka sink");

        // Flush any pending messages
        self.flush().await?;

        self.running.store(false, Ordering::Relaxed);
        self.partition_client = None;
        self.client = None;

        info!("Kafka sink stopped");
        Ok(())
    }

    async fn write(&mut self, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        if !self.is_running_inner() {
            return Err("Kafka sink is not running".into());
        }

        self.ensure_producer().await?;

        let producer = self.producer.as_mut().ok_or("Producer not initialized")?;

        let record = Record {
            key: None,
            value: Some(data),
            headers: vec![],
            timestamp: chrono::Utc::now().timestamp_millis(),
        };

        producer.produce(record).await?;

        debug!("Record produced to topic: {}", self.config.topic);
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(producer) = &mut self.producer {
            producer.flush().await?;
            debug!("Flushed Kafka producer");
        }
        Ok(())
    }

    fn is_ready(&self) -> bool {
        self.is_running_inner() && self.producer.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = KafkaSinkConfig::default();
        assert_eq!(config.brokers, vec!["localhost:9092"]);
        assert_eq!(config.topic, "default-topic");
        assert_eq!(config.partition, -1);
        assert_eq!(config.required_acks, 1);
    }

    #[test]
    fn test_kafka_sink_creation() {
        let sink = KafkaSink::new(
            vec!["localhost:9092".to_string()],
            "test-topic".to_string(),
        );
        assert!(!sink.is_ready());
    }
}
