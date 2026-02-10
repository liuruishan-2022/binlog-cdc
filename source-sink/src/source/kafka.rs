//! Kafka-based source implementation
//!
//! Provides high-performance Kafka consumer using rskafka.

use crate::source::Source;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::{info, warn};

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
        }
    }

    fn is_running_inner(&self) -> bool {
        self.running.load(Ordering::Relaxed)
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
        self.running.store(true, Ordering::Relaxed);

        // TODO: Implement rskafka 0.6 consumer
        warn!("Kafka source started (placeholder implementation)");
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.is_running_inner() {
            info!("Kafka source is not running");
            return Ok(());
        }

        info!("Stopping Kafka source");
        self.running.store(false, Ordering::Relaxed);
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
