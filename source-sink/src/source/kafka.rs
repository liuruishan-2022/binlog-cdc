//! Kafka-based source implementation
//!
//! Provides high-performance Kafka consumer using rskafka.

use crate::common::DebeziumFormat;
use crate::config::Config;
use crate::pipeline::message::{PipelineMessage, RouteInfo};
use crate::source::Source;
use async_trait::async_trait;
use rskafka::client::partition::PartitionClient;
use rskafka::client::partition::UnknownTopicHandling;
use rskafka::client::{Client, ClientBuilder};
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
    client: Client,
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

        return KafkaSource { client: client };
    }
}

///
/// 实现Source这个trait,提供Kafka的基本的操作
impl Source for KafkaSource {}
