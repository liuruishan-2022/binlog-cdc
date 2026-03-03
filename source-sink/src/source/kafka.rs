//! Kafka-based source implementation
//!
//! Provides high-performance Kafka consumer using rskafka.

use crate::source::Source;
use kafka::consumer::Consumer;
use rskafka::client::partition::{PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use rskafka::topic::Topic;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::time::Duration;
use tracing::info;

/// Kafka source implementation with multi-topic support
pub struct KafkaSource {
    consumer: Consumer,
}

impl KafkaSource {
    pub fn create(connections: &str) -> Self {
        let connections = connections
            .split(',')
            .map(|s| s.trim().to_string())
            .collect::<Vec<String>>();
        let mut builder = Consumer::from_hosts(connections)
            .with_group("liuxu-test-kafka-rust-rs".to_string())
            .with_fallback_offset(kafka::client::FetchOffset::Earliest)
            .with_fetch_max_wait_time(Duration::from_secs(1))
            .with_fetch_min_bytes(1_000)
            .with_fetch_max_bytes_per_partition(100_000)
            .with_retry_max_bytes_limit(1_000_000)
            .with_offset_storage(Some(kafka::client::GroupOffsetStorage::Kafka))
            .with_client_id("kafka-rust-console-consumer".into());
        builder = builder.with_topic("kafka-press".into());
        let consumer = builder.create().expect("created Kafka Client error");

        return KafkaSource { consumer: consumer };
    }

    fn consumer(&mut self) {
        loop {
            for messages in self
                .consumer
                .poll()
                .expect("consumer message failed")
                .iter()
            {
                for message in messages.messages() {
                    info!(
                        "kafka message:{} {} {}",
                        message.offset,
                        String::from_utf8_lossy(message.key),
                        String::from_utf8_lossy(message.value)
                    );
                }
                let _ = self.consumer.consume_messageset(messages);
            }
            self.consumer
                .commit_consumed()
                .expect("commit consumed failed");
        }
    }
}

///
/// 实现Source这个trait,提供Kafka的基本的操作
impl Source for KafkaSource {
    fn consumer(&mut self) {
        self.consumer();
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
