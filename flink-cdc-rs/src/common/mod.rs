use std::{fmt::Display, sync::Arc};

use chrono::{Local, TimeZone, offset::LocalResult};
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, gauge::Gauge},
    registry::Registry,
};
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::warn;

pub mod schema;

///
/// 目前这个 mod 下放置一些杂项：错误类型、监控指标/labels 等
///

#[derive(Error, Debug)]
pub enum CdcError {
    BinlogIo(String),
    BinlogUnexpected(String),
    Other(String),
}

impl Display for CdcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CdcError::BinlogIo(msg) => write!(f, "BinlogIo: {}", msg),
            CdcError::BinlogUnexpected(msg) => write!(f, "BinlogUnexpected: {}", msg),
            CdcError::Other(msg) => write!(f, "Other: {}", msg),
        }
    }
}

pub fn format_timestamp(timestamp: i64) -> String {
    let millis = timestamp / 1000;
    match Local.timestamp_millis_opt(millis) {
        LocalResult::Single(time) => time.format("%Y-%m-%d %H:%M:%S").to_string(),
        _ => {
            warn!("timestamp is invalid:{}!", timestamp);
            "".to_string()
        }
    }
}

pub async fn register_metrics(registry: Arc<Mutex<Registry>>) -> Metrics {
    let mut registry = registry.lock().await;
    let metrics = Metrics::default();
    metrics.register(&mut registry);
    return metrics;
}

///
/// 有关监控的 labels 与指标定义
///
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct EventLabel {
    type_name: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct DescTableLabel {
    db_name: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct KafkaLabel {
    event: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct BinlogEventLabel {}

/// 整个服务的所有指标都定义在这里
#[derive(Debug)]
pub struct Metrics {
    flink_mysql_cdc: Family<EventLabel, Counter>,
    flink_mysql_desc_table: Family<DescTableLabel, Counter>,
    flink_sink_kafka_message: Family<KafkaLabel, Counter>,
    flink_mysql_binlog_event_timestamp: Family<BinlogEventLabel, Gauge<i64>>,
}

impl Metrics {
    pub fn default() -> Self {
        Metrics {
            flink_mysql_cdc: Family::default(),
            flink_mysql_desc_table: Family::default(),
            flink_sink_kafka_message: Family::default(),
            flink_mysql_binlog_event_timestamp: Family::default(),
        }
    }

    pub fn register(&self, registry: &mut Registry) {
        registry.register("flink_mysql_cdc", "flink mysql cdc event count", self.flink_mysql_cdc.clone());
        registry.register("flink_mysql_desc_table", "flink mysql desc table command total count", self.flink_mysql_desc_table.clone());
        registry.register("flink_sink_kafka_message", "flink sink kafka message send count total", self.flink_sink_kafka_message.clone());
        registry.register("flink_mysql_binlog_event_timestamp", "flink mysql binlog event timestamp", self.flink_mysql_binlog_event_timestamp.clone());
    }

    pub fn inc_flink_mysql_cdc(&self, type_name: &str) {
        self.flink_mysql_cdc.get_or_create(&EventLabel { type_name: type_name.to_string() }).inc();
    }

    pub fn inc_flink_mysql_desc_table(&self, db_name: &str) {
        self.flink_mysql_desc_table.get_or_create(&DescTableLabel { db_name: db_name.to_string() }).inc();
    }

    pub fn inc_flink_sink_kafka_message(&self, event: &str, count: u64) {
        self.flink_sink_kafka_message.get_or_create(&KafkaLabel { event: event.to_string() }).inc_by(count);
    }

    pub fn stat_binlog_event_timestamp(&self, timestamp: u32) {
        self.flink_mysql_binlog_event_timestamp.get_or_create(&BinlogEventLabel {}).set(timestamp as i64);
    }
}


///
/// channel 深度/占用率指标: 判别 source 与 sink 谁是瓶颈的关键数据
/// 深度持续=容量 → sink 消费不动; 深度持续≈0 → source 供不上
///
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ChannelLabel {
    index: i64,
}

pub struct ChannelMetrics {
    flink_channel_depth: Family<ChannelLabel, Gauge>,
    flink_channel_usage_percent: Family<ChannelLabel, Gauge>,
}

impl ChannelMetrics {
    pub fn register(registry: &mut Registry) -> Self {
        let depth = Family::default();
        let usage = Family::default();
        registry.register(
            "flink_channel_depth",
            "channel 当前队列深度(消息数)",
            depth.clone(),
        );
        registry.register(
            "flink_channel_usage_percent",
            "channel 占用率百分比(0-100)",
            usage.clone(),
        );
        Self {
            flink_channel_depth: depth,
            flink_channel_usage_percent: usage,
        }
    }

    pub fn set(&self, index: usize, depth: usize, capacity: usize) {
        let label = ChannelLabel { index: index as i64 };
        self.flink_channel_depth.get_or_create(&label).set(depth as i64);
        let usage = if capacity > 0 {
            (depth as f64 / capacity as f64 * 100.0) as i64
        } else {
            0
        };
        self.flink_channel_usage_percent
            .get_or_create(&label)
            .set(usage);
    }
}
