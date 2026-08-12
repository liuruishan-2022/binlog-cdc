use std::{fmt::Display, sync::Arc};

use chrono::{Local, TimeZone, offset::LocalResult};
use prometheus_client::registry::Registry;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::warn;

use crate::binlog::Metrics;

///
/// 目前这个mod下放置一些杂项，暂时不多，所以不做拆分，暂时放置如下的信息
/// 1. 错误类型
/// 2. 监控的lables这些

///
/// 定义自己项目模块的Error类型
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

///
/// 放置监控等对应的信息的
///

pub async fn register_metrics(registry: Arc<Mutex<Registry>>) -> Metrics {
    let mut registry = registry.lock().await;
    let metrics = Metrics::default();
    metrics.register(&mut registry);
    return metrics;
}
