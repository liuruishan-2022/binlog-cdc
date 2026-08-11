use std::fmt::Display;

use chrono::{Local, TimeZone, offset::LocalResult};
use thiserror::Error;
use tracing::warn;

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
