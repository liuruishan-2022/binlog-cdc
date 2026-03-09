use std::fmt::Display;

use thiserror::Error;

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
