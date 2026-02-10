//! Common utilities and data structures
//!
//! This module contains shared utilities including Debezium format support.

pub mod debezium;

pub use debezium::{DebeziumFormat, DebeziumSource, MessageKey, format_timestamp};
