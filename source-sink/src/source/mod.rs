//! Source module for data ingestion
//!
//! This module provides abstractions and implementations for various data sources.

pub mod console;
pub mod file;
pub mod kafka;
pub mod mysql;

///
/// 提供一个Source的trait,定义基本的数据源操作
pub trait Source {}
