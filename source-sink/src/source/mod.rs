//! Source module for data ingestion
//!
//! This module provides abstractions and implementations for various data sources.

pub mod file;
pub mod kafka;
pub mod mysql;

use async_trait::async_trait;

#[async_trait]
pub trait Source: Send + Sync {
    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>>;

    async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>>;

    fn is_running(&self) -> bool;
}
