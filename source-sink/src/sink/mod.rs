//! Sink module for data output
//!
//! This module provides abstractions and implementations for various data sinks.

pub mod console;
pub mod file;
pub mod kafka;
pub mod mysql;

use async_trait::async_trait;

#[async_trait]
pub trait Sink: Send + Sync {
    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn write(&mut self, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error>>;

    async fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>>;

    fn is_ready(&self) -> bool;
}
