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

/// Type-erased sink wrapper
pub struct BoxSink {
    inner: Box<dyn Sink + Send + Sync>,
}

impl BoxSink {
    pub fn new(inner: Box<dyn Sink + Send + Sync>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl Sink for BoxSink {
    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.inner.start().await
    }

    async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.inner.stop().await
    }

    async fn write(&mut self, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        self.inner.write(data).await
    }

    async fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.inner.flush().await
    }

    fn is_ready(&self) -> bool {
        self.inner.is_ready()
    }
}
