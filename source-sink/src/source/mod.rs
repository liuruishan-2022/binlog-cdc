//! Source module for data ingestion
//!
//! This module provides abstractions and implementations for various data sources.

pub mod console;
pub mod file;
pub mod kafka;
pub mod mysql;

use async_trait::async_trait;
use tokio::sync::mpsc;

#[async_trait]
pub trait Source: Send + Sync {
    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>>;

    async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>>;

    fn is_running(&self) -> bool;

    fn set_sender(&mut self, sender: mpsc::Sender<crate::pipeline::message::PipelineMessage>);
}

/// Type-erased source wrapper
pub struct BoxSource {
    inner: Box<dyn Source + Send + Sync>,
}

impl BoxSource {
    pub fn new(inner: Box<dyn Source + Send + Sync>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl Source for BoxSource {
    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.inner.start().await
    }

    async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.inner.stop().await
    }

    fn is_running(&self) -> bool {
        self.inner.is_running()
    }

    fn set_sender(&mut self, sender: mpsc::Sender<crate::pipeline::message::PipelineMessage>) {
        self.inner.set_sender(sender);
    }
}
