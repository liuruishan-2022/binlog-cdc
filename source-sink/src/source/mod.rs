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
    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    fn is_running(&self) -> bool;

    fn set_sender(&mut self, sender: mpsc::Sender<crate::pipeline::message::PipelineMessage>);

    async fn start_with_config(&mut self, _config: &crate::config::Config) -> Result<(), Box<dyn std::error::Error>> {
        self.start().await
    }
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

    async fn start_with_config(&mut self, config: &crate::config::Config) -> Result<(), Box<dyn std::error::Error>> {
        self.inner.start_with_config(config).await
    }
}
