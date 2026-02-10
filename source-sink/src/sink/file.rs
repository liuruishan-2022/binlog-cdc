//! File-based sink implementation

use crate::sink::Sink;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::info;

pub struct FileSink {
    path: String,
    running: Arc<AtomicBool>,
}

impl FileSink {
    pub fn new(path: String) -> Self {
        Self {
            path,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    fn is_running_inner(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl Sink for FileSink {
    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.is_running_inner() {
            info!("File sink is already running");
            return Ok(());
        }

        info!("Starting file sink to: {}", self.path);
        self.running.store(true, Ordering::Relaxed);
        info!("File sink started (placeholder implementation)");
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.is_running_inner() {
            info!("File sink is not running");
            return Ok(());
        }

        info!("Stopping file sink");
        self.running.store(false, Ordering::Relaxed);
        info!("File sink stopped");
        Ok(())
    }

    async fn write(&mut self, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        if !self.is_running_inner() {
            return Err("File sink is not running".into());
        }

        info!("Writing {} bytes to file: {}", data.len(), self.path);
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    fn is_ready(&self) -> bool {
        self.is_running_inner()
    }
}
