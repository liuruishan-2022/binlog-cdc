//! Console source implementation

use crate::source::Source;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

pub struct ConsoleSource {
    name: String,
    running: Arc<AtomicBool>,
}

impl ConsoleSource {
    pub fn new(name: String) -> Self {
        Self {
            name,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    fn is_running_inner(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl Source for ConsoleSource {
    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.is_running_inner() {
            info!("Console source is already running");
            return Ok(());
        }

        info!("Starting console source: {}", self.name);
        self.running.store(true, Ordering::Relaxed);
        info!("Console source started (placeholder implementation)");
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.is_running_inner() {
            info!("Console source is not running");
            return Ok(());
        }

        info!("Stopping console source");
        self.running.store(false, Ordering::Relaxed);
        info!("Console source stopped");
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.is_running_inner()
    }

    fn set_sender(&mut self, _sender: mpsc::Sender<crate::pipeline::message::PipelineMessage>) {
        // Console source doesn't use sender in current implementation
    }
}
