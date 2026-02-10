//! File-based source implementation

use crate::source::Source;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

pub struct FileSource {
    name: String,
    path: String,
    running: Arc<AtomicBool>,
}

impl FileSource {
    pub fn new(name: String, path: String) -> Self {
        Self {
            name,
            path,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    fn is_running_inner(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl Source for FileSource {
    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.is_running_inner() {
            info!("File source is already running");
            return Ok(());
        }

        info!("Starting file source: {} from {}", self.name, self.path);
        self.running.store(true, Ordering::Relaxed);
        info!("File source started (placeholder implementation)");
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.is_running_inner() {
            info!("File source is not running");
            return Ok(());
        }

        info!("Stopping file source");
        self.running.store(false, Ordering::Relaxed);
        info!("File source stopped");
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.is_running_inner()
    }

    fn set_sender(&mut self, _sender: mpsc::Sender<crate::pipeline::message::PipelineMessage>) {
        // File source doesn't use sender in current implementation
    }
}
