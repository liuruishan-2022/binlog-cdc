//! Console sink implementation for debugging/monitoring

use crate::sink::Sink;
use async_trait::async_trait;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::info;

pub struct ConsoleSink {
    running: Arc<AtomicBool>,
}

impl ConsoleSink {
    pub fn new() -> Self {
        Self {
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    fn is_running_inner(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl Sink for ConsoleSink {
    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.is_running_inner() {
            info!("Console sink is already running");
            return Ok(());
        }

        info!("Starting console sink");
        self.running.store(true, Ordering::Relaxed);
        info!("Console sink started");
        Ok(())
    }

    async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.is_running_inner() {
            info!("Console sink is not running");
            return Ok(());
        }

        info!("Stopping console sink");
        self.running.store(false, Ordering::Relaxed);
        info!("Console sink stopped");
        Ok(())
    }

    async fn write(&mut self, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        if !self.is_running_inner() {
            return Err("Console sink is not running".into());
        }

        if let Ok(s) = String::from_utf8(data) {
            println!("{}", s);
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        std::io::stdout().flush()?;
        Ok(())
    }

    fn is_ready(&self) -> bool {
        self.is_running_inner()
    }
}
