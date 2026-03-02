//! Pipeline implementation

use crate::config::Config;
use crate::pipeline::message::{PipelineMessage, RouteInfo};
use crate::sink::Sink;
use crate::source::Source;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

const CHANNEL_CAPACITY: usize = 10000;

///
/// Pipeline的实现思想是: 通过source和sink之间建立一个消息流通信,
/// 是通过channel进行线程之间的通信方式

pub struct Pipeline<S, SINK>
where
    S: Source + Send + Sync,
    SINK: Sink + Send + Sync,
{
    source: Option<S>,

    sinks: Vec<SINK>,

    tx: Option<mpsc::Sender<PipelineMessage>>,

    running: Arc<AtomicBool>,

    name: String,

    config: Option<Arc<Config>>,
}

impl<S, SINK> Pipeline<S, SINK>
where
    S: Source + Send + Sync + 'static,
    SINK: Sink + Send + Sync + 'static,
{
    pub fn new(name: String, source: S, sinks: Vec<SINK>) -> Self {
        Self {
            source: Some(source),
            sinks,
            tx: None,
            running: Arc::new(AtomicBool::new(false)),
            name,
            config: None,
        }
    }

    pub fn new_single(name: String, source: S, sink: SINK) -> Self {
        Self::new(name, source, vec![sink])
    }

    pub fn with_config(mut self, config: Arc<Config>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn process_message(
        sink: &mut SINK,
        msg: &PipelineMessage,
        route_info: &RouteInfo,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let json_bytes = msg.to_json()?.into_bytes();

        debug!(
            "Processing message: source_table={}, sink_table={}",
            route_info.source_table(),
            route_info.sink_table()
        );

        sink.write(json_bytes).await?;

        Ok(())
    }

    pub async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.is_running() {
            info!("Pipeline '{}' is not running", self.name);
            return Ok(());
        }

        info!("Stopping pipeline '{}'", self.name);

        self.running.store(false, Ordering::Relaxed);

        self.tx = None;

        for sink in &mut self.sinks {
            if let Err(e) = sink.stop().await {
                warn!("Error stopping sink: {}", e);
            }
        }

        info!("Pipeline '{}' stopped", self.name);
        Ok(())
    }

    pub fn sender(&self) -> Option<mpsc::Sender<PipelineMessage>> {
        self.tx.clone()
    }
}

pub struct PipelineBuilder;

impl PipelineBuilder {
    pub fn from_config(_config: &Config) -> Result<(), Box<dyn std::error::Error>> {
        Err("Pipeline building from config not yet implemented".into())
    }
}
