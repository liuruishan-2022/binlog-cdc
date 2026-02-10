//! Pipeline implementation

use crate::config::Config;
use crate::pipeline::message::{PipelineMessage, RouteInfo};
use crate::sink::Sink;
use crate::source::Source;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

const CHANNEL_CAPACITY: usize = 10000;

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
        }
    }

    pub fn new_single(name: String, source: S, sink: SINK) -> Self {
        Self::new(name, source, vec![sink])
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.is_running() {
            info!("Pipeline '{}' is already running", self.name);
            return Ok(());
        }

        info!("Starting pipeline '{}'", self.name);

        let (tx, mut rx) = mpsc::channel(CHANNEL_CAPACITY);
        self.tx = Some(tx);

        let mut source = self.source.take().ok_or("Source already consumed")?;
        source.start().await?;
        info!("Pipeline '{}': source started", self.name);

        for sink in &mut self.sinks {
            sink.start().await?;
        }
        info!("Pipeline '{}': {} sink(s) started", self.name, self.sinks.len());

        self.running.store(true, Ordering::Relaxed);

        let running = self.running.clone();
        let pipeline_name = self.name.clone();
        let _sink_count = self.sinks.len();

        let mut sinks = std::mem::take(&mut self.sinks);

        tokio::spawn(async move {
            info!("Pipeline '{}': message processor started", pipeline_name);

            while running.load(Ordering::Relaxed) {
                match rx.recv().await {
                    Some(msg) => {
                        debug!(
                            "Pipeline '{}': received message, routes: {}",
                            pipeline_name,
                            msg.route_count()
                        );

                        for route_info in msg.routes() {
                            for sink in &mut sinks {
                                if let Err(e) = Self::process_message(sink, &msg, route_info).await {
                                    error!(
                                        "Pipeline '{}': error processing message: {}",
                                        pipeline_name, e
                                    );
                                }
                            }
                        }
                    }
                    None => {
                        debug!("Pipeline '{}': channel closed", pipeline_name);
                        break;
                    }
                }
            }

            info!("Pipeline '{}': message processor stopped", pipeline_name);
        });

        info!("Pipeline '{}' started successfully", self.name);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{DebeziumFormat, MessageKey};
    use crate::config::{source, sink};
    use serde_json::json;

    struct MockSource {
        running: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl Source for MockSource {
        async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
            self.running.store(true, Ordering::Relaxed);
            Ok(())
        }

        async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
            self.running.store(false, Ordering::Relaxed);
            Ok(())
        }

        fn is_running(&self) -> bool {
            self.running.load(Ordering::Relaxed)
        }
    }

    impl MockSource {
        fn new() -> Self {
            Self {
                running: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    struct MockSink {
        running: Arc<AtomicBool>,
        data: std::sync::Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    }

    #[async_trait::async_trait]
    impl Sink for MockSink {
        async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
            self.running.store(true, Ordering::Relaxed);
            Ok(())
        }

        async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
            self.running.store(false, Ordering::Relaxed);
            Ok(())
        }

        async fn write(&mut self, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
            let mut data_store = self.data.lock().unwrap();
            data_store.push(data);
            Ok(())
        }

        async fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
            Ok(())
        }

        fn is_ready(&self) -> bool {
            self.running.load(Ordering::Relaxed)
        }
    }

    impl MockSink {
        fn new() -> Self {
            Self {
                running: Arc::new(AtomicBool::new(false)),
                data: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        fn received_count(&self) -> usize {
            self.data.lock().unwrap().len()
        }
    }

    #[tokio::test]
    async fn test_pipeline_creation() {
        let source = MockSource::new();
        let sink = MockSink::new();

        let pipeline = Pipeline::new_single("test-pipeline".to_string(), source, sink);
        assert_eq!(pipeline.name(), "test-pipeline");
        assert!(!pipeline.is_running());
    }

    #[tokio::test]
    async fn test_pipeline_start_stop() {
        let source = MockSource::new();
        let sink = MockSink::new();

        let mut pipeline = Pipeline::new_single("test-pipeline".to_string(), source, sink);
        pipeline.start().await.unwrap();
        assert!(pipeline.is_running());

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        pipeline.stop().await.unwrap();
        assert!(!pipeline.is_running());
    }

    #[test]
    fn test_route_info() {
        let route = RouteInfo::new("db.table".to_string(), "sink.table".to_string());
        assert_eq!(route.source_table(), "db.table");
        assert_eq!(route.sink_table(), "sink.table");
    }

    #[test]
    fn test_pipeline_message() {
        let data = DebeziumFormat::insert(
            json!({"id": 1}),
            "db",
            "table",
            MessageKey::new(Default::default()),
        );

        let route = RouteInfo::new("db.table".to_string(), "sink.table".to_string());
        let msg = PipelineMessage::with_single_route(data, route);

        assert_eq!(msg.route_count(), 1);
        assert_eq!(msg.operation(), "c");
        assert!(msg.has_routes());
    }
}
