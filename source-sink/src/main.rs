use clap::Parser;
use source_sink::config::{load_config, source, sink};
use source_sink::pipeline::Pipeline;
use source_sink::source::BoxSource;
use source_sink::sink::BoxSink;
use tracing::info;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the configuration file
    #[arg(short, long)]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    info!("Starting source-sink application");
    info!("Loading config from: {}", args.config);

    let config = load_config(&args.config);

    let source = create_source(&config.source)?;
    let sink = create_sink(&config.sink)?;

    let pipeline_name = config
        .pipeline()
        .map(|p| p.name().to_string())
        .unwrap_or_else(|| "Default Pipeline".to_string());

    info!("Creating pipeline: {}", pipeline_name);
    let mut pipeline = Pipeline::new_single(pipeline_name, source, sink);

    info!("Starting pipeline...");
    pipeline.start().await?;

    info!("Pipeline is running. Press Ctrl+C to stop.");

    tokio::signal::ctrl_c().await?;
    info!("Received shutdown signal");

    info!("Stopping pipeline...");
    pipeline.stop().await?;

    info!("Application stopped");
    Ok(())
}

fn create_source(config: &source::SourceConfig) -> Result<BoxSource, Box<dyn std::error::Error>> {
    match config {
        source::SourceConfig::Kafka(cfg) => {
            info!("Creating Kafka source: {}", cfg.name);
            Ok(BoxSource::new(Box::new(source_sink::source::kafka::KafkaSource::with_config(
                source_sink::source::kafka::KafkaSourceConfig {
                    brokers: cfg.bootstrap_servers.clone(),
                    topic: cfg.topic.clone().unwrap_or_default(),
                    partition: cfg.partition.unwrap_or(0),
                    group_id: Some(cfg.group_id.clone()),
                    start_offset: cfg.start_offset.clone(),
                },
            ))))
        }
        source::SourceConfig::MySql(cfg) => {
            info!("Creating MySQL source: {}", cfg.name);
            Ok(BoxSource::new(Box::new(source_sink::source::mysql::MySqlSource::with_config(
                source_sink::source::mysql::MySqlSourceConfig {
                    connection_url: cfg.connection_url.clone(),
                    query: cfg.query.clone(),
                    poll_interval: cfg.poll_interval_secs,
                    max_connections: cfg.max_connections,
                    min_connections: 5,
                    connect_timeout: cfg.connect_timeout_secs,
                },
            ))))
        }
        source::SourceConfig::File(cfg) => {
            info!("Creating File source: {}", cfg.name);
            Ok(BoxSource::new(Box::new(source_sink::source::file::FileSource::new(
                cfg.name.clone(),
                cfg.path.clone(),
            ))))
        }
        source::SourceConfig::Console(cfg) => {
            info!("Creating Console source: {}", cfg.name);
            Ok(BoxSource::new(Box::new(source_sink::source::console::ConsoleSource::new(
                cfg.name.clone(),
            ))))
        }
    }
}

fn create_sink(config: &sink::SinkConfig) -> Result<BoxSink, Box<dyn std::error::Error>> {
    match config {
        sink::SinkConfig::Kafka(cfg) => {
            info!("Creating Kafka sink: {}", cfg.name);
            Ok(BoxSink::new(Box::new(source_sink::sink::kafka::KafkaSink::with_config(
                source_sink::sink::kafka::KafkaSinkConfig {
                    brokers: cfg.bootstrap_servers.clone(),
                    topic: cfg.topic.clone(),
                    partition: -1,
                    required_acks: cfg.required_acks,
                    ack_timeout_ms: cfg.ack_timeout_ms,
                    max_buffer_time: std::time::Duration::from_millis(cfg.linger_ms as u64),
                },
            ))))
        }
        sink::SinkConfig::MySql(cfg) => {
            info!("Creating MySQL sink: {}", cfg.name);
            Ok(BoxSink::new(Box::new(source_sink::sink::mysql::MySqlSink::with_config(
                source_sink::sink::mysql::MySqlSinkConfig {
                    connection_url: cfg.connection_url(),
                    max_connections: cfg.max_connections,
                    min_connections: 1,
                    connect_timeout: cfg.connect_timeout_secs,
                    idle_timeout: 600,
                    max_lifetime: 1800,
                    use_tls: false,
                    cache_ttl_secs: 300,
                    cache_max_capacity: 10000,
                },
            ))))
        }
        sink::SinkConfig::File(cfg) => {
            info!("Creating File sink: {}", cfg.name);
            Ok(BoxSink::new(Box::new(source_sink::sink::file::FileSink::new(
                cfg.path.clone(),
            ))))
        }
        sink::SinkConfig::Console(cfg) => {
            info!("Creating Console sink: {}", cfg.name);
            Ok(BoxSink::new(Box::new(source_sink::sink::console::ConsoleSink::new())))
        }
    }
}
