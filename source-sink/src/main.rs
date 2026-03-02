use clap::Parser;
use source_sink::config::{load_config, sink, source};
use source_sink::pipeline::Pipeline;
use source_sink::sink::BoxSink;
use std::sync::Arc;
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

    Ok(())
}
