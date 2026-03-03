use clap::Parser;
use source_sink::{config::args::Args, pipeline::Pipeline};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    Pipeline::create().start();

    Ok(())
}
