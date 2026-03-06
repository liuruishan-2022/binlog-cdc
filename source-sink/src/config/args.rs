//! Command-line arguments for source-sink
//!
//! Provides argument parsing using clap.

use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Path to the configuration file
    #[arg(short, long)]
    pub config: String,
}

impl Args {
    pub fn new(config: String) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &str {
        &self.config
    }
}
