//! Configuration module for source-sink
//!
//! Provides YAML-based configuration loading for pipelines, sources, and sinks.

pub mod args;
pub mod pipeline;
pub mod sink;
pub mod source;

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

use crate::config::pipeline::Pipeline;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Route {
    #[serde(rename = "source-table")]
    pub source_table: String,

    #[serde(rename = "sink-table")]
    pub sink_table: String,
}

impl Route {
    pub fn new(source_table: String, sink_table: String) -> Self {
        Self {
            source_table,
            sink_table,
        }
    }

    pub fn source_table(&self) -> &str {
        &self.source_table
    }

    pub fn sink_table(&self) -> &str {
        &self.sink_table
    }

    pub fn source_database(&self) -> Option<&str> {
        self.source_table.split('.').next()
    }

    pub fn source_table_name(&self) -> &str {
        self.source_table
            .split('.')
            .last()
            .unwrap_or(&self.source_table)
    }
}

pub fn load_config<P: AsRef<Path>>(config_path: P) -> Config {
    let content = fs::read_to_string(config_path).expect("Unable to read config file");
    serde_yaml::from_str(&content).expect("Unable to parse config file")
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Config {
    pub source: source::SourceConfig,

    pub sink: sink::SinkConfig,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<Pipeline>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub route: Option<Vec<Route>>,
}

impl Config {
    pub fn new(
        source: source::SourceConfig,
        sink: sink::SinkConfig,
        pipeline: Option<Pipeline>,
        route: Option<Vec<Route>>,
    ) -> Self {
        Self {
            source,
            sink,
            pipeline,
            route,
        }
    }

    pub fn source(&self) -> &source::SourceConfig {
        &self.source
    }

    pub fn sink(&self) -> &sink::SinkConfig {
        &self.sink
    }

    pub fn pipeline(&self) -> Option<&Pipeline> {
        self.pipeline.as_ref()
    }

    pub fn route(&self) -> Option<&Vec<Route>> {
        self.route.as_ref()
    }

    pub fn has_pipeline(&self) -> bool {
        self.pipeline.is_some()
    }

    pub fn has_route(&self) -> bool {
        self.route.is_some() && !self.route.as_ref().unwrap().is_empty()
    }

    pub fn find_sink_table(&self, source_table: &str) -> Option<String> {
        self.route.as_ref()?.iter().find_map(|route| {
            if route.source_table == source_table {
                Some(route.sink_table.clone())
            } else {
                None
            }
        })
    }
}
