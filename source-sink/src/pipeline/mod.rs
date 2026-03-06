//! Pipeline module
//!
//! Provides data pipeline implementation for connecting sources and sinks.

pub mod message;
pub mod pipeline;

pub use message::{PipelineMessage, RouteInfo};
pub use pipeline::Pipeline;
