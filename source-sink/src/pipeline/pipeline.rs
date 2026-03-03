//! Pipeline implementation

use crate::config::Config;
use crate::pipeline::message::{PipelineMessage, RouteInfo};
use crate::sink::Sink;
use crate::source::Source;
use crate::source::kafka::KafkaSource;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

///
/// Pipeline的实现思想是: 通过source和sink之间建立一个消息流通信,
/// 是通过channel进行线程之间的通信方式

pub struct Pipeline {
    source: Box<dyn Source>,
}

impl Pipeline {
    pub fn create() -> Self {
        let source = KafkaSource::create("172.16.1.135:9092,172.16.1.118:9092,172.16.1.149:9092");
        Pipeline {
            source: Box::new(source),
        }
    }
}
