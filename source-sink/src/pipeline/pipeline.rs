//! Pipeline implementation

use crate::source::Source;
use crate::source::kafka::KafkaSource;

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

    pub fn start(&mut self) {
        self.source.consumer();
    }
}
