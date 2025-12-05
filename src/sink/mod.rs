use crate::binlog::row::DebeziumFormat;

pub mod kafka_sink;

///
/// 在Sink侧统一处理批量的DebeziumFormat数据的trait定义
pub trait SinkStream {
    async fn send_batch_messages(&self, messages: Vec<DebeziumFormat>);
}
