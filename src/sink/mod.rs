use crate::binlog::row::DebeziumFormat;

pub mod kafka_sink;
pub mod mysql_sink;

///
/// 在Sink侧统一处理批量的DebeziumFormat数据的trait定义
/// 然后使用dyn trait的方式来实现
pub trait SinkStream {
    async fn handle_messages(&self, messages: Vec<DebeziumFormat>);
}
