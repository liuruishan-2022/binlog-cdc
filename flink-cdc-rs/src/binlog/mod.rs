use std::sync::Arc;

use mysql_binlog_connector_rust::event::event_data::EventData;
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, gauge::Gauge},
    registry::Registry,
};
use tokio::sync::Mutex;
use tracing::info;

use crate::{
    binlog::dump::Dumper,
    common::CdcError,
    config::cdc::FlinkCdc,
};

pub mod dump;
pub mod event_channel;
pub mod row;
pub mod schema;

///
/// 新旧版本的速度对比,我们都是以相同的binlog的文件和数据内容做比对的
/// 1. 旧版本的速度大概是:18分钟,然后一直耗费:1C,基本上是100%
/// 2. 新版本的速度大概是:3分钟,然后CPU基本上是在: 130%左右,但是这个Receivers的消费者没有真正的处理消息内容
///
/// 总体看,是能够提升6倍的速度,也就是说,这个基本上是极限了,因为Sender端基本上是固定的了.单线程无法扩充了
pub async fn start_dump(
    registry: Arc<Mutex<Registry>>,
    config: &FlinkCdc,
) -> Result<i32, CdcError> {
    info!("use channel pipeline hanle binlog cdc...");
    let mut dumper = Dumper::new(config, registry).await?;
    dumper.start(config).await?;
    Ok(0)
}

///
/// 有关监控的一些配置信息
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct EventLabel {
    type_name: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct DescTableLabel {
    db_name: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct KafkaLabel {
    event: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct BinlogEventLabel {}

///
/// 整个服务的所有指标都定义在这里了
#[derive(Debug)]
pub struct Metrics {
    ///
    /// 记录mysql binlog cdc event的个数(按照不同的类型进行区分)
    flink_mysql_cdc: Family<EventLabel, Counter>,

    ///
    /// 监控table-map-event真实请求mysql的个数
    flink_mysql_desc_table: Family<DescTableLabel, Counter>,

    ///
    /// 监控发送到kafka的消息个数
    flink_sink_kafka_message: Family<KafkaLabel, Counter>,

    ///
    /// 记录读取到的binlog event的事件戳
    flink_mysql_binlog_event_timestamp: Family<BinlogEventLabel, Gauge<i64>>,
}

impl Metrics {
    pub fn default() -> Self {
        Metrics {
            flink_mysql_cdc: Family::default(),
            flink_mysql_desc_table: Family::default(),
            flink_sink_kafka_message: Family::default(),
            flink_mysql_binlog_event_timestamp: Family::default(),
        }
    }

    pub fn register(&self, registry: &mut Registry) {
        registry.register(
            "flink_mysql_cdc",
            "flink mysql cdc event count",
            self.flink_mysql_cdc.clone(),
        );
        registry.register(
            "flink_mysql_desc_table",
            "flink mysql desc table command total count",
            self.flink_mysql_desc_table.clone(),
        );
        registry.register(
            "flink_sink_kafka_message",
            "flink sink kafka message send count total",
            self.flink_sink_kafka_message.clone(),
        );
        registry.register(
            "flink_mysql_binlog_event_timestamp",
            "flink mysql binlog event timestamp",
            self.flink_mysql_binlog_event_timestamp.clone(),
        );
    }

    pub fn inc_flink_mysql_cdc(&self, type_name: &str) {
        let labels = EventLabel {
            type_name: type_name.to_string(),
        };
        self.flink_mysql_cdc.get_or_create(&labels).inc();
    }

    pub fn inc_flink_mysql_desc_table(&self, db_name: &str) {
        self.flink_mysql_desc_table
            .get_or_create(&DescTableLabel {
                db_name: db_name.to_string(),
            })
            .inc();
    }

    pub fn inc_flink_sink_kafka_message(&self, event: &str, count: u64) {
        self.flink_sink_kafka_message
            .get_or_create(&KafkaLabel {
                event: event.to_string(),
            })
            .inc_by(count);
    }

    pub fn stat_binlog_event_timestamp(&self, timestamp: u32) {
        self.flink_mysql_binlog_event_timestamp
            .get_or_create(&BinlogEventLabel {})
            .set(timestamp as i64);
    }
}

///
/// 提供一个能够存储Event信息的struct
/// 在各个thread之间进行传递
/// 整体的流程我们准备写成一个Pipeline
/// 这种风格的操作,然后数据在不同的线程之间进行流转
///
pub struct BinlogEventData {
    binlog: String,
    event_data: EventData,
}

impl BinlogEventData {
    pub fn new(binlog: String, event_data: EventData) -> Self {
        BinlogEventData {
            binlog: binlog,
            event_data: event_data,
        }
    }
}
