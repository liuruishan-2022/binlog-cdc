use crate::{
    config::{CdcConfig, sink, source},
    sink::mysql_sink::MysqlSink,
    source::kafka_source::KafkaSource,
};

///
/// 提供无限stream的能力对接
/// 临时先给一个: Kafka ---> Mysql的能力

pub(crate) async fn pipeline(config: &CdcConfig) {
    match (config.source(), config.sink()) {
        (source::Source::Kafka(kafka), sink::Sink::Mysql(mysql)) => {
            let sink = MysqlSink::new(&mysql.url()).await;
            let source = KafkaSource::new(sink, kafka, config);
            source.start().await;
        }
        _ => {
            tracing::warn!("do not support this config");
        }
    }
}
