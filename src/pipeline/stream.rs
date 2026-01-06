use tracing::info;
use url::Url;

use crate::{
    sink::mysql_sink::MysqlSink,
    source::kafka_source::KafkaSource,
};

///
/// 提供无限stream的能力对接
/// 临时先给一个: Kafka ---> Mysql的能力

pub struct StreamPipeline {
    source: KafkaSource<MysqlSink>,
}

impl StreamPipeline {
    pub async fn new() -> Self {
        let sink = MysqlSink::new(&Self::url()).await;
        let source = KafkaSource::new(sink);
        StreamPipeline { source }
    }

    pub fn url() -> String {
        let uri = format!("mysql://{}:{}", "172.16.1.77", "3306");
        let mut uri = Url::parse(&uri).unwrap();
        let _ = uri.set_username("root");
        let _ = uri.set_password(Some("jx@xw!@#$~|{}"));

        return uri.as_str().to_string();
    }

    pub async fn start(&self) {
        info!("start stream pipeline Kafka ---> Mysql");
        self.source.start().await;
    }
}
