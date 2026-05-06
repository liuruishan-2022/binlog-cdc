use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use chrono::{Local, Utc};
use rand::Rng;
use rskafka::{
    client::{
        ClientBuilder,
        partition::{PartitionClient, UnknownTopicHandling},
    },
    record::Record,
};
use tokio::sync::Mutex;
use tracing::warn;

use crate::{config::cdc::CdcConfig, parser::debezium::DebeziumFormat};

type Producer = PartitionClient;

pub struct KafkaSink {
    config: Arc<CdcConfig>,
    client: rskafka::client::Client,
    partition_clients: Arc<Mutex<HashMap<String, Arc<Producer>>>>,
}

impl KafkaSink {
    pub async fn create(config: Arc<CdcConfig>) -> Self {
        let servers = config
            .kafka()
            .bootstrap_servers()
            .split(',')
            .map(|ele| ele.to_string())
            .collect::<Vec<String>>();
        let client = ClientBuilder::new(servers).build().await.unwrap();

        KafkaSink {
            client,
            config,
            partition_clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn producer(&self, topic: &str, partition: u32) -> Arc<Producer> {
        let key = format!("{}:{}", topic, partition);

        {
            let clients = self.partition_clients.lock().await;
            if let Some(client) = clients.get(&key) {
                return client.clone();
            }
        }

        let producer = Arc::new(
            self.client
                .partition_client(topic, partition as i32, UnknownTopicHandling::Error)
                .await
                .map_err(|e| format!("Failed to create partition client for {}: {}", key, e))
                .expect("Create partition client error"),
        );
        let mut clients = self.partition_clients.lock().await;
        clients.insert(key, producer.clone());

        producer
    }

    pub async fn send_messages(&self, messages: Vec<DebeziumFormat>, topic: &str) {
        let partition_count = self.config.cdc().search_partition(topic);

        let records = messages
            .into_iter()
            .map(|ele| ele.into())
            .collect::<Vec<Record>>();

        let random_partition = if partition_count > 0 {
            rand::thread_rng().gen_range(0..partition_count)
        } else {
            0
        };
        let producer = self.producer(topic, random_partition).await;

        if self.config.cdc().is_send_to_kafka() {
            if let Err(e) = producer
                .produce(records, rskafka::client::partition::Compression::Lz4)
                .await
            {
                warn!("发送消息失败:{:?}", e);
            }
        }
    }
}

impl From<DebeziumFormat> for Record {
    fn from(ele: DebeziumFormat) -> Self {
        let body = ele.to_json();
        let key = Local::now().timestamp_millis();
        let key = key.to_string().as_bytes().to_vec();

        Record {
            key: Some(key.clone()),
            value: Some(body.as_bytes().to_vec()),
            headers: BTreeMap::from([("key".to_string(), key)]),
            timestamp: Utc::now(),
        }
    }
}
