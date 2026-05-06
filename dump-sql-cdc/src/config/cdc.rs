use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct CdcConfig {
    kafka: Kafka,
    cdc: Cdc,
}

impl CdcConfig {
    pub fn read_from(config_path: &str) -> Self {
        let config_file =
            std::fs::read_to_string(config_path).expect("读取配置文件失败,请查看具体的错误信息");
        serde_yaml::from_str(&config_file).expect("解析配置文件失败")
    }

    pub fn kafka(&self) -> &Kafka {
        &self.kafka
    }

    pub fn cdc(&self) -> &Cdc {
        &self.cdc
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Kafka {
    #[serde(rename = "bootstrap-servers")]
    bootstrap_servers: String,
}

impl Kafka {
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Cdc {
    parallelism: Option<u32>,
    path: String,
    capacity: Option<u64>,
    password: Option<String>,
    #[serde(rename = "channel-capacity")]
    channel_capacity: Option<u32>,
    #[serde(rename = "producer-threads")]
    producer_threads: Option<u32>,
    routes: Vec<Route>,
}

impl Cdc {
    const DEFAULT_CAPACITY: u64 = 100_000;
    const DEFAULT_PARALLELISM: u32 = 4;
    const DEFAULT_CHANNEL_CAPACITY: u32 = 1_000_000;
    const DEFAULT_PRODUCER_THREADS: u32 = 4;

    pub fn parallelism(&self) -> u32 {
        self.parallelism.unwrap_or(Self::DEFAULT_PARALLELISM)
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn capacity(&self) -> u64 {
        self.capacity.unwrap_or(Self::DEFAULT_CAPACITY)
    }

    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    pub fn channel_capacity(&self) -> u32 {
        self.channel_capacity
            .unwrap_or(Self::DEFAULT_CHANNEL_CAPACITY)
    }

    pub fn producer_threads(&self) -> u32 {
        self.producer_threads
            .unwrap_or(Self::DEFAULT_PRODUCER_THREADS)
    }

    pub fn routes(&self) -> &[Route] {
        &self.routes
    }

    pub fn search_topic(&self, table_name: &str) -> Option<String> {
        self.routes()
            .iter()
            .find(|route| table_name.starts_with(route.table()))
            .map(|route| route.topic().to_string())
    }

    pub fn search_partition(&self, topic: &str) -> u32 {
        self.routes()
            .iter()
            .find(|route| route.topic() == topic)
            .map(|route| route.partition())
            .unwrap_or(Route::DEFAULT_PARTITION)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Route {
    table: String,
    topic: String,
    partition: Option<u32>,
}

impl Route {
    const DEFAULT_PARTITION: u32 = 3;

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> u32 {
        self.partition.unwrap_or(Self::DEFAULT_PARTITION)
    }
}
