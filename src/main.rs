use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    extract::State,
    http::{Response, header::CONTENT_TYPE},
    response::IntoResponse,
    routing::get,
};
use prometheus_client::{encoding::text::encode, registry::Registry};
use tokio::sync::Mutex;
use tracing::info;
use tracing_subscriber::fmt::{format::Writer, time::FormatTime};

use crate::{args::arguments::Args, config::cdc::FlinkCdc};
use clap::Parser;

pub mod args;
pub mod binlog;
pub mod config;
pub mod pipeline;
pub mod savepoint;
pub mod sink;
pub mod source;
pub mod transform;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_timer(LocalTimer)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .init();
    info!("start flink cdc task...");

    let args = Args::parse();
    info!("args:{}!", args.to_string());
    let flink_cdc_path = args.flink_cdc().to_string();

    let registry = Arc::new(Mutex::new(Registry::default()));
    let _registry_binlog = registry.clone();

    tokio::spawn(async move {
        //临时屏蔽掉binlog的代码,测试下从Kafka消费数据写入到Mysql的功能
        //let _config = FlinkCdc::read_from(&flink_cdc_path);
        //binlog::dump_and_parse(registry_binlog, &config).await;
        pipeline::start_pipeline(&flink_cdc_path).await;
    });

    let registry_metrics = registry.clone();
    init_axum(registry_metrics).await;
}

async fn init_axum(registry: Arc<Mutex<Registry>>) {
    let app = Router::new()
        .route("/version", get(version))
        .route("/metrics", get(metrics_handler))
        .with_state(registry);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:9249")
        .await
        .expect("start metrics web server error!");
    axum::serve(listener, app)
        .await
        .expect("run metrics web server error!");
}

async fn version() -> &'static str {
    "0.1.0"
}
async fn metrics_handler(State(state): State<Arc<Mutex<Registry>>>) -> impl IntoResponse {
    let state = state.lock().await;
    let mut buffer = String::new();
    encode(&mut buffer, &state).unwrap();
    return Response::builder()
        .header(
            CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )
        .body(Body::from(buffer))
        .unwrap();
}

struct LocalTimer;

const fn east_utf8() -> Option<chrono::FixedOffset> {
    chrono::FixedOffset::east_opt(8 * 3600)
}

impl FormatTime for LocalTimer {
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        let now = chrono::Utc::now().with_timezone(&east_utf8().unwrap());
        write!(w, "{}", now.format("%FT%T%.3f"))
    }
}
