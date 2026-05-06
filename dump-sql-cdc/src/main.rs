use std::sync::Arc;

use tracing::info;
use tracing_subscriber::fmt::{format::Writer, time::FormatTime};

pub mod config;
pub mod kafka;
pub mod parser;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    tracing_subscriber::fmt()
        .with_timer(LocalTimer)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_line_number(true)
        .init();
    info!("启动解析mysqldump系列的sql文件服务...");

    let config = Arc::new(config::read_config());

    let thread_count = config.cdc().parallelism() as usize;
    info!("启动{}个线程进行解析...", thread_count);
    rayon::ThreadPoolBuilder::new()
        .num_threads(thread_count)
        .build_global()
        .expect("设置线程个数失败...");
    parser::start_parse(config).await;
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
