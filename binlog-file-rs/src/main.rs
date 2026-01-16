use clap::Parser;
use tracing::info;
use tracing_subscriber::fmt::{format::Writer, time::FormatTime};

use crate::args::arguments::Args;

pub mod args;
pub mod parse;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_line_number(true)
        .with_timer(LocalTimer)
        .init();
    info!("启动解析binlog文件...");

    let args = Args::parse();
    parse::binlog_parser::parse_binlog(args.file()).await;
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
