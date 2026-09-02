use tokio::sync::mpsc::Sender;

use crate::{config::CdcConfig, pipeline::message::PipelineRecord};

///
/// 当来源是来自binlog文件的时候的解析
///
/// 不过我们还是需要区分mysql binlog/pg binlog/oracle binlog等等
///

pub struct MysqlBinlogFile<'a> {
    config: &'a CdcConfig,
    channels: Vec<Sender<PipelineRecord>>,
}

impl<'a> MysqlBinlogFile<'a> {
    pub fn create(config: &'a CdcConfig, channels: Vec<Sender<PipelineRecord>>) -> Self {
        MysqlBinlogFile {
            config: config,
            channels: channels,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::LocalTimer;

    fn init_log() {
        tracing_subscriber::fmt()
            .with_timer(LocalTimer)
            .with_line_number(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .init();
    }

    #[test]
    fn test_glob_walker() {
        init_log();
        for ele in globwalk::glob("/home/liuxu/Pictures/**/*.png").unwrap() {
            if let Ok(ele) = ele {
                tracing::info!("{:?}", ele.path());
            }
        }
    }
}
