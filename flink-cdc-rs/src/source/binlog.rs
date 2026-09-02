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
