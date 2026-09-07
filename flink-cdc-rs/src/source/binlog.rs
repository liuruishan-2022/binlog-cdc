use std::{
    collections::HashMap,
    fs::File,
    intrinsics::breakpoint,
    io::{self, ErrorKind, Seek},
};

use mysql_binlog_connector_rust::{binlog_error::BinlogError, binlog_parser::BinlogParser};
use tokio::sync::mpsc::Sender;

use crate::{
    config::{CdcConfig, source::BinlogFile},
    pipeline::message::PipelineRecord,
};

///
/// 当来源是来自binlog文件的时候的解析
///
/// 不过我们还是需要区分mysql binlog/pg binlog/oracle binlog等等
///

pub struct MysqlBinlogFile<'a> {
    config: &'a CdcConfig,
    binlog_file: &'a BinlogFile,
    channels: Vec<Sender<PipelineRecord>>,
}

impl<'a> MysqlBinlogFile<'a> {
    pub fn create(
        config: &'a CdcConfig,
        binlog_file: &'a BinlogFile,
        channels: Vec<Sender<PipelineRecord>>,
    ) -> Self {
        MysqlBinlogFile {
            config: config,
            binlog_file: binlog_file,
            channels: channels,
        }
    }

    pub async fn read(&self) {
        match globwalk::glob(self.binlog_file.path()) {
            Ok(walker) => {
                for file in walker {
                    if let Ok(ele) = file {
                        let mut file = File::open(ele.path());
                    }
                }
            }
            Err(err) => {
                tracing::warn!("path of:{} err:{}", self.binlog_file.path(), err);
            }
        }
    }

    async fn parse_binlog(file: &str) -> Result<(), io::Error> {
        let mut file = File::open(file)?;

        let mut parser = BinlogParser {
            checksum_length: 4,
            table_map_event_by_table_id: HashMap::new(),
        };

        let file_len = file.metadata().map(|metadata| metadata.len()).unwrap_or(0);
        match parser.check_magic(&mut file) {
            Ok(_) => loop {
                let event_start_pos = file.stream_position().unwrap_or(0);
                match parser.next(&mut file) {
                    Ok((header, data)) => {
                        //todo 封装成PipelineRecord的数据格式
                    }
                    Err(BinlogError::IoError(err)) if err.kind() == ErrorKind::UnexpectedEof => {
                        if event_start_pos == file_len {
                            tracing::info!("binlog parse ok");
                        } else {
                            tracing::error!(
                                "binlog have EOF error,of offset:{} of file_len:{}",
                                event_start_pos,
                                file_len
                            );
                        }
                        break;
                    }
                    Err(err) => {
                        tracing::error!("parse binlog file data failed:{:?}", err);
                        break;
                    }
                }
            },
            Err(err) => {
                tracing::warn!("magic number error:{:?}", err);
            }
        }

        Ok(())
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

        for ele in globwalk::glob("/home/liuxu/Pictures/*").unwrap() {
            if let Ok(ele) = ele {
                tracing::info!("{:?}", ele.path());
            }
        }
    }
}
