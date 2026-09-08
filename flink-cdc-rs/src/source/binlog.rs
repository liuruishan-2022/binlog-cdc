use std::{
    collections::HashMap,
    fs::File,
    io::{self, ErrorKind, Seek},
    sync::Arc,
};

use mysql_binlog_connector_rust::{
    binlog_error::BinlogError, binlog_parser::BinlogParser, event::{event_data::EventData, table_map_event::TableMapEvent},
};
use tokio::sync::mpsc::Sender;

use crate::{
    config::{CdcConfig, source::BinlogFile},
    mysql::schema::{TableMeta, TableSchema},
    pipeline::message::PipelineRecord,
};

///
/// 当来源是来自binlog文件的时候的解析
///
/// 不过我们还是需要区分mysql binlog/pg binlog/oracle binlog等等
///

type TableMetaCache = HashMap<String, HashMap<u64, Arc<TableMeta>>>;

pub struct MysqlBinlogFile<'a> {
    config: &'a CdcConfig,
    binlog_file: &'a BinlogFile,
    channels: Vec<Sender<PipelineRecord>>,
    table_schema: TableSchema,
    table_meta_cache: TableMetaCache,
}

impl<'a> MysqlBinlogFile<'a> {
    pub async fn create(
        config: &'a CdcConfig,
        binlog_file: &'a BinlogFile,
        channels: Vec<Sender<PipelineRecord>>,
    ) -> Self {
        let table_schemta = TableSchema::new(&binlog_file.url())
            .await
            .expect("create mysql table schema error");
        MysqlBinlogFile {
            config: config,
            binlog_file: binlog_file,
            channels: channels,
            table_schema: table_schemta,
            table_meta_cache: HashMap::new(),
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
                        match data {
                            EventData::TableMap(event) => {
                                //todo
                            }
                            EventData::WriteRows(event) => {}
                            EventData::UpdateRows(event) => {}
                            EventData::DeleteRows(event) => {}
                            _ => {
                                //ignore the event data
                            }
                        }
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

    async fn record_table_meta(&mut self, event: TableMapEvent) {
        let cache = self.table_meta_cache.entry(key)
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
