use std::{collections::HashMap, fs::File, io::{ErrorKind, Seek}};

use mysql_binlog_connector_rust::{binlog_error::BinlogError, binlog_parser::BinlogParser};
use tracing::{error, info, warn};

pub async fn parse_binlog(file: &str) {
    let mut file = File::open(file).expect(format!("读取文件失败:{}", file).as_str());
    let mut parser = BinlogParser {
        checksum_length: 4,
        table_map_event_by_table_id: HashMap::new(),
    };
    let mut count = 0;
    let file_len = file.metadata().map(|metadata| metadata.len()).unwrap_or(0);
    match parser.check_magic(&mut file) {
        Ok(_) => loop {
            let event_start_pos = file.stream_position().unwrap_or(0);
            match parser.next(&mut file) {
                Ok((_header, _data)) => {
                    count = count + 1;
                    if count % 1000000 == 0 {
                        info!("成功解析:{count}条binlog数据的event");
                    }
                }
                Err(BinlogError::IoError(err)) if err.kind() == ErrorKind::UnexpectedEof => {
                    if event_start_pos == file_len {
                        info!("binlog文件解析完成,共解析:{count}条binlog数据的event");
                    } else {
                        error!("binlog文件可能不完整,在offset:{event_start_pos}读取event时遇到EOF,文件长度:{file_len}");
                    }
                    break;
                }
                Err(err) => {
                    error!("解析binlog数据失败:{:?}", err);
                    break;
                }
            }
        },
        Err(e) => {
            warn!("验证magic number错误:{}", e);
        }
    }
}
