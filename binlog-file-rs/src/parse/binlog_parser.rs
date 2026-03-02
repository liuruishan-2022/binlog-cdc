use std::{collections::HashMap, fs::File};

use mysql_binlog_connector_rust::binlog_parser::BinlogParser;
use tracing::{info, warn};

pub async fn parse_binlog(file: &str) {
    let mut file = File::open(file).expect(format!("读取文件失败:{}", file).as_str());
    let mut parser = BinlogParser {
        checksum_length: 4,
        table_map_event_by_table_id: HashMap::new(),
    };
    let mut count = 0;
    match parser.check_magic(&mut file) {
        Ok(_) => loop {
            match parser.next(&mut file) {
                Ok((_header, _data)) => {
                    count = count + 1;
                    if count % 1000000 == 0 {
                        info!("成功解析:{count}条binlog数据的event");
                    }
                }
                Err(err) => {
                    warn!("解析binlog数据失败:{:?}", err);
                }
            }
        },
        Err(e) => {
            warn!("验证magic number错误:{}", e);
        }
    }
}
