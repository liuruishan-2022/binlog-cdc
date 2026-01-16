use std::{collections::HashMap, fs::File};

use mysql_binlog_connector_rust::binlog_parser::BinlogParser;
use tracing::{info, warn};

pub async fn parse_binlog(file: &str) {
    let mut file = File::open(file).expect(format!("读取文件失败:{}", file).as_str());
    let mut parser = BinlogParser {
        checksum_length: 4,
        table_map_event_by_table_id: HashMap::new(),
    };
    match parser.check_magic(&mut file) {
        Ok(_) => loop {
            if let Ok((header, data)) = parser.next(&mut file) {
                info!("成功解析一条binlog数据event");
            } else {
                warn!("解析binlog数据失败");
            }
        },
        Err(e) => {
            warn!("验证magic number错误:{}", e);
        }
    }
}
