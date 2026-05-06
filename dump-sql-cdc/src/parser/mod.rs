use std::sync::Arc;

use crate::{config::cdc::CdcConfig, parser::dumpsql::MysqlDumpSqlParser};

///
/// 放置解析sql文件的逻辑
/// 其实整体的逻辑不是很复杂:
/// 1. 读取sql文件
/// 2. 执行解析
/// 3. 发送到Kafka指定的Topic中
///
pub mod debezium;
pub mod dumpsql;
pub mod error;
pub mod insert;
pub mod sql_parser;

pub async fn start_parse(config: Arc<CdcConfig>) {
    let mut parser = MysqlDumpSqlParser::new(config).await;
    parser.start().await;
}
