use mysql_binlog_connector_rust::binlog_error::BinlogError;

///
/// 定义自己项目模块的Error类型
///

#[derive(Error, Debug)]
pub enum CdcError {
    BinlogIo(#[from] BinlogError::IoError),
    BinlogUnexpected(#[from] BinlogError::UnexpectedError),
    Other(String),
}
