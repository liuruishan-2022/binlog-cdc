use thiserror::Error;

///
/// 自定义错误的枚举类型
///

#[derive(Error, Debug)]
pub enum MyError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("SQL parse error: {0}")]
    Parse(#[from] sqlparser::parser::ParserError),
}
