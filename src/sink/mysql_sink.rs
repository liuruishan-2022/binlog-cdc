use sqlx::MySqlPool;
use tracing::warn;

use crate::{binlog::schema::ColumnMeta, sink::SinkStream};

///
/// 放置mysql作为sink的处理代码
/// 我在想，上一个数据源和下一个目标数据源如何交互
/// 我们在插入到mysql的时候,首先是根据debezium的json的op决定是:删除/更新/插入的操作
/// 然后,我们如何进行具体的操作:
/// 1. 插入的时候，直接全部就行了
/// 2. 更新的时候,根据primary key来决定如何更新，所有的字段,那么如何获取到primary key.定时缓存失效的方式来处理
/// 3. 删除的时候,也是根据primary key来进行删除
/// 还需要考虑到:如果表没有primary key,怎么处理删除和更新的动作?
///
pub struct MysqlSink {
    pool: MySqlPool,
}

impl MysqlSink {
    pub async fn new(url: &str) -> Self {
        let pool = MySqlPool::connect(url)
            .await
            .expect(format!("connect to mysql use:{} failed", url).as_str());
        MysqlSink { pool }
    }

    pub async fn desc_table(&self, table_id: u64, db_name: &str, table_name: &str) -> TableMeta {
        let sql = format!("desc `{}`.{}", db_name, table_name);
        let mut rows = sqlx::query(&sql).fetch(&self.pool);

        let mut source_position = 1;
        let mut columns = vec![];
        while let Some(row) = rows.try_next().await.unwrap() {
            let field: &str = row.try_get("Field").expect("fetch desc table field error!");
            let key: Result<Vec<u8>, sqlx::Error> = row.try_get("Key");
            let key = Self::judge_primary_key(key);

            columns.push(ColumnMeta {
                ordinal_position: source_position,
                column_name: field.to_string(),
                is_primaty_key: key,
            });
            source_position = source_position + 1;
        }

        return TableMeta::new(
            table_id,
            db_name.to_string(),
            table_name.to_string(),
            columns,
        );
    }

    fn judge_primary_key(key: Result<Vec<u8>, sqlx::Error>) -> bool {
        match key {
            Ok(key) => match String::from_utf8(key) {
                Ok(key) => key == "PRI",
                Err(err) => {
                    warn!("can not convert to utf8:{:?}!", err);
                    false
                }
            },
            Err(err) => {
                warn!("error:{:?}", err);
                false
            }
        }
    }
}

impl SinkStream for MysqlSink {
    async fn handle_messages(&self, messages: Vec<crate::binlog::row::DebeziumFormat>) {
        todo!("等待实现");
    }
}

///
/// 放置table的元数据信息的struct
///
#[derive(Debug)]
pub struct TableMeta {
    db_name: String,
    table_name: String,
    columns: Vec<ColumnMeta>,
    primary_keys: Vec<String>,
}
