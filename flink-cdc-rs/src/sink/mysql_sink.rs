use futures_util::TryStreamExt;
use moka::sync::Cache;
use sqlx::MySqlPool;
use sqlx::Row;
use tracing::info;
use tracing::warn;

use crate::binlog::row::DebeziumFormat;
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
    cache: Cache<String, TableMeta>,
}

impl MysqlSink {
    pub async fn new(url: &str) -> Self {
        let pool = MySqlPool::connect(url)
            .await
            .expect(format!("connect to mysql use:{} failed", url).as_str());
        let cache = Cache::new(10000);
        MysqlSink { pool, cache }
    }

    pub async fn table_info(&self, _source: &str) -> TableMeta {
        let table = "sedp_biz_test.sedp_gsms_user";
        if let Some(meta) = self.cache.get(table) {
            return meta;
        }

        self.desc_table(table).await
    }

    ///
    /// 现在陷入了一个难题,是如何获取到topic的名称,因为需要根据topic来决定使用哪个表
    pub async fn delete(&self, debezium: &DebeziumFormat, topic: &str) {
        let meta = self.table_info(topic).await;
        let where_sql = meta
            .primary_keys()
            .iter()
            .map(|ele| {
                let data = debezium
                    .before_column(ele)
                    .expect("fetch column data error")
                    .to_string();
                format!("{} = '{}'", ele, data)
            })
            .collect::<Vec<String>>()
            .join(" AND ");
        let sql = format!("DELETE FROM {} WHERE {}", meta.table(), where_sql);
        info!("执行mysql的删除操作:{}", sql);
    }

    pub async fn insert_data(&self, debezium: &DebeziumFormat, topic: &str) {
        let meta = self.table_info(topic).await;
        let columns = meta
            .columns
            .iter()
            .map(|ele| ele.column_name().to_string())
            .collect::<Vec<String>>()
            .join(", ");
        let values = meta
            .columns
            .iter()
            .map(|ele| {
                let data = debezium
                    .after_column(ele.column_name())
                    .map(|val| format!("{}", val.to_string()));
                return data;
            })
            .filter_map(|ele| ele)
            .collect::<Vec<String>>()
            .join(", ");
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            meta.table(),
            columns,
            values
        );
        info!("执行mysql的插入操作:{}", sql);
    }

    pub async fn update_data(&self, debezium: &DebeziumFormat, topic: &str) {
        let meta = self.table_info(topic).await;
        let set_sql = meta
            .columns
            .iter()
            .map(|ele| {
                let data = debezium
                    .after_column(ele.column_name())
                    .map(|val| format!("{} = '{}'", ele.column_name(), val.to_string()));
                return data;
            })
            .filter_map(|ele| ele)
            .collect::<Vec<String>>()
            .join(", ");
        let where_sql = meta
            .primary_keys()
            .iter()
            .map(|ele| {
                let data = debezium
                    .before_column(ele)
                    .map(|val| format!("{} = '{}'", ele, val.to_string()));
                return data;
            })
            .filter_map(|ele| ele)
            .collect::<Vec<String>>()
            .join(" AND ");
        let sql = format!(
            "UPDATE {} SET {} WHERE {}",
            meta.table(),
            set_sql,
            where_sql
        );
        info!("执行mysql的更新操作:{}", sql);
    }

    async fn desc_table(&self, table: &str) -> TableMeta {
        let sql = format!("desc {}", table);
        let mut rows = sqlx::query(&sql).fetch(&self.pool);

        let mut source_position = 1;
        let mut columns = vec![];
        while let Some(row) = rows.try_next().await.unwrap() {
            let field: &str = row.try_get("Field").expect("fetch desc table field error!");
            let key: Result<Vec<u8>, sqlx::Error> = row.try_get("Key");
            let key = Self::judge_primary_key(key);

            columns.push(ColumnMeta::new(source_position, field.to_string(), key));
            source_position = source_position + 1;
        }

        return TableMeta::new(table.to_string(), columns);
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
    async fn process(&self, debezium: &DebeziumFormat, topic: &str) {
        match debezium.op() {
            "d" => {
                info!("执行删除的动作");
                self.delete(debezium, topic).await;
            }
            "c" => {
                info!("执行create动作");
                self.insert_data(debezium, topic).await;
            }
            "u" => {
                info!("执行更新的动作");
                self.update_data(debezium, topic).await;
            }
            _ => {
                warn!("未知的操作类型:{}", debezium.op());
            }
        }
    }
}

///
/// 放置table的元数据信息的struct
///
#[derive(Debug, Clone)]
pub struct TableMeta {
    table: String,
    columns: Vec<ColumnMeta>,
    primary_keys: Vec<String>,
}

impl TableMeta {
    pub fn new(table: String, columns: Vec<ColumnMeta>) -> Self {
        let keys = columns
            .iter()
            .filter(|ele| ele.is_primary())
            .map(|ele| ele.column_name().to_string())
            .collect::<Vec<String>>();
        TableMeta {
            table,
            columns,
            primary_keys: keys,
        }
    }

    pub fn primary_keys(&self) -> &Vec<String> {
        &self.primary_keys
    }

    pub fn table(&self) -> &str {
        self.table.as_str()
    }
}
