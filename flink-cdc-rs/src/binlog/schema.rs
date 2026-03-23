use hashbrown::HashMap;

use futures_util::TryStreamExt;
use mysql_binlog_connector_rust::event::table_map_event::TableMapEvent;
use sqlx::MySqlPool;
use sqlx::Row;
use tracing::info;
use tracing::warn;

use crate::binlog::Metrics;
use crate::common::CdcError;
use crate::config::cdc::FlinkCdc;
use crate::config::cdc::TableInclude;

///
/// 主要是获取对应表的columns信息，用来做debezium的json的转换
/// 看了下Flink CDC 3的源码实现，看到使用的是SHOW/DESC/SHOW CREATE这类的语法实现的，所以我们也使用这类语法进行实现
///

pub struct TableSchema {
    pool: MySqlPool,
}

impl TableSchema {
    pub async fn new(url: &str) -> Result<Self, CdcError> {
        let pool = MySqlPool::connect(url)
            .await
            .map_err(|e| CdcError::Other(format!("Connection mysql error: {:?}", e)))?;
        return Ok(TableSchema { pool });
    }

    pub async fn desc_table(
        &self,
        table_id: u64,
        db_name: &str,
        table_name: &str,
    ) -> Option<TableMeta> {
        let sql = format!("desc `{}`.{}", db_name, table_name);
        let mut rows = sqlx::query(&sql).fetch(&self.pool);

        let mut source_position = 1;
        let mut columns = vec![];

        loop {
            match rows.try_next().await {
                Ok(Some(row)) => {
                    let field: &str = match row.try_get("Field") {
                        Ok(f) => f,
                        Err(e) => {
                            warn!("fetch desc table field error: {:?}", e);
                            return None;
                        }
                    };
                    let key: Result<Vec<u8>, sqlx::Error> = row.try_get("Key");
                    let key = Self::judge_primary_key(key);

                    columns.push(ColumnMeta {
                        ordinal_position: source_position,
                        column_name: field.to_string(),
                        is_primaty_key: key,
                    });
                    source_position = source_position + 1;
                }
                Ok(None) => break,
                Err(e) => {
                    warn!("desc table error for {}.{}: {:?}", db_name, table_name, e);
                    return None;
                }
            }
        }

        return Some(TableMeta::new(
            table_id,
            db_name.to_string(),
            table_name.to_string(),
            columns,
        ));
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

#[derive(Debug, Clone)]
pub struct TableMeta {
    table_id: u64,
    db_name: String,
    table_name: String,
    columns: HashMap<u32, ColumnMeta>,
    primary_key: String,
    primary_key_position: u32,
}

impl TableMeta {
    pub fn new(
        table_id: u64,
        db_name: String,
        table_name: String,
        columns: Vec<ColumnMeta>,
    ) -> Self {
        let primary_key_column = columns.iter().find(|c| c.is_primary());

        let (primary_key, primary_key_position) = if let Some(col) = primary_key_column {
            (col.column_name().to_string(), col.ordinal_position)
        } else {
            warn!("{}.{} not have primary key!", db_name, table_name);
            let fallback_col = columns
                .iter()
                .min_by_key(|col| col.ordinal_position)
                .expect(
                    format!("{}.{} no columns to use primary key", db_name, table_name).as_str(),
                );
            (
                fallback_col.column_name().to_string(),
                fallback_col.ordinal_position,
            )
        };

        let columns = columns
            .into_iter()
            .map(|col| (col.ordinal_position, col))
            .collect::<HashMap<u32, ColumnMeta>>();
        TableMeta {
            table_id: table_id,
            db_name: db_name,
            table_name: table_name,
            columns: columns,
            primary_key: primary_key,
            primary_key_position: primary_key_position,
        }
    }

    pub fn db_name(&self) -> &str {
        &self.db_name
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn column(&self, position: usize) -> Option<&ColumnMeta> {
        self.columns.get(&(position as u32))
    }

    pub fn primary_column(&self) -> &str {
        &self.primary_key
    }

    pub fn primary_key_position(&self) -> u32 {
        self.primary_key_position
    }

    pub fn qualified_table_name(&self) -> String {
        format!("{}.{}", self.db_name, self.table_name)
    }

    pub fn table_id(&self) -> u64 {
        self.table_id
    }
}

#[derive(Debug, Clone)]
pub struct ColumnMeta {
    ordinal_position: u32,
    column_name: String,
    is_primaty_key: bool,
}

impl ColumnMeta {
    pub fn new(ordinal_position: u32, column_name: String, is_primaty_key: bool) -> Self {
        ColumnMeta {
            ordinal_position,
            column_name,
            is_primaty_key,
        }
    }

    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    pub fn is_primary(&self) -> bool {
        self.is_primaty_key
    }
}

///
/// 包含缓存的操作，key:table-id value: table-meta
///

pub struct TableMetaHandler<'a> {
    table_schema: TableSchema,
    cache: HashMap<u64, TableMeta>,
    table_include: TableInclude,
    metrics: &'a Metrics,
}

impl<'a> TableMetaHandler<'a> {
    pub async fn new(config: &'a FlinkCdc, metrics: &'a Metrics) -> Result<Self, CdcError> {
        let table_schema = TableSchema::new(&config.source_url()).await?;
        Ok(TableMetaHandler {
            table_schema: table_schema,
            cache: HashMap::new(),
            table_include: config.source_table_include(),
            metrics: metrics,
        })
    }

    //
    //1. 按照目前的过滤的速度大概: 18-19w/s的速度
    //2. 实验下增加了这个表的schema的读取的速度,大概会降低1w/s的速度，现在大概是17w/s的速度，还是可以的，每个binlog文件大概是: 600w-700w个事件
    pub async fn record_table_meta(&mut self, event: TableMapEvent) {
        if self
            .table_include
            .can_exclude(&event.database_name, &event.table_name)
        {
            return;
        } else {
            self.cache_table_meta(&event).await;
        }
    }

    async fn cache_table_meta(&mut self, event: &TableMapEvent) {
        if self.cache.contains_key(&event.table_id) {
            return;
        } else {
            info!(
                "cache table meta information of :table-id:{} {}.{}",
                event.table_id, event.database_name, event.table_name
            );
            self.metrics
                .inc_flink_mysql_desc_table(&event.database_name);
            let metadata = self
                .table_schema
                .desc_table(event.table_id, &event.database_name, &event.table_name)
                .await;

            match metadata {
                Some(meta) => {
                    self.cache.insert(event.table_id, meta);
                }
                None => {
                    warn!(
                        "failed to get table meta for {}.{} (table may have been deleted), skipping cache",
                        event.database_name, event.table_name
                    );
                }
            }
        }
    }

    ///
    /// 增加binlog filename信息的记录,方便插入缓存的时候记录下这个信息
    /// 因为binlog的事件是严格按照顺序流转的,所以我们只需要记录一次就行了
    pub fn clear_cache(&mut self, filename: &str) {
        self.cache.clear();
        info!("clear cache and set new binlog filename:{filename}");
    }

    pub fn table_schema(&self, table_id: u64) -> Option<&TableMeta> {
        self.cache.get(&table_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试不存在表的情况，查看异常信息
    #[tokio::test]
    async fn test_desc_table_nonexistent_table() {
        let url = "mysql://root:dsap2018@172.16.1.67:3306/mostest_gsms";

        let table_schema = TableSchema::new(url)
            .await
            .expect("Failed to connect to database");

        let table_id = 999u64;
        let db_name = "mostest_gsms";
        let table_name = "nonexistent_table_xyz";

        // 执行 desc_table - 对于不存在的表，应该返回 None
        let result = table_schema.desc_table(table_id, db_name, table_name).await;

        // 验证返回 None（表不存在）
        assert!(result.is_none(), "Expected None for nonexistent table");
        println!("Successfully handled nonexistent table, returned None as expected");
    }
}
