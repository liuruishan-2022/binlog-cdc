use hashbrown::HashMap;

use futures_util::TryStreamExt;
use moka::sync::Cache;
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

#[derive(Debug, Clone)]
pub struct TableMeta {
    table_id: u64,
    db_name: String,
    table_name: String,
    columns: HashMap<u32, ColumnMeta>,
    primary_key: String,
}

impl TableMeta {
    pub fn new(
        table_id: u64,
        db_name: String,
        table_name: String,
        columns: Vec<ColumnMeta>,
    ) -> Self {
        let primary_key = columns
            .iter()
            .find(|c| c.is_primary())
            .map(|c| c.column_name().to_string())
            .unwrap_or_else(|| {
                warn!("{}.{} not have primary key!", db_name, table_name);
                return columns
                    .iter()
                    .min_by_key(|col| col.ordinal_position)
                    .map(|col| col.column_name().to_string())
                    .expect(
                        format!("{}.{} no columns to use primary key", db_name, table_name)
                            .as_str(),
                    );
            });
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

    pub fn qualified_table_name(&self) -> String {
        format!("{}.{}", self.db_name, self.table_name)
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
/// 增加一个binlog层次的缓存
///

type TableCache = HashMap<u64, TableMeta>;
type BinlogTableCache = Cache<String, HashMap<u64, TableMeta>>;

pub struct TableMetaHandler<'a> {
    config: &'a FlinkCdc,
    table_schema: TableSchema,
    cache: TableCache,
    binlog_cache: BinlogTableCache,
    table_include: TableInclude,
    metrics: &'a Metrics,
    binlog_filename: String,
}

impl<'a> TableMetaHandler<'a> {
    pub async fn new(config: &'a FlinkCdc, metrics: &'a Metrics) -> Result<Self, CdcError> {
        let table_schema = TableSchema::new(&config.source_url()).await?;
        Ok(TableMetaHandler {
            config: config,
            table_schema: table_schema,
            cache: TableCache::new(),
            binlog_cache: Cache::new(1000),
            table_include: config.source_table_include(),
            metrics: metrics,
            binlog_filename: String::from(""),
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
        }
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
            self.cache.insert(event.table_id, metadata.clone());
            self.cache_binlog_table_meta(metadata);
        }
    }

    fn cache_binlog_table_meta(&mut self, metadata: TableMeta) {
        if let Some(table_cache) = self.binlog_cache.get(&self.binlog_filename) {
            let mut new_cache = table_cache.clone();
            new_cache.insert(metadata.table_id, metadata);
            self.binlog_cache
                .insert(self.binlog_filename.clone(), new_cache);
        } else {
            let mut new_cache = TableCache::new();
            new_cache.insert(metadata.table_id, metadata);
            self.binlog_cache
                .insert(self.binlog_filename.clone(), new_cache);
        }
    }

    ///
    /// 增加binlog filename信息的记录,方便插入缓��的时候记录下这个信息
    /// 因为binlog的事件是严格按照顺序流转的,所以我们只需要记录一次就行了
    pub fn clear_cache(&mut self, filename: &str) {
        self.cache.clear();
        self.binlog_filename = filename.to_string();
        if self.binlog_cache.contains_key(filename) {
            info!("binlog cache has exists key:{filename}");
            return;
        } else {
            self.binlog_cache
                .insert(filename.to_string(), TableCache::new());
        }
    }

    pub fn table_schema(&self, table_id: u64) -> Option<&TableMeta> {
        self.cache.get(&table_id)
    }
}
