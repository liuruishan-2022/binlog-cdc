///
/// 使用rust的channel来做线程间的通信
///
use dashmap::DashMap;
use moka::sync::Cache;
use mysql_binlog_connector_rust::event::table_map_event::TableMapEvent;
use tracing::info;

use crate::binlog::schema::TableMeta;
use crate::binlog::schema::TableSchema;
use crate::common::CdcError;
use crate::config::cdc::FlinkCdc;
use crate::config::cdc::TableInclude;
use std::sync::Arc;

type BinlogTableMeta = DashMap<u64, TableMeta>;

///
/// Binlog级别的表元数据缓存处理器
/// 每个binlog文件对应一个独立的表元数据缓存
///
pub struct BinlogTableMetaHandler {
    table_schema: TableSchema,
    binlog_cache: Cache<String, Arc<BinlogTableMeta>>,
    table_include: TableInclude,
}

impl BinlogTableMetaHandler {
    pub async fn new(config: &FlinkCdc) -> Result<Self, CdcError> {
        let table_schema = TableSchema::new(&config.source_url()).await?;
        Ok(BinlogTableMetaHandler {
            table_schema,
            binlog_cache: Cache::new(1000),
            table_include: config.source_table_include(),
        })
    }

    ///
    /// 记录表元数据到binlog级别的缓存中
    ///
    pub async fn record_table_meta(&self, filename: &str, event: TableMapEvent) {
        // 检查是否需要同步这个表
        if self
            .table_include
            .can_exclude(&event.database_name, &event.table_name)
        {
            return;
        }

        if let Some(table_cache) = self.binlog_cache.get(filename) {
            if table_cache.contains_key(&event.table_id) {
                return;
            } else {
                info!(
                    "cache binlog table meta information to cache:{}!",
                    &event.table_id
                );
                let metadata = self
                    .table_schema
                    .desc_table(event.table_id, &event.database_name, &event.table_name)
                    .await;
                table_cache.insert(event.table_id, metadata);
                self.binlog_cache.insert(filename.to_string(), table_cache);
            }
        } else {
            info!("build new binlog table meta cache:{}", &event.table_id);
            let metadata = self
                .table_schema
                .desc_table(event.table_id, &event.database_name, &event.table_name)
                .await;
            let table_cache = DashMap::new();
            table_cache.insert(event.table_id, metadata);
            self.binlog_cache
                .insert(filename.to_string(), Arc::new(table_cache));
        }
    }

    ///
    /// 获取指定table_id的表元数据
    /// 注意moka的get操作是对entry的value做clone动作的,我们修改成Arc之后对比下:
    /// 之前的clone动作是耗费8.8c-9c，然后处理一个binlog大概需要:12s左右.
    /// 然后使用Arc之后,大概耗费:5.5c,然后处理一个binlog大概需要:8s-10s左右
    ///
    pub fn table_schema(&self, filename: &str, table_id: u64) -> Option<TableMeta> {
        self.binlog_cache
            .get(filename)?
            .get(&table_id)
            .map(|r| r.clone())
    }
}
