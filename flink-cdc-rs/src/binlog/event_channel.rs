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

pub struct EventChannelHandler {
    binlog_table_handler: BinlogTableMetaHandler,
}

impl EventChannelHandler {
    pub async fn new(config: &FlinkCdc) -> Result<Self, CdcError> {
        let binlog_table_handler = BinlogTableMetaHandler::new(&config.source_url()).await?;
        Ok(EventChannelHandler {
            binlog_table_handler,
        })
    }
}

///
/// Binlog级别的表元数据缓存处理器
/// 每个binlog文件对应一个独立的表元数据缓存
///
pub struct BinlogTableMetaHandler {
    table_schema: TableSchema,
    binlog_cache: Cache<String, DashMap<u64, TableMeta>>,
    binlog_filename: String,
}

impl BinlogTableMetaHandler {
    pub async fn new(mysql_url: &str) -> Result<Self, CdcError> {
        let table_schema = TableSchema::new(mysql_url).await?;
        Ok(BinlogTableMetaHandler {
            table_schema,
            binlog_cache: Cache::new(1000),
            binlog_filename: String::from(""),
        })
    }

    ///
    /// 记录表元数据到binlog级别的缓存中
    ///
    pub async fn record_table_meta(&mut self, event: TableMapEvent) {
        if let Some(mut table_cache) = self.binlog_cache.get(&self.binlog_filename) {
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
                self.binlog_cache
                    .insert(self.binlog_filename.clone(), table_cache);
            }
        } else {
            info!("build new binlog table meta cache:{}", &event.table_id);
            let metadata = self
                .table_schema
                .desc_table(event.table_id, &event.database_name, &event.table_name)
                .await;
            let mut table_cache = DashMap::new();
            table_cache.insert(event.table_id, metadata);
            self.binlog_cache
                .insert(self.binlog_filename.clone(), table_cache);
        }
    }

    ///
    /// 清理缓存并设置新的binlog文件名
    /// 因为binlog的事件是严格按照顺序流转的,所以我们只需要记录一次就行了
    ///
    pub fn clear_cache(&mut self, filename: &str) {
        self.binlog_filename = filename.to_string();
        if self.binlog_cache.contains_key(filename) {
            info!("binlog cache has exists key:{filename}");
            return;
        } else {
            self.binlog_cache
                .insert(filename.to_string(), DashMap::new());
        }
    }

    ///
    /// 获取指定table_id的表元数据
    ///
    pub fn table_schema(&self, table_id: u64) -> Option<TableMeta> {
        self.binlog_cache
            .get(&self.binlog_filename)?
            .get(&table_id)
            .map(|r| r.clone())
    }

    ///
    /// 需要严格按照串行的流程处理binlog的信息
    /// 这个地方返回当前的binlog的名字
    ///
    pub fn current_binlog(&self) -> String {
        self.binlog_filename.clone()
    }
}
