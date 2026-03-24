///
/// 使用rust的channel来做线程间的通信
///
use dashmap::DashMap;
use moka::notification::RemovalCause;
use moka::policy::EvictionPolicy;
use moka::sync::Cache;
use moka::sync::CacheBuilder;
use mysql_binlog_connector_rust::event::table_map_event::TableMapEvent;
use tracing::info;
use tracing::warn;

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

        // 创建失效监听器
        let cache_size = config.source_binlog_cache_size();

        let binlog_cache = CacheBuilder::new(cache_size)
            .eviction_policy(EvictionPolicy::lru())
            .build();

        Ok(BinlogTableMetaHandler {
            table_schema,
            binlog_cache: binlog_cache,
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

                if let Some(meta) = metadata {
                    table_cache.insert(event.table_id, meta);
                    self.binlog_cache.insert(filename.to_string(), table_cache);
                } else {
                    warn!(
                        "failed to get table meta for {}.{} (table may have been deleted), skipping cache and all subsequent operations",
                        event.database_name, event.table_name
                    );
                }
            }
        } else {
            info!("build new binlog table meta cache:{}", &event.table_id);
            let metadata = self
                .table_schema
                .desc_table(event.table_id, &event.database_name, &event.table_name)
                .await;

            if let Some(meta) = metadata {
                let table_cache = DashMap::new();
                table_cache.insert(event.table_id, meta);
                self.binlog_cache
                    .insert(filename.to_string(), Arc::new(table_cache));
            } else {
                warn!(
                    "failed to get table meta for {}.{} (table may have been deleted), skipping cache and all subsequent operations",
                    event.database_name, event.table_name
                );
            }
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

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use moka::{policy::EvictionPolicy, sync::Cache};
    use tracing::info;
    use tracing_subscriber::fmt;

    fn init() {
        fmt().init();
    }

    #[test]
    fn test_cache_limit() {
        init();
        let cache = Cache::builder()
            .max_capacity(40)
            .eviction_policy(EvictionPolicy::lru())
            .build();

        info!("初始缓存的个数:{}!", cache.entry_count());

        // 插入 100 个元素
        for index in 1..=100 {
            cache.insert(format!("index-el:{}", index), String::from("test"));
        }

        info!("插入后立即查看缓存的个数:{}!", cache.entry_count());

        // 处理待办任务
        cache.run_pending_tasks();

        info!("run_pending_tasks 后缓存的个数:{}!", cache.entry_count());

        // 统计存在和不存在的 key
        let mut missing = vec![];
        let mut existing = vec![];

        for i in 1..=100 {
            if !cache.contains_key(format!("index-el:{}", i).as_str()) {
                missing.push(i);
            } else {
                existing.push(i);
            }
        }

        info!(
            "总结: 存在={} 个, 不存在={} 个",
            existing.len(),
            missing.len()
        );
        info!("不存在的 keys: {:?}", missing);
        info!(
            "存在的 keys 范围: {:?}...{:?}",
            existing.first(),
            existing.last()
        );

        // 插入第 101 个元素
        cache.insert("index-el:101".to_string(), "test".to_string());

        // 检查 index-el:61 和 index-el:62
        info!("index-el:61 存在: {}", cache.contains_key("index-el:61"));
        info!("index-el:62 存在: {}", cache.contains_key("index-el:62"));
        info!("index-el:101 存在: {}", cache.contains_key("index-el:101"));

        // 统计当前存在的所有 keys
        let mut current_keys = vec![];
        for i in 61..=101 {
            if cache.contains_key(format!("index-el:{}", i).as_str()) {
                current_keys.push(i);
            }
        }
        info!("当前存在的 keys: {:?}", current_keys);
        info!("当前缓存个数: {}", cache.entry_count());
    }
}
