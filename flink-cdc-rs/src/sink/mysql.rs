use std::collections::HashMap;

use futures_util::TryStreamExt;
use moka::sync::Cache;
use sea_query::{Alias, Expr, ExprTrait, MysqlQueryBuilder, OnConflict, Query};
use sqlx::{MySqlPool, Row};
use tracing::{info, warn};

use crate::config::CdcConfig;
use crate::pipeline::formatter::DebeziumFormat;
use crate::pipeline::message::PipelineRecord;
use crate::sink::SinkStream;

/// MySQL sink for Debezium records.
/// 执行数据转成sql(insert/delete/update)的DDL语句
///
pub struct MysqlSink {
    pool: MySqlPool,
    cache: Cache<String, TableMeta>,
    route: HashMap<String, String>,
}

impl MysqlSink {
    pub async fn create(config: &CdcConfig) -> Self {
        let sink = match config.sink() {
            crate::config::sink::Sink::Mysql(mysql) => mysql,
            _ => panic!("mysql sink need mysql config"),
        };
        let route = config
            .route()
            .map(|routes| {
                routes
                    .iter()
                    .map(|route| (route.source().to_string(), route.sink().to_string()))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();

        let pool = MySqlPool::connect(&sink.url())
            .await
            .expect(format!("connect to mysql use:{} failed", &sink.url()).as_str());
        let cache = Cache::new(10000);
        MysqlSink {
            pool: pool,
            cache: cache,
            route: route,
        }
    }

    pub async fn table_info(&self, source: &str) -> TableMeta {
        let table = self.table_name(source);
        if let Some(meta) = self.cache.get(table.as_str()) {
            return meta;
        }

        let meta = self.desc_table(table.as_str()).await;
        self.cache.insert(table, meta.clone());
        meta
    }

    pub async fn delete(&self, debezium: &DebeziumFormat, topic: &str) {
        let meta = self.table_info(topic).await;
        if meta.primary_keys().is_empty() {
            warn!(
                "skip mysql delete because table has no primary key: {}",
                meta.table()
            );
            return;
        }
        let mut delete = Query::delete();
        let delete = delete.from_table(meta.table().to_string());
        for ele in meta.primary_keys() {
            if let Some(value) = debezium.before_column(ele) {
                delete.and_where(Expr::col(ele.to_string()).eq(value.to_string()));
            }
        }

        let result = sqlx::query(&delete.to_string(MysqlQueryBuilder))
            .execute(&self.pool)
            .await;
        match result {
            Ok(result) => info!(
                "mysql delete rows:{} table:{}",
                result.rows_affected(),
                meta.table()
            ),
            Err(err) => warn!("mysql delete error:{:?} table:{}", err, meta.table()),
        }
    }

    pub async fn insert_update_data(&self, debezium: &DebeziumFormat, topic: &str) {
        let meta = self.table_info(topic).await;
        let (columns, values): (Vec<Alias>, Vec<Expr>) = meta
            .columns
            .iter()
            .map(|ele| {
                let value = debezium
                    .after_column(ele.column_name())
                    .map_or(Expr::null(), |v| Expr::val(v.clone()));
                (Alias::new(ele.column_name()), value)
            })
            .unzip();

        let update_columns = meta
            .columns
            .iter()
            .filter(|ele| !ele.is_primary())
            .map(|ele| Alias::new(ele.column_name()))
            .collect::<Vec<Alias>>();

        let on_conflict = OnConflict::new().update_columns(update_columns).to_owned();

        let update_insert = Query::insert()
            .into_table(Alias::new(meta.table()))
            .columns(columns)
            .values_panic(values)
            .on_conflict(on_conflict)
            .take();
        let result = sqlx::query(&update_insert.to_string(MysqlQueryBuilder))
            .execute(&self.pool)
            .await;
        match result {
            Ok(result) => info!(
                "mysql delete rows:{} table:{}",
                result.rows_affected(),
                meta.table()
            ),
            Err(err) => warn!("mysql delete error:{:?} table:{}", err, meta.table()),
        }
    }

    pub async fn process_record(&self, record: &PipelineRecord) {
        match record {
            PipelineRecord::KafkaDebezium(data) => {
                self.process(data.data(), data.topic()).await;
            }
            _ => {
                warn!("unknown type do data process");
            }
        }
    }

    async fn desc_table(&self, table: &str) -> TableMeta {
        let sql = format!("desc `{}`", table);
        let mut rows = sqlx::query(&sql).fetch(&self.pool);

        let mut columns = vec![];
        while let Some(row) = rows.try_next().await.unwrap() {
            let field: &str = row.try_get("Field").expect("fetch desc table field error!");
            let column_type: &str = row.try_get("Type").expect("fetch desc table type error!");
            let key: Result<Vec<u8>, sqlx::Error> = row.try_get("Key");
            let key = Self::judge_primary_key(key);

            columns.push(MysqlColumnMeta::new(
                field.to_string(),
                column_type.to_string(),
                key,
            ));
        }

        TableMeta::new(table.to_string(), columns)
    }

    fn table_name(&self, source: &str) -> String {
        self.route
            .get(source)
            .cloned()
            .or_else(|| self.route.get("*").cloned())
            .unwrap_or_else(|| source.to_string())
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
                self.delete(debezium, topic).await;
            }
            "c" | "r" | "u" => {
                self.insert_update_data(debezium, topic).await;
            }
            _ => {
                warn!(
                    "unknown operator type:{} (d:delete c:create r:read u:update)",
                    debezium.op()
                );
            }
        }
    }

    async fn handle_messages(&self, _messages: Vec<DebeziumFormat>) {}
}

#[derive(Debug, Clone, PartialEq)]
enum MysqlBindValue {
    Null,
    Bool(bool),
    String(String),
}

#[derive(Debug, Clone)]
pub struct MysqlColumnMeta {
    column_name: String,
    column_type: String,
    is_primary_key: bool,
}

impl MysqlColumnMeta {
    fn new(column_name: String, column_type: String, is_primary_key: bool) -> Self {
        Self {
            column_name,
            column_type,
            is_primary_key,
        }
    }

    fn column_name(&self) -> &str {
        &self.column_name
    }

    fn column_type(&self) -> &str {
        &self.column_type
    }

    fn is_primary(&self) -> bool {
        self.is_primary_key
    }
}

///
/// 放置table的元数据信息的struct
///
#[derive(Debug, Clone)]
pub struct TableMeta {
    table: String,
    columns: Vec<MysqlColumnMeta>,
    primary_keys: Vec<String>,
}

impl TableMeta {
    pub fn new(table: String, columns: Vec<MysqlColumnMeta>) -> Self {
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
