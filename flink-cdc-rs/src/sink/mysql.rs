use std::collections::HashMap;

use futures_util::TryStreamExt;
use moka::sync::Cache;
use sea_query::{Expr, ExprTrait, MysqlQueryBuilder, Query};
use serde_json::Value;
use sqlx::query_builder::Separated;
use sqlx::{MySql, MySqlPool, QueryBuilder, Row};
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
        /// 使用sea-query
        let mut delete = Query::delete();
        let delete = delete.from_table(meta.table().to_string());
        for ele in meta.primary_keys() {
            if let Some(value) = debezium.before_column(ele) {
                delete.and_where(Expr::col(ele.to_string()).eq(value.to_string()));
            }
        }

        let result = sqlx::query(&delete.to_string(MysqlQueryBuilder)).fetch(&self.pool);
        match result {
            Ok(result) => info!(
                "mysql delete rows:{} table:{}",
                result.rows_affected(),
                meta.table()
            ),
            Err(err) => warn!("mysql delete error:{:?} table:{}", err, meta.table()),
        }
    }

    pub async fn upsert_data(&self, debezium: &DebeziumFormat, topic: &str) {
        let meta = self.table_info(topic).await;
        let columns = meta
            .columns
            .iter()
            .filter(|column| debezium.after_column(column.column_name()).is_some())
            .collect::<Vec<_>>();

        if columns.is_empty() {
            warn!(
                "skip mysql upsert because after has no known columns, topic={}",
                topic
            );
            return;
        }

        let values = columns
            .iter()
            .map(|column| {
                let value = debezium
                    .after_column(column.column_name())
                    .unwrap_or(&Value::Null);
                (
                    column.column_name(),
                    json_to_mysql_value(value, column.column_type()),
                )
            })
            .collect::<Vec<_>>();
        let mut builder = build_upsert_query(meta.table(), &values, meta.primary_keys());

        let result = builder.build().execute(&self.pool).await;
        match result {
            Ok(result) => info!(
                "mysql upsert op:{} rows:{} table:{}",
                debezium.op(),
                result.rows_affected(),
                meta.table()
            ),
            Err(err) => warn!(
                "mysql upsert op:{} error:{:?} table:{}",
                debezium.op(),
                err,
                meta.table()
            ),
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
        let sql = format!("desc {}", table);
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
                self.upsert_data(debezium, topic).await;
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

fn json_to_mysql_value(value: &Value, column_type: &str) -> MysqlBindValue {
    if column_type.eq_ignore_ascii_case("bit(1)") {
        return match value {
            Value::Null => MysqlBindValue::Null,
            Value::Bool(value) => MysqlBindValue::Bool(*value),
            Value::Number(value) if value.as_u64() == Some(0) => MysqlBindValue::Bool(false),
            Value::Number(value) if value.as_u64() == Some(1) => MysqlBindValue::Bool(true),
            Value::String(value) if value == "0" => MysqlBindValue::Bool(false),
            Value::String(value) if value == "1" => MysqlBindValue::Bool(true),
            _ => MysqlBindValue::String(value.to_string()),
        };
    }

    match value {
        Value::Null => MysqlBindValue::Null,
        Value::Bool(value) => MysqlBindValue::String(if *value {
            "1".to_string()
        } else {
            "0".to_string()
        }),
        Value::Number(value) => MysqlBindValue::String(value.to_string()),
        Value::String(value) => MysqlBindValue::String(value.clone()),
        Value::Array(_) | Value::Object(_) => MysqlBindValue::String(value.to_string()),
    }
}

fn build_upsert_query(
    table: &str,
    columns: &[(&str, MysqlBindValue)],
    primary_keys: &[String],
) -> QueryBuilder<'static, MySql> {
    let mut builder = QueryBuilder::<MySql>::new("INSERT INTO ");
    builder.push(table).push(" (");
    {
        let mut column_names = builder.separated(", ");
        for (column, _) in columns {
            column_names.push(column);
        }
    }
    builder.push(") VALUES (");
    {
        let mut values = builder.separated(", ");
        for (_, value) in columns {
            push_mysql_bind(&mut values, value.clone());
        }
    }
    builder.push(") ON DUPLICATE KEY UPDATE ");
    {
        let mut update_columns = columns
            .iter()
            .map(|(column, _)| *column)
            .filter(|column| !primary_keys.iter().any(|key| key == column))
            .collect::<Vec<_>>();
        if update_columns.is_empty() {
            update_columns.push(columns[0].0);
        }

        let mut assignments = builder.separated(", ");
        for column in update_columns {
            assignments
                .push(column)
                .push_unseparated(" = VALUES(")
                .push_unseparated(column)
                .push_unseparated(")");
        }
    }
    builder
}

fn push_bound_equality<'qb, 'args>(
    separated: &mut Separated<'qb, 'args, MySql, &'static str>,
    column: &str,
    value: MysqlBindValue,
) {
    separated.push(column).push_unseparated(" = ");
    push_mysql_bind_unseparated(separated, value);
}

fn push_mysql_bind<'qb, 'args>(
    separated: &mut Separated<'qb, 'args, MySql, &'static str>,
    value: MysqlBindValue,
) {
    match value {
        MysqlBindValue::Null => separated.push_bind(Option::<String>::None),
        MysqlBindValue::Bool(value) => separated.push_bind(value),
        MysqlBindValue::String(value) => separated.push_bind(value),
    };
}

fn push_mysql_bind_unseparated<'qb, 'args>(
    separated: &mut Separated<'qb, 'args, MySql, &'static str>,
    value: MysqlBindValue,
) {
    match value {
        MysqlBindValue::Null => separated.push_bind_unseparated(Option::<String>::None),
        MysqlBindValue::Bool(value) => separated.push_bind_unseparated(value),
        MysqlBindValue::String(value) => separated.push_bind_unseparated(value),
    };
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

    fn column(&self, column_name: &str) -> Option<&MysqlColumnMeta> {
        self.columns
            .iter()
            .find(|column| column.column_name() == column_name)
    }

    pub fn table(&self) -> &str {
        self.table.as_str()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use sqlx::{MySql, QueryBuilder};

    use super::{MysqlBindValue, build_upsert_query, json_to_mysql_value, push_bound_equality};

    #[test]
    fn bit_one_numeric_values_are_bound_as_booleans() {
        assert_eq!(
            json_to_mysql_value(&json!(0), "bit(1)"),
            MysqlBindValue::Bool(false)
        );
        assert_eq!(
            json_to_mysql_value(&json!(1), "BIT(1)"),
            MysqlBindValue::Bool(true)
        );
    }

    #[test]
    fn non_bit_values_keep_the_existing_string_binding() {
        assert_eq!(
            json_to_mysql_value(&json!(0), "int(11)"),
            MysqlBindValue::String("0".to_string())
        );
    }

    #[test]
    fn upsert_inserts_missing_rows_and_updates_non_primary_columns() {
        let columns = vec![
            ("id", MysqlBindValue::String("1603999".to_string())),
            ("hostname", MysqlBindValue::String(String::new())),
            ("port", MysqlBindValue::String("-1".to_string())),
        ];
        let primary_keys = vec!["id".to_string()];

        let builder = build_upsert_query("test_table", &columns, &primary_keys);

        assert_eq!(
            builder.sql(),
            "INSERT INTO test_table (id, hostname, port) VALUES (?, ?, ?) \
ON DUPLICATE KEY UPDATE hostname = VALUES(hostname), port = VALUES(port)"
        );
    }

    #[test]
    fn bound_equalities_in_set_are_separated_by_commas() {
        let mut builder = QueryBuilder::<MySql>::new("UPDATE test_table SET ");
        {
            let mut set = builder.separated(", ");
            push_bound_equality(
                &mut set,
                "hostname",
                MysqlBindValue::String("db-host".to_string()),
            );
            push_bound_equality(&mut set, "port", MysqlBindValue::String("3306".to_string()));
        }

        assert_eq!(
            builder.sql(),
            "UPDATE test_table SET hostname = ?, port = ?"
        );
    }

    #[test]
    fn bound_equalities_in_where_are_separated_by_and() {
        let mut builder = QueryBuilder::<MySql>::new("DELETE FROM test_table WHERE ");
        {
            let mut conditions = builder.separated(" AND ");
            push_bound_equality(
                &mut conditions,
                "id",
                MysqlBindValue::String("1".to_string()),
            );
            push_bound_equality(
                &mut conditions,
                "tenant_id",
                MysqlBindValue::String("2".to_string()),
            );
        }

        assert_eq!(
            builder.sql(),
            "DELETE FROM test_table WHERE id = ? AND tenant_id = ?"
        );
    }
}
