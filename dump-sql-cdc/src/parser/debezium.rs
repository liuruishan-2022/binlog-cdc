use crate::parser::dumpsql::Table;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Serialize, Debug)]
pub struct DebeziumFormat {
    before: Option<Value>,
    after: Option<Value>,
    op: &'static str,
    source: DebeziumSource,
}

impl DebeziumFormat {
    pub fn build(table: &Table, rows: Vec<Vec<Value>>) -> Vec<Self> {
        rows.into_iter()
            .map(|row| DebeziumFormat::single_row(table, row))
            .collect()
    }

    fn single_row(table: &Table, row: Vec<Value>) -> Self {
        let mut map = serde_json::Map::with_capacity(row.len());

        for (index, data) in row.into_iter().enumerate() {
            if let Some(column) = table.columns_by_index(index) {
                map.insert(column.name().to_string(), data);
            }
        }

        DebeziumFormat::insert(json!(map), table.name())
    }

    fn insert(after: Value, table: &str) -> Self {
        DebeziumFormat {
            before: None,
            after: Some(after),
            op: "c",
            source: DebeziumSource {
                table: table.to_string(),
            },
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("serialization debezium to json error")
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct DebeziumSource {
    table: String,
}
