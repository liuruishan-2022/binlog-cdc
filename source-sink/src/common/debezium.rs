//! Debezium format support
//!
//! Provides Debezium-compatible JSON format for CDC (Change Data Capture) events.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tracing::warn;

#[derive(Serialize, Debug, Deserialize, Clone)]
pub struct DebeziumFormat {
    pub before: Option<Value>,

    pub after: Option<Value>,

    pub op: String,

    pub source: DebeziumSource,

    #[serde(skip)]
    pub key: MessageKey,
}

impl DebeziumFormat {
    pub fn insert(after: Value, db: &str, table: &str, key: MessageKey) -> Self {
        DebeziumFormat {
            before: None,
            after: Some(after),
            op: "c".to_string(),
            source: DebeziumSource {
                db: db.to_string(),
                table: table.to_string(),
            },
            key,
        }
    }

    pub fn update(before: Value, after: Value, db: &str, table: &str, key: MessageKey) -> Self {
        DebeziumFormat {
            before: Some(before),
            after: Some(after),
            op: "u".to_string(),
            source: DebeziumSource {
                db: db.to_string(),
                table: table.to_string(),
            },
            key,
        }
    }

    pub fn delete(before: Value, db: &str, table: &str, key: MessageKey) -> Self {
        DebeziumFormat {
            before: Some(before),
            after: None,
            op: "d".to_string(),
            source: DebeziumSource {
                db: db.to_string(),
                table: table.to_string(),
            },
            key,
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("Failed to serialize DebeziumFormat to JSON")
    }

    pub fn keys(&self) -> String {
        self.key.keys_json()
    }

    pub fn op(&self) -> &str {
        &self.op
    }

    pub fn before_column(&self, column_name: &str) -> Option<&Value> {
        self.before.as_ref()?.get(column_name)
    }

    pub fn after_column(&self, column_name: &str) -> Option<&Value> {
        self.after.as_ref()?.get(column_name)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DebeziumSource {
    pub db: String,

    pub table: String,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct MessageKey {
    pub keys: Map<String, Value>,
}

impl MessageKey {
    pub fn new(keys: Map<String, Value>) -> Self {
        MessageKey { keys }
    }

    pub fn keys_json(&self) -> String {
        serde_json::to_string(&self.keys).unwrap_or_else(|_| "{}".to_string())
    }
}

pub fn format_timestamp(timestamp: i64) -> String {
    use chrono::{Local, TimeZone};

    let millis = timestamp / 1000;
    match Local.timestamp_millis_opt(millis) {
        chrono::LocalResult::Single(time) => time.format("%Y-%m-%d %H:%M:%S").to_string(),
        _ => {
            warn!("timestamp is invalid: {}!", timestamp);
            String::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_debezium_insert() {
        let after = json!({"id": 1, "name": "Alice"});
        let event = DebeziumFormat::insert(
            after.clone(),
            "testdb",
            "users",
            MessageKey::new(Map::new()),
        );

        assert_eq!(event.op, "c");
        assert_eq!(event.before, None);
        assert_eq!(event.after, Some(after));
        assert_eq!(event.source.db, "testdb");
        assert_eq!(event.source.table, "users");
    }

    #[test]
    fn test_debezium_update() {
        let before = json!({"id": 1, "name": "Alice"});
        let after = json!({"id": 1, "name": "Bob"});
        let event = DebeziumFormat::update(
            before.clone(),
            after.clone(),
            "testdb",
            "users",
            MessageKey::new(Map::new()),
        );

        assert_eq!(event.op, "u");
        assert_eq!(event.before, Some(before));
        assert_eq!(event.after, Some(after));
    }

    #[test]
    fn test_debezium_delete() {
        let before = json!({"id": 1, "name": "Alice"});
        let event = DebeziumFormat::delete(
            before.clone(),
            "testdb",
            "users",
            MessageKey::new(Map::new()),
        );

        assert_eq!(event.op, "d");
        assert_eq!(event.before, Some(before));
        assert_eq!(event.after, None);
    }

    #[test]
    fn test_debezium_to_json() {
        let after = json!({"id": 1, "name": "Alice"});
        let event = DebeziumFormat::insert(after, "testdb", "users", MessageKey::new(Map::new()));

        let json_str = event.to_json();
        assert!(json_str.contains("\"op\":\"c\""));
        assert!(json_str.contains("\"db\":\"testdb\""));
        assert!(json_str.contains("\"table\":\"users\""));
    }

    #[test]
    fn test_debezium_after_column() {
        let after = json!({"id": 1, "name": "Alice", "age": 30});
        let event = DebeziumFormat::insert(after, "testdb", "users", MessageKey::new(Map::new()));

        assert_eq!(event.after_column("name"), Some(&json!("Alice")));
        assert_eq!(event.after_column("age"), Some(&json!(30)));
        assert_eq!(event.after_column("invalid"), None);
    }

    #[test]
    fn test_message_key() {
        let mut keys = Map::new();
        keys.insert("id".to_string(), json!(1));
        keys.insert("table".to_string(), json!("users"));

        let key = MessageKey::new(keys.clone());
        let json_str = key.keys_json();

        assert!(json_str.contains("\"id\":1"));
        assert!(json_str.contains("\"table\":\"users\""));
    }

    #[test]
    fn test_format_timestamp() {
        let timestamp: i64 = 1763976387000000; // 2025-11-24 14:46:27
        let formatted = format_timestamp(timestamp);
        assert_eq!(formatted, "2025-11-24 14:46:27");
    }

    #[test]
    fn test_format_timestamp_invalid() {
        let invalid_timestamp: i64 = -1;
        let formatted = format_timestamp(invalid_timestamp);
        assert_eq!(formatted, "");
    }
}
