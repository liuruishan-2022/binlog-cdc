//! Pipeline message types

use crate::common::DebeziumFormat;
use crate::config::Route;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteInfo {
    pub source_table: String,
    pub sink_table: String,
}

impl RouteInfo {
    pub fn from_route(route: &Route) -> Self {
        Self {
            source_table: route.source_table.clone(),
            sink_table: route.sink_table.clone(),
        }
    }

    pub fn new(source_table: String, sink_table: String) -> Self {
        Self {
            source_table,
            sink_table,
        }
    }

    pub fn source_table(&self) -> &str {
        &self.source_table
    }

    pub fn sink_table(&self) -> &str {
        &self.sink_table
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineMessage {
    pub data: DebeziumFormat,
    pub routes: Vec<RouteInfo>,
}

impl PipelineMessage {
    pub fn new(data: DebeziumFormat, routes: Vec<RouteInfo>) -> Self {
        Self { data, routes }
    }

    pub fn with_single_route(data: DebeziumFormat, route: RouteInfo) -> Self {
        Self {
            data,
            routes: vec![route],
        }
    }

    pub fn from_route(data: DebeziumFormat, route: &Route) -> Self {
        Self {
            data,
            routes: vec![RouteInfo::from_route(route)],
        }
    }

    pub fn from_routes(data: DebeziumFormat, routes: &[Route]) -> Self {
        Self {
            data,
            routes: routes.iter().map(RouteInfo::from_route).collect(),
        }
    }

    pub fn data(&self) -> &DebeziumFormat {
        &self.data
    }

    pub fn routes(&self) -> &[RouteInfo] {
        &self.routes
    }

    pub fn route_count(&self) -> usize {
        self.routes.len()
    }

    pub fn has_routes(&self) -> bool {
        !self.routes.is_empty()
    }

    pub fn operation(&self) -> &str {
        self.data.op()
    }

    pub fn source_database(&self) -> Option<&str> {
        self.routes.first()?.source_table.split('.').next()
    }

    pub fn source_table_name(&self) -> Option<&str> {
        self.routes
            .first()?
            .source_table
            .split('.')
            .last()
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{DebeziumFormat, MessageKey};
    use serde_json::json;

    #[test]
    fn test_route_info() {
        let route = RouteInfo::new("app_db.orders".to_string(), "kafka_ods_orders".to_string());
        assert_eq!(route.source_table(), "app_db.orders");
        assert_eq!(route.sink_table(), "kafka_ods_orders");
    }

    #[test]
    fn test_pipeline_message_single_route() {
        let data = DebeziumFormat::insert(
            json!({"id": 1, "name": "test"}),
            "test_db",
            "users",
            MessageKey::new(Default::default()),
        );

        let route = RouteInfo::new("test_db.users".to_string(), "sink_users".to_string());
        let msg = PipelineMessage::with_single_route(data.clone(), route);

        assert_eq!(msg.route_count(), 1);
        assert!(msg.has_routes());
        assert_eq!(msg.operation(), "c");
        assert_eq!(msg.source_table_name(), Some("users"));
    }

    #[test]
    fn test_pipeline_message_multiple_routes() {
        let data = DebeziumFormat::insert(
            json!({"id": 1}),
            "app_db",
            "orders",
            MessageKey::new(Default::default()),
        );

        let routes = vec![
            RouteInfo::new("app_db.orders".to_string(), "kafka_ods_orders".to_string()),
            RouteInfo::new("app_db.orders".to_string(), "mysql_ods_orders".to_string()),
        ];

        let msg = PipelineMessage::new(data, routes);

        assert_eq!(msg.route_count(), 2);
        assert_eq!(msg.routes()[0].sink_table(), "kafka_ods_orders");
        assert_eq!(msg.routes()[1].sink_table(), "mysql_ods_orders");
    }

    #[test]
    fn test_pipeline_message_json() {
        let data = DebeziumFormat::insert(
            json!({"id": 1}),
            "db",
            "table",
            MessageKey::new(Default::default()),
        );

        let msg = PipelineMessage::with_single_route(
            data,
            RouteInfo::new("db.table".to_string(), "sink_table".to_string()),
        );

        let json_str = msg.to_json().unwrap();
        assert!(json_str.contains("source_table"));
        assert!(json_str.contains("sink_table"));
    }
}
