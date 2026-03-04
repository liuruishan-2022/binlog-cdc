use mysql_binlog_connector_rust::event::{
    delete_rows_event::DeleteRowsEvent, rotate_event::RotateEvent, table_map_event::TableMapEvent,
    update_rows_event::UpdateRowsEvent, write_rows_event::WriteRowsEvent,
};
use tracing::info;

use crate::{
    binlog::{Metrics, row::RowEventHandler, schema::TableMetaHandler},
    config::cdc::FlinkCdc,
};

///
/// 处理CDC事件的处理器
/// 从线下的CDC的事件的类型分析，发现TABLE_MAP_EVENT事件大概占据所有事件的:50%的样子，所以如果每次都去请求表的结构，那么速度会非常的慢速
pub struct EventHandler<'a> {
    table_map_event_handler: TableMetaHandler<'a>,
    row_event_handler: RowEventHandler<'a>,
}

impl<'a> EventHandler<'a> {
    pub async fn new(config: &'a FlinkCdc, metrics: &'a Metrics) -> Self {
        let table_map_event_handler = TableMetaHandler::new(config, metrics).await;
        let row_event_handler = RowEventHandler::build(config, metrics).await;
        EventHandler {
            table_map_event_handler: table_map_event_handler,
            row_event_handler: row_event_handler,
        }
    }

    pub async fn handle_table_map_event(&mut self, event: &TableMapEvent) {
        self.table_map_event_handler.record_table_meta(event).await;
    }

    pub fn handle_rotate_event(&mut self, event: &RotateEvent) {
        info!(
            "Rotate event clear cache! binlog:{} position:{}!",
            event.binlog_filename, event.binlog_position
        );
        self.table_map_event_handler.clear_cache();
    }

    pub async fn handle_write_rows_event(&mut self, event: WriteRowsEvent) {
        let table_meta = self.table_map_event_handler.table_schema(event.table_id);
        self.row_event_handler
            .handle_write_event(table_meta, event)
            .await;
    }

    pub async fn handle_update_rows_event(&mut self, event: UpdateRowsEvent) {
        let table_meta = self.table_map_event_handler.table_schema(event.table_id);
        self.row_event_handler
            .handle_update_event(table_meta, event)
            .await;
    }

    pub async fn handle_delete_rows_event(&mut self, event: DeleteRowsEvent) {
        let table_meta = self.table_map_event_handler.table_schema(event.table_id);
        self.row_event_handler
            .handle_delete_event(table_meta, event)
            .await;
    }
}
