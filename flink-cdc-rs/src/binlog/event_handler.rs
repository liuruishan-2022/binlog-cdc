use mysql_binlog_connector_rust::event::{
    delete_rows_event::DeleteRowsEvent, rotate_event::RotateEvent, table_map_event::TableMapEvent,
    update_rows_event::UpdateRowsEvent, write_rows_event::WriteRowsEvent,
};
use tracing::info;

use crate::{
    binlog::{Metrics, row::RowEventHandler, schema::TableMetaHandler},
    common::CdcError,
    config::cdc::FlinkCdc,
};

///
/// 处理CDC事件的处理器
/// 从线下的CDC的事件的类型分析，
/// 发现TABLE_MAP_EVENT事件大概占据所有事件的:50%的样子，
/// 所以如果每次都去请求表的结构，那么速度会非常的慢速
/// 更正下 TABLE_MAP_EVENT事件在生产的分布情况:
/// 大概总体的EVENT处理速度为:8w/s的时候，TABLE_MAP_EVENT大概占据了:20%的样子,
/// 也就是1.5w/s-1.6w/s的速度，比write,delete,update事件都高
pub struct EventHandler<'a> {
    table_map_event_handler: TableMetaHandler<'a>,
    row_event_handler: RowEventHandler<'a>,
}

impl<'a> EventHandler<'a> {
    pub async fn new(config: &'a FlinkCdc, metrics: &'a Metrics) -> Result<Self, CdcError> {
        let table_map_event_handler = TableMetaHandler::new(config, metrics).await?;
        let row_event_handler = RowEventHandler::build(config, metrics).await;
        Ok(EventHandler {
            table_map_event_handler: table_map_event_handler,
            row_event_handler: row_event_handler,
        })
    }

    pub async fn handle_table_map_event(&mut self, event: TableMapEvent) {
        self.table_map_event_handler.record_table_meta(event).await;
    }

    pub fn handle_rotate_event(&mut self, event: &RotateEvent) {
        info!(
            "Rotate event clear cache! binlog:{} position:{}!",
            event.binlog_filename, event.binlog_position
        );
        self.table_map_event_handler
            .clear_cache(&event.binlog_filename);
    }

    ///
    /// TODO 整体进行binlog的解析和Kafka的消息投递的CPU耗费还是很高的
    /// 但是现在的问题是这样子:
    /// 1. CPU只能耗费到1C,所以这个是程序的问题,串行化了
    /// 2. 现在暂时还不知道CPU耗费在什么地方.不过也能猜个八九不离十
    pub async fn handle_write_rows_event(&mut self, event: WriteRowsEvent) {
        if std::env::var("CHANNEL").is_ok() {
            return;
        }

        let table_meta = self.table_map_event_handler.table_schema(event.table_id);
        if let Some(table_meta) = table_meta {
            self.row_event_handler
                .handle_write_event(table_meta, event)
                .await;
        }
    }

    pub async fn handle_update_rows_event(&mut self, event: UpdateRowsEvent) {
        if std::env::var("CHANNEL").is_ok() {
            return;
        }

        let table_meta = self.table_map_event_handler.table_schema(event.table_id);
        if let Some(table_meta) = table_meta {
            self.row_event_handler
                .handle_update_event(table_meta, event)
                .await;
        }
    }

    pub async fn handle_delete_rows_event(&mut self, event: DeleteRowsEvent) {
        if std::env::var("CHANNEL").is_ok() {
            return;
        }

        let table_meta = self.table_map_event_handler.table_schema(event.table_id);
        if let Some(table_meta) = table_meta {
            self.row_event_handler
                .handle_delete_event(table_meta, event)
                .await;
        }
    }
}
