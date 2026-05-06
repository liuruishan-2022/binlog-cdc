use crate::config::{CdcConfig, source::Source};

///
/// mysqldump zip/file as source.
///
pub struct MysqlDump {
    config: MysqlDump,
}

impl MysqlDump {
    pub fn create(cdc: &CdcConfig) -> Self {
        if let Source::MysqlDump(mysqldump) = cdc.source() {
            MysqlDump {
                config: mysqldump.clone(),
            }
        } else {
            panic!("Invalid source type for MysqlDump");
        }
    }
}
