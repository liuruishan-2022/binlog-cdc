use clap::Parser;

use crate::config::cdc::CdcConfig;

pub mod args;
pub mod cdc;

pub fn read_config() -> CdcConfig {
    CdcConfig::read_from(args::Arguments::parse().cdc_file())
}
