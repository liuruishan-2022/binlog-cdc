use crate::config::CdcConfig;
use crate::pipeline::message::PipelineRecord;
use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;

/// 主要是放置数据处理的Pipeline的逻辑
/// 类似于流水线的思想去做
///
/// 我们的想法如下:
/// 1. 整体的逻辑是source--->channel---->transformer(待定去做)--->sink
/// 2. source投递的数据为:数据本身+源数据
/// 3. 源数据是多种类型的enum,包含各种自定义的数据信息
///
pub mod message;

pub async fn pipeline(_cdc: &CdcConfig) {}

fn channels(cdc: &CdcConfig) -> (Vec<Sender<PipelineRecord>>, Vec<Receiver<PipelineRecord>>) {
    let parallelism = cdc
        .pipeline()
        .map(|pipeline| pipeline.parallelism())
        .unwrap_or(6);
    let capacity = cdc
        .pipeline()
        .map(|pipeline| pipeline.capacity())
        .unwrap_or(1000);

    (1..=parallelism)
        .map(|_index| crossbeam_channel::bounded(capacity as usize))
        .unzip()
}
