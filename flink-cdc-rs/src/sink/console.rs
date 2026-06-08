use tracing::info;

use crate::pipeline::message::PipelineRecord;

///
/// 提供一个朝向控制台输出的Sink实现, 主要用于测试和调试
///

pub struct ConsoleSink {
    channels: Vec<crossbeam_channel::Receiver<PipelineRecord>>,
}

impl ConsoleSink {
    pub fn create(channels: Vec<crossbeam_channel::Receiver<PipelineRecord>>) -> Self {
        ConsoleSink { channels: channels }
    }

    pub fn start(&self) -> Vec<tokio::task::JoinHandle<()>> {
        self.channels
            .iter()
            .enumerate()
            .map(|(index, receiver)| {
                let receiver = receiver.clone();
                tokio::task::spawn_blocking(move || {
                    info!("console sink receiver启动, index={}", index);
                    while let Ok(message) = receiver.recv() {
                        info!("data:{}", message);
                    }
                })
            })
            .collect::<Vec<_>>()
    }

    pub async fn write(&self) {
        let handles = self.start();
        for handle in handles {
            handle.await.expect("console sink write task failed");
        }
    }
}
