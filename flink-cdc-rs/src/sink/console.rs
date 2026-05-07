use tracing::info;

use crate::binlog::row::DebeziumFormat;

///
/// 提供一个朝向控制台输出的Sink实现, 主要用于测试和调试
///

pub struct ConsoleSink {
    channels: Vec<crossbeam_channel::Receiver<DebeziumFormat>>,
}

impl ConsoleSink {
    pub fn create(channels: Vec<crossbeam_channel::Receiver<DebeziumFormat>>) -> Self {
        ConsoleSink { channels: channels }
    }

    pub fn start(self) -> Vec<tokio::task::JoinHandle<()>> {
        self.channels
            .into_iter()
            .enumerate()
            .map(|(index, receiver)| {
                tokio::task::spawn_blocking(move || {
                    info!("console sink receiver启动, index={}", index);
                    while let Ok(message) = receiver.recv() {
                        info!(
                            "console sink receiver收到消息: message={}",
                            message.to_json()
                        );
                    }
                })
            })
            .collect::<Vec<_>>()
    }

    pub async fn write(self) {
        let handles = self.start();
        for handle in handles {
            handle.await.expect("console sink write task failed");
        }
    }
}
