
use crate::config::load_config;

///
/// Pipeline是组合Source和Sink的地方
/// 因为Source具备了获取到消息的能力,
/// 然后Sink具备了处理消息的能力
/// 那么Pipeline的能力就是把: Source和Sink组合起来
///
pub mod stream;

pub async fn start_pipeline(config_path: &str) {
    let config = load_config(config_path);
    stream::pipeline(&config).await;
}
