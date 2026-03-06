//! Pipeline implementation

///
/// Pipeline的实现思想是: 通过source和sink之间建立一个消息流通信,
/// 是通过channel进行线程之间的通信方式

pub struct Pipeline {}

impl Pipeline {
    pub fn create() -> Self {
        Pipeline {}
    }

    pub fn start(&mut self) {}
}
