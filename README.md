# 1. 工具编写出发点

1. 在使用 flink cdc 3.2 的时候，发现性能提不上去，总是徘徊在: 7000/s---8000/s 之间，经过查看源码并且做了各种尝试之后，发现还是无法解决,不过作者本人也是新入手 flink cdc.并且不擅长 flink，而且不支持翻译 bit(1)为 int/tinyint 类型
2. 在升级到 flink cdc 3.5.0 之后，发现性能还是跟不上生产的速度，生产需要能够提供:2.5w/s---3.0w/s 的速度性能支撑,即使扩充了 8 个 taskmanager，但是还是无法达到这个目标,后来发现是 binlog 的采集端是单线程，因为需要保证有序，所以只能单线程处理
3. 整个 flink+flink cdc 架构还是有点复杂，入手不是很容易，但是作者本人又是那种兼职的工作，并且并没有很多的 flink 任务要跑，仅仅是同步业务数据库的 mysql 到 doris 而以，当然这个中间需要经过 kafka,因为还有其他模块也需要同时消费这些数据

基于以上的需求和问题，决定使用 rust 编写一个 flink cdc 的 binlog-cdc 任务，只做 binlog 的 cdc 采集投递到 kafka 的简单功能，后续有需求之后，再进行完善，耗时:5 天完成,还算可以，比之前研究 flink cdc 3.x 耗费了 1-2 个月也没有解决性能问题.算是一个不错的选择.

# 2. 支持的功能如下

1. 同步 mysql 的 binlog 到 kafka 的 topic 中
2. 提供同步的 savepoint 功能(每次度渠道一个新的 binlog，则会写入到 savepoint 中这个 binlog 的名字，下次任务重启就从这个 binlog 开始同步)

# 3. 构建

## 3.1 构建成本地的可执行文件

```bash
cargo build --release

#在./target/release 目录下面会看到: binlog-cdc可执行程序文件
```

## 3.2 构建 docker 镜像

```bash
docker build -t binlog-cdc:v3.5.0 .
```
