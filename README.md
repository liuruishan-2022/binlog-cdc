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

# 4. 如何使用

## 4.1 前提条件

1. Kafka 集群已经搭建
2. mysql 已经具备了 flink cdc 同步需要的用户，以及应该赋予用户的权限

## 4.2 提供配置文件

```yaml
source:
  type: mysql
  hostname: 192.168.1.106
  port: 3306
  username: binlog_cdc
  password: binlog_cdc_123456
  tables: db.table1,db.table2
  server-time-zone: Asia/Shanghai
  server-id: 9529-9530
  scan.startup.mode: specific-offset
  scan.startup.specific-offset.file: mysql-bin.004717
  scan.startup.specific-offset.pos: 0
sink:
  type: kafka
  name: Kafka-Sink
  properties.bootstrap.servers: kafka-1:9092,kafka-2:9092,kafka-3:9092
  properties.compression.type: lz4
  topic: binlog-cdc-defaults
  properties.batch.size: 40000
  properties.linger.ms: 100
transform:
  - source-table: db.table1
    projection: "'' as hostname,-1 as port,'' as account,'' as password"
  - source-table: db.table2
    projection: LOCALTIMESTAMP as update_time
pipeline:
  name: sync mysql to kafka
  parallelism: 1
```

字段解释:

source 字段解释:

| 字段名                            | 字段值              | 解释                                                                                                   |
| --------------------------------- | ------------------- | ------------------------------------------------------------------------------------------------------ |
| type                              | mysql               | 数据源类型，指定为 MySQL（Flink CDC 专属 MySQL 连接器，基于 Debezium 解析 binlog）                     |
| hostname                          | 192.168.1.106       | MySQL 数据库的连接地址（IP 或域名）                                                                    |
| port                              | 3306                | MySQL 数据库的服务端口（默认值为 3306，需与目标数据库端口一致）                                        |
| username                          | binlog_cdc          | 连接 MySQL 的用户名，需具备 binlog 读取权限（如 REPLICATION SLAVE、REPLICATION CLIENT 权限）           |
| password                          | binlog_cdc_123456   | 连接 MySQL 的用户密码                                                                                  |
| tables                            | db.table1,db.table2 | 需同步的目标表，格式为「库名.表名」，多表之间用逗号分隔                                                |
| server-time-zone                  | Asia/Shanghai       | 时区配置，需与 MySQL 服务器时区保持一致，避免时间字段同步偏差                                          |
| server-id                         | 9529-9530           | CDC 客户端的 server-id 范围（MySQL 主从复制机制要求），需避免与其他从库/CDC 客户端冲突，支持多并发读取 |
| scan.startup.mode                 | specific-offset     | CDC 启动模式，此处为「从指定 binlog 偏移量启动」，适用于补数据、断点续传场景                           |
| scan.startup.specific-offset.file | mysql-bin.004717    | 启动时指定的 binlog 文件名，即从该文件开始捕获变更数据                                                 |
| scan.startup.specific-offset.pos  | 0                   | 启动时指定的 binlog 偏移量位置，0 表示从对应 binlog 文件的起始位置开始读取                             |

sink 字段解释:

| 字段名                       | 字段值                                 | 解释                                                                                 |
| ---------------------------- | -------------------------------------- | ------------------------------------------------------------------------------------ |
| type                         | kafka                                  | 数据输出目标类型，指定为 Kafka                                                       |
| name                         | Kafka-Sink                             | Sink 组件名称，用于任务监控、日志标识，便于区分多个输出目标                          |
| properties.bootstrap.servers | kafka-1:9092,kafka-2:9092,kafka-3:9092 | Kafka 集群的连接地址，多个节点用逗号分隔，格式为「节点标识:端口」                    |
| properties.compression.type  | lz4                                    | 数据压缩格式，lz4 兼顾压缩比和读写性能，可选值还包括 gzip、snappy、none 等           |
| topic                        | binlog-cdc-defaults                    | 数据写入的 Kafka Topic 名称（需提前创建，所有同步表的变更数据默认写入该 Topic）      |
| properties.batch.size        | 40000                                  | Kafka 生产者批量发送阈值，当累计数据量达到 40000 条时触发发送，控制批量粒度          |
| properties.linger.ms         | 100                                    | 批量发送延迟时间，即使未达到 batch.size，等待 100ms 后也会触发发送，平衡延迟与吞吐量 |

transform 字段解释:

| 字段名       | 字段值                                                                                                                                  | 解释                                                                                                                                                                                                                                                                                                                                     |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| source-table | db.table1 / db.table2                                                                                                                   | 转换规则对应的源表，格式为「库名.表名」，需与 source.tables 中配置的表一致                                                                                                                                                                                                                                                               |
| projection   | 针对 db.table1：<br>"" as hostname,-1 as port,<br>"" as account,'' as password<br><br>针对 db.table2：<br>LOCALTIMESTAMP as update_time | 字段投影转换规则，用于新增、固定或转换字段（原始表字段会保留，新增字段追加）：<br>1. 字符串常量用 "" 表示，数字常量直接写值，格式为「常量/函数 as 新字段名」；<br>2. db.table1：新增 4 个固定值字段（hostname 为空串、port 为 -1、account 和 password 为空串）；<br>3. db.table2：新增 update_time 字段，值为 Flink 运行时的本地当前时间 |

## 4.3 配置文件保存为 flink-cdc.yaml

## 4.4 启动服务

```bash
./binlog-cdc --flink-cdc ./flink-cdc.yaml
```

# 5. 性能数据

| 性能指标              | 具体数值              |
| --------------------- | --------------------- |
| 内存占用              | 80MB-100MB 之间       |
| CPU 占用              | 0.1-1.0 核之间        |
| CDC 采集速率          | 3w/s（3 万条/秒）     |
| Kafka 投递速率        | 3.2w/s（3.2 万条/秒） |
| 每日投递消息数据量    | 3.2 亿左右            |
| 每日处理 CDC 事件个数 | 16 亿左右             |

# 6. 版本信息

## 1.0.0 版本

1. 支持最基本的从 mysql 指定的 binlog 文件读取 CDC 数据信息写入到对应的 Kafka 的 Topic 中
2. 支持自定义函数: LOCALTIMESTAMP,以及 "" as 或者是:-1 as 这种解析
3. 支持最新采集的 binlog 文件名字写入到 savepoint 文件夹中

## 1.1.0

修复:

1. 当 mysql 出现重启或者是网络出现问题的时候,支持自动的重新连接

特性:

1. 增加 mysql 8.0.20 的 desc xxx,获取 key 的时候的兼容支持

## 1.1.1

1. 处理兼容数据库名字含有 .\*#等特殊符号的情况

## 1.2.0

1. 支撑数据源是 Kafka--->mysql 目标的配置和功能

# 问题记录

## x.1 生产在跑了:几百万的 binlog 解析之后,出现了如下的报错信息:

```bash
2026-01-16 15:31:38
2026-01-16T15:31:38.719977769+08:00 stdout F 2026-01-16T15:31:38.717  INFO binlog_cdc::sink::kafka_sink: send message ok,keys:{"TableId":"mos2_gsms.gsms_msg_pack_sms_0116","id":1461740446534438912}!
2026-01-16 15:31:38
2026-01-16T15:31:38.719997096+08:00 stderr F

2026-01-16 15:31:38
2026-01-16T15:31:38.720020204+08:00 stderr F thread 'tokio-runtime-worker' (7) panicked at src/binlog/mod.rs:47:50:
2026-01-16 15:31:38
2026-01-16T15:31:38.720029391+08:00 stderr F read mysql binlog error!: IoError(Error { kind: InvalidData, message: "stream did not contain valid UTF-8" })
2026-01-16 15:31:38
2026-01-16T15:31:38.720037141+08:00 stderr F note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
```
