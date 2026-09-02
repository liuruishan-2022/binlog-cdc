# binlog-cdc

Rust 编写的 MySQL Binlog CDC 工具集:把 MySQL 的数据变更(binlog)实时捕获、解析,并以 Debezium JSON 格式投递到 Kafka / Console,用于数据同步、缓存刷新、报表链路等场景。

对标 Java 生态的 Flink CDC / Debezium,单二进制、低资源、无 JVM。

## Workspace 组成

| Crate | 说明 |
|---|---|
| **flink-cdc-rs** | 核心引擎:MySQL binlog → Kafka / Console 的实时同步(本文档主角) |
| mysql-binlog-connector-rust | MySQL binlog 协议客户端(副本协议解析,binlog 事件解码) |
| binlog-file-rs | 离线 binlog 文件解析工具 |
| dump-sql-cdc | mysqldump 文件解析为 CDC 事件 |
| flink-cdc-init | MySQL **全量初始化导出**工具:全量扫表 → Debezium insert 事件 → Kafka/文件/控制台(配合 flink-cdc-rs 实现先全量后增量) |

## flink-cdc-rs

### 特性

- **实时 binlog 订阅**:伪装 MySQL 副本读取 binlog,ROW 格式全量行事件(insert/update/delete)
- **Debezium JSON 输出**:`{before, after, op, source{db, table}}`,下游按 Debezium 生态消费
- **主键分区保序**:按主键 hash 分区到多个 channel,同一主键的变更严格有序,多 channel 并行消费
- **全异步流水线**:tokio + tokio mpsc,source → N channel → N sink task,无同步阻塞
- **断点续传**:savepoint 记录 binlog 位点,重启续读
- **Prometheus 指标**:吞吐/积压/延迟/发送成败全可观测
- **低资源**:release 单二进制约 3MB,tikv-jemallocator 优化高频小对象分配

### 架构

```
                          ┌── channel[0] ──▶ sink task 0 ──▶ Kafka partition
 MySQL binlog ─▶ source   ├── channel[1] ──▶ sink task 1 ──▶ Kafka partition
  (binlog 解析,          ├─      ...           ...
   行转 Debezium JSON,   └── channel[N] ──▶ sink task N ──▶ Kafka partition
   按主键 hash 选 channel)
```

### 快速开始

```bash
# 构建(release)
cargo build --release -p flink-cdc-rs

# 运行: mysql binlog -> kafka
./target/release/flink-cdc-rs --flink-cdc flink-cdc-rs/mysql-to-kafka.yaml

# 运行: mysql binlog -> console(调试用)
./target/release/flink-cdc-rs --flink-cdc flink-cdc-rs/mysql-to-console.yaml
```

### 配置示例(mysql → kafka)

```yaml
source:
  type: mysql
  name: mysql-source
  hostname: 127.0.0.1
  port: 3306
  username: <user>
  password: <password>
  tables: mydb.my_table              # 支持库.表 正则: mydb.order_[0-9]+
  server-id: "100"                   # 副本ID, 需为字符串且全局唯一
  scan.startup.mode: specific-offset # earliest-offset / latest-offset / specific-offset
  scan.startup.specific-offset.file: binlog.000001
  scan.startup.specific-offset.pos: 4
sink:
  type: kafka
  name: kafka-sink
  properties.bootstrap.servers: 10.0.0.1:9092,10.0.0.2:9092
  properties.compression.type: lz4
  topic: my-topic                    # topic 需预先存在
pipeline:
  name: mydb-to-kafka
  parallelism: 6                     # channel/sink 并行度
  capacity: 1000                     # 每个 channel 缓冲容量
```

> 前置要求:MySQL 开启 binlog 且 `binlog_format=ROW`,账号具备 `REPLICATION SLAVE, REPLICATION CLIENT` 权限;Kafka topic 需预先创建。

### 监控指标(`GET :9249/metrics`)

| 指标 | 类型 | 含义 |
|---|---|---|
| `flink_mysql_cdc{type_name}` | counter | binlog 事件数(按 write-rows/update-rows/... 分类) |
| `flink_mysql_binlog_event_timestamp` | gauge | 最新消费到的 binlog 事件时间(与当前时间差即 CDC 延迟) |
| `flink_channel_depth{index}` | gauge | 各 channel 当前积压消息数 |
| `flink_channel_usage_percent{index}` | gauge | 各 channel 占用率(**判别瓶颈:持续100%=sink慢,持续0%=source慢**) |
| `flink_sink_kafka_message_total{result}` | counter | Kafka 发送消息数(success/error) |
| `flink_sink_kafka_batch_size` | histogram | 每次 produce 批次大小 |
| `flink_sink_kafka_produce_duration_seconds` | histogram | 单次 produce 耗时(含 broker ack) |

### 性能

**压测结论**(单线程 source,channel 并行消费):

| 优化阶段 | 吞吐 | 说明 |
|---|---|---|
| glibc malloc 基线 | ~1.1 万行/s | libc 分配占 37% CPU |
| 换 tikv-jemallocator | **~1.6 万行/s(+50%)** | 分配开销降至 13% |
| 多 channel 并行消费 | 整体 6x | 18 分钟 → 3 分钟完成同等回放 |

**生产运行状态**(dev 集群,zadig 命名空间,Grafana/Prometheus 采集):
- 实例 `flink-cdc-rs-mysql131-to-kafka` 持续运行,`up=1`
- 6 个 channel `usage_percent` 长期为 0 —— sink 消费完全跟得上,零积压
- CDC 延迟秒级(binlog 事件时间戳与当前时间差 < 15s)

**已知瓶颈与优化方向**:sink 当前逐条 produce 并等待 broker ack(实测单次 ~46ms),吞吐上限受 RTT 约束;`flink_sink_kafka_batch_size` 与 `produce_duration_seconds` 两个指标可量化改进收益,攒批发送是下一步优化项(预期 5-20x)。

### Kubernetes 部署

镜像构建:`./build.sh`(cargo build + docker build/push)。部署骨架(ConfigMap + Deployment + Service + ServiceMonitor)见 flink-cdc-rs 目录,端口 9249(monitoring),ServiceMonitor 按 15s 抓取 `/metrics`。
