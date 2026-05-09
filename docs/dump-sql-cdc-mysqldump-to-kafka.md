# dump-sql-cdc：一个用 Rust 编写的 mysqldump 到 Kafka 同步工具

很多数据同步系统都会选择直接消费 MySQL binlog，但在一些存量迁移、离线补数、历史数据重放场景里，我们手里拿到的往往不是 binlog，而是一批 `mysqldump` 导出的 SQL 文件。

这类 SQL 文件看起来简单：

```sql
CREATE TABLE `user` (
  `id` bigint NOT NULL,
  `name` varchar(64) DEFAULT NULL
);

INSERT INTO `user` VALUES (1, 'Alice'), (2, 'Bob');
```

但如果想把它变成一条条 CDC 消息，并以 Debezium 风格投递给 Kafka，就需要解决几个问题：

- 如何从 dump 文件中识别有效 SQL
- 如何知道每个 `INSERT` 值对应哪个字段
- 如何处理字符串、数字、NULL、二进制数据
- 如何批量解析大文件并控制内存
- 如何把解析结果高效写入 Kafka

`dump-sql-cdc` 这个子项目就是围绕这个目标实现的：读取 mysqldump 生成的 SQL 压缩包，解析其中的表结构和数据行，转换为 Debezium 风格 JSON，然后发送到 Kafka。

## 1. 整体链路

项目入口在 `dump-sql-cdc/src/main.rs`。

启动后会完成三件事：

1. 读取 YAML 配置
2. 初始化 Rayon 全局线程池
3. 启动 SQL 解析流程

核心链路可以简化成这样：

```text
zip 文件目录
   |
   v
遍历 .zip 文件
   |
   v
读取 zip 内 SQL 文件
   |
   v
按分号聚合完整 SQL
   |
   v
SQL 分类：CREATE TABLE / INSERT / OTHER
   |
   +--> CREATE TABLE -> 解析字段 -> 写入 table_cache
   |
   +--> INSERT       -> 并行解析 values -> Debezium JSON -> Kafka
```

这个设计里有一个关键点：`INSERT INTO ... VALUES ...` 本身通常不带字段名，所以必须先解析 `CREATE TABLE`，把表名和字段顺序缓存下来。后续解析 `INSERT` 时，才能把第 0 个值映射到第 0 个字段，第 1 个值映射到第 1 个字段。

## 2. 配置驱动

示例配置在 `dump-sql-cdc/config.yaml`：

```yaml
kafka:
  bootstrap-servers: localhost:9092

cdc:
  path: /path/to/mysqldump-zips
  parallelism: 4
  capacity: 100000
  channel-capacity: 1000000
  producer-threads: 4
  # password: optional-zip-password
  routes:
    - table: example_table
      topic: example-topic
      partition: 3
```

几个重要字段：

- `cdc.path`：mysqldump zip 文件所在目录
- `cdc.parallelism`：Rayon 并行解析线程数
- `cdc.capacity`：单批聚合多少条 SQL 后开始处理
- `cdc.channel-capacity`：解析线程到 Kafka 发送线程之间的 channel 容量
- `cdc.producer-threads`：Kafka 发送协程数量
- `cdc.routes`：表文件名到 Kafka topic 的路由配置

启动方式：

```bash
cargo run -p dump-sql-cdc -- --cdc-file dump-sql-cdc/config.yaml
```

配置读取使用 `clap` 解析命令行参数，再用 `serde_yaml` 反序列化：

```rust
pub fn read_config() -> CdcConfig {
    CdcConfig::read_from(args::Arguments::parse().cdc_file())
}
```

## 3. 读取 zip 文件

mysqldump 文件通常很大，所以项目没有一次性把文件读入内存，而是使用 `BufReader` 按行读取。

`MysqlDumpSqlParser::start` 会先遍历配置目录下所有 `.zip` 文件：

```rust
fn walk_sql_files(&self) -> Vec<PathBuf> {
    WalkDir::new(self.config.cdc().path())
        .into_iter()
        .filter(|ele| ele.is_ok())
        .map(|ele| ele.unwrap())
        .filter(|ele| ele.file_type().is_file())
        .filter(|ele| ele.path().extension().and_then(|ext| ext.to_str()) == Some("zip"))
        .map(|ele| ele.into_path())
        .collect::<Vec<PathBuf>>()
}
```

然后逐个打开 zip 包，读取其中的 SQL 文件。如果配置了 `password`，则走加密 zip 解压逻辑；否则直接按文件名读取。

路由判断也发生在这里：

```rust
if let Some(topic) = self.config.cdc().search_topic(&actual_filename) {
    self.process_sql_file(reader, &topic).await;
} else {
    warn!("文件名字：{} 不在配置文件内,忽略读取!", filename);
}
```

也就是说，只有匹配到 `routes.table` 前缀的文件才会继续解析，不在配置中的文件会被忽略。这可以减少无关 SQL 的解析成本。

## 4. 按 SQL 语句聚合

dump 文件里一条 SQL 可能跨多行，所以不能简单地按行解析。

项目的处理方式是：

- 跳过空行
- 跳过 `/* ... */` 和 `-- ...` 开头的注释行
- 把有效行追加到 `sql_line`
- 当发现 SQL 以 `;\n` 结尾时，认为得到了一条完整 SQL
- 累积到 `capacity` 后批量解析

核心逻辑在 `process_sql_file`：

```rust
while reader.read_until(b'\n', &mut line_bytes).unwrap() > 0 {
    if line_bytes.len() <= 2 {
        line_bytes.clear();
        continue;
    }

    if (line_bytes[0] == b'/' && line_bytes[1] == b'*')
        || (line_bytes[0] == b'-' && line_bytes[1] == b'-')
    {
        line_bytes.clear();
        continue;
    }

    sql_line.append(&mut line_bytes);

    if sql_line.ends_with(&[b';', b'\n']) {
        sql_buffer.push(sql_line);

        if sql_buffer.len() >= self.config.cdc().capacity() as usize {
            self.parallel_parse(sql_buffer, topic).await;
            sql_buffer = Vec::new();
        }

        sql_line = Vec::new();
    }
}
```

这里选择 `Vec<u8>` 而不是 `String`，是一个很实际的选择。mysqldump 里可能包含非 UTF-8 字节、二进制字段、转义字符，如果过早转成字符串，很容易在边界数据上失败。

## 5. SQL 分类：先快筛，再精 parse

不是所有 SQL 都值得完整解析。项目先通过字节匹配做 SQL 类型分类：

```rust
pub enum SqlType {
    Insert,
    CreateTable,
    DropTable,
    LockTables,
    UnlockTables,
    Other,
}
```

`classify_sql` 会跳过前导空白，然后将字节转成大写做快速判断：

```rust
if sql_upper.starts_with(b"INSERT INTO") {
    return SqlType::Insert;
}

if sql_upper.starts_with(b"CREATE")
    && sql_upper.contains(&b'T')
    && sql_upper.contains(&b'A')
    && sql_upper.contains(&b'B')
    && sql_upper.contains(&b'L')
    && sql_upper.contains(&b'E')
{
    return SqlType::CreateTable;
}
```

这样做的收益是明显的：大部分无关 SQL 可以快速过滤，不需要进入完整 SQL parser。

## 6. 解析 CREATE TABLE：建立字段顺序缓存

`CREATE TABLE` 使用 `sqlparser` 解析。因为建表语句相对复杂，用成熟 parser 比自己手写更稳。

当识别到 `Statement::CreateTable` 后，项目提取表名和字段列表：

```rust
fn parse_create_table(&mut self, event: &CreateTable) {
    let name = &event.name.0.get(0).unwrap();
    match name {
        sqlparser::ast::ObjectNamePart::Identifier(ident) => {
            let columns = self.parse_columns(event);
            let table = Table::new(ident.value.clone(), columns);
            self.table_cache.insert(ident.value.clone(), table);
        }
        _ => {
            warn!("忽略");
        }
    }
}
```

字段结构里保存了三个信息：

```rust
pub struct Column {
    index: usize,
    name: String,
    data_type: DataType,
}
```

最关键的是 `index`。因为 mysqldump 的 `INSERT` 常见格式是：

```sql
INSERT INTO `user` VALUES (1, 'Alice');
```

它没有写字段名。只有通过建表语句拿到字段顺序，才能知道 `1` 是 `id`，`Alice` 是 `name`。

## 7. 解析 INSERT：为 mysqldump 定制一个轻量 parser

`INSERT` 没有直接交给 `sqlparser`，而是项目自己实现了一个轻量 lexer 和 parser，位置在 `dump-sql-cdc/src/parser/insert.rs`。

支持的 token 包括：

- `INSERT`
- `INTO`
- `VALUES`
- 表名
- 字符串字面量
- 数字
- `NULL`
- `_binary`
- 括号、逗号、分号、反引号

解析入口很清晰：

```rust
pub fn parse_insert(sql: &[u8]) -> Result<InsertStatement, String> {
    let mut lexer = Lexer::new(sql.to_vec());
    let tokens = lexer.tokenize()?;
    let mut parser = Parser::new(tokens);
    parser.parse()
}
```

parser 期望的语法形态是：

```text
INSERT INTO table_name VALUES (...), (...), ...;
```

也就是 mysqldump 最常见的批量插入形式。

字符串解析里处理了 MySQL 常见转义：

```rust
match escaped {
    b'0' => result.push(0x00),
    b'\'' => result.push(b'\''),
    b'"' => result.push(b'"'),
    b'\\' => result.push(b'\\'),
    b'n' => result.push(b'\n'),
    b'r' => result.push(b'\r'),
    b't' => result.push(b'\t'),
    b'b' => result.push(0x08),
    b'Z' => result.push(0x1A),
    b'%' => result.push(b'%'),
    b'_' => result.push(b'_'),
    _ => result.push(escaped),
}
```

对于 `_binary '...'` 这类字段，项目会标记成二进制数据，后续转 JSON 时做 base64 编码。

## 8. 转换成 Debezium 风格 JSON

解析出一行 values 后，就要和 `table_cache` 中的字段顺序合并。

`DebeziumFormat::build` 会逐行转换：

```rust
pub fn build(table: &Table, rows: Vec<Vec<Value>>) -> Vec<Self> {
    rows.into_iter()
        .map(|row| DebeziumFormat::single_row(table, row))
        .collect()
}
```

单行转换逻辑是按下标找字段名：

```rust
for (index, data) in row.into_iter().enumerate() {
    if let Some(column) = table.columns_by_index(index) {
        map.insert(column.name().to_string(), data);
    }
}
```

最终输出结构是一个简化版 Debezium envelope：

```json
{
  "before": null,
  "after": {
    "id": 1,
    "name": "Alice"
  },
  "op": "c",
  "source": {
    "table": "user"
  }
}
```

这里的 `op: "c"` 表示 create，也就是插入事件。因为 mysqldump 表达的是历史快照数据，不包含 update/delete 的变更过程，所以当前项目只生成 insert 语义。

需要注意：这不是完整 Debezium Kafka Connect envelope，没有 schema、ts_ms、db、server_id 等字段。它更像是一个面向下游消费的轻量 Debezium 风格消息。

## 9. 并行解析和异步发送

真正处理批次数据的是 `parallel_parse`。

它先把 SQL 按类型分流：

```rust
let mut diversion = sql_buffer
    .into_iter()
    .fold(HashMap::new(), |mut map, sql_line| {
        let sql_type = sql_parser::classify_sql(&sql_line);
        map.entry(sql_type).or_insert_with(Vec::new).push(sql_line);
        map
    });
```

然后先处理 `CREATE TABLE`，更新表结构缓存；再处理 `INSERT`。

`INSERT` 解析使用 Rayon 并行执行：

```rust
sql_lines
    .into_par_iter()
    .filter_map(|line| self.parse_insert(line))
    .for_each(|ele| {
        tx.send(ele).expect("发送数据到channel tx失败");
    });
```

Kafka 发送侧则启动多个 tokio task，从 crossbeam channel 里取解析好的 Debezium 消息：

```rust
for _ in 0..producer_threads {
    let rx_clone: crossbeam_channel::Receiver<Vec<DebeziumFormat>> = rx.clone();
    let sink = sink.clone();
    let topic = topic_clone.clone();

    let handle = tokio::spawn(async move {
        while let Ok(data) = rx_clone.recv() {
            sink.send_messages(data, &topic).await;
        }
    });

    send_handles.push(handle);
}
```

这里 `Receiver::clone()` 的语义是多个消费者竞争同一个队列。每批消息只会被一个发送 task 消费，不会广播给所有 task。

这个模型把 CPU 密集的 SQL 解析和 IO 密集的 Kafka 发送拆开了：

- Rayon 负责并行解析
- crossbeam channel 负责连接解析和发送
- tokio task 负责异步投递 Kafka

## 10. Kafka 投递

Kafka 发送实现位于 `dump-sql-cdc/src/kafka/mod.rs`，使用的是 `rskafka`。

项目会缓存 `PartitionClient`，避免每次发送都重新创建：

```rust
partition_clients: Arc<Mutex<HashMap<String, Arc<Producer>>>>,
```

发送时先根据 topic 找到配置的分区数量，然后随机选择一个分区：

```rust
let random_partition = if partition_count > 0 {
    rand::thread_rng().gen_range(0..partition_count)
} else {
    0
};
let producer = self.producer(topic, random_partition).await;
```

最后把 Debezium JSON 转成 Kafka `Record`：

```rust
impl From<DebeziumFormat> for Record {
    fn from(ele: DebeziumFormat) -> Self {
        let body = ele.to_json();
        let key = Local::now().timestamp_millis();
        let key = key.to_string().as_bytes().to_vec();

        Record {
            key: Some(key.clone()),
            value: Some(body.as_bytes().to_vec()),
            headers: BTreeMap::from([("key".to_string(), key)]),
            timestamp: Utc::now(),
        }
    }
}
```

批量发送时使用 LZ4 压缩：

```rust
producer
    .produce(records, rskafka::client::partition::Compression::Lz4)
    .await
```

## 11. 为什么适合用 Rust

这个场景很适合 Rust：

- 文件很大，需要控制内存分配
- SQL 里可能有非 UTF-8 字节，不能轻易用字符串处理
- 解析逻辑 CPU 密集，适合多线程并行
- Kafka 投递是 IO 密集，适合异步运行时
- 类型系统可以把 `Table`、`Column`、`DebeziumFormat`、`Record` 的边界定义清楚

项目还使用了 `tikv-jemallocator` 作为全局 allocator：

```rust
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
```

对于大量小对象分配、JSON 构造、批量解析这种场景，jemalloc 往往能减少内存分配带来的抖动。

## 12. 当前实现的边界

这个项目更像是一个面向 mysqldump 快照导入的 CDC 工具，而不是完整 SQL 引擎，所以它有明确边界：

- 当前主要处理 `CREATE TABLE` 和 `INSERT INTO ... VALUES ...`
- `INSERT` 默认依赖建表语句中的字段顺序
- 输出是简化 Debezium 风格，不是完整 Debezium envelope
- Kafka key 当前使用时间戳生成，没有按业务主键分区
- 发送侧使用 `crossbeam_channel::Receiver::recv()`，在 tokio task 中是阻塞调用，后续可以考虑 `spawn_blocking` 或改成 async channel

这些边界不是问题，但需要在生产使用前明确。

## 13. 可以继续优化的方向

如果要把这个项目继续往生产级推进，可以优先考虑这些点：

1. Kafka key 改为业务主键

   现在 Kafka key 使用当前毫秒时间戳。更理想的方式是根据表主键或路由字段生成 key，这样同一业务实体可以稳定进入同一个分区。

2. 输出完整 Debezium envelope

   如果下游系统严格依赖 Debezium 标准结构，可以补充 `schema`、`payload`、`ts_ms`、`source.db` 等字段。

3. 更细的错误处理

   当前部分解析错误使用 `expect` 直接中断。生产环境可以改成错误计数、坏数据落盘、继续处理下一批。

4. 改造发送侧阻塞接收

   `crossbeam_channel` 很适合多线程同步场景，但在 tokio task 中直接 `recv()` 会阻塞运行时线程。可以改成 `tokio::sync::mpsc`，或者把阻塞接收放到 `spawn_blocking`。

5. 按表过滤提前下推

   项目已经通过文件名路由减少无关文件解析。如果一个 zip 中包含多个表，还可以在 SQL 分类前做更细的表级过滤。

## 14. 小结

`dump-sql-cdc` 的核心思路很清晰：

先从 `CREATE TABLE` 中拿到字段元数据，再从 `INSERT` 中解析行数据，最后按字段顺序组装成 Debezium 风格 JSON 并写入 Kafka。

它没有试图做一个完整 SQL 数据库解析器，而是抓住 mysqldump 的稳定格式做定制解析：`CREATE TABLE` 用成熟 parser，`INSERT VALUES` 用轻量字节级 parser，批量处理用 Rayon，投递用 rskafka。

这也是 Rust 做这类数据工具比较舒服的地方：既能贴近字节流处理，又能通过类型系统把复杂链路拆成清晰的结构。
