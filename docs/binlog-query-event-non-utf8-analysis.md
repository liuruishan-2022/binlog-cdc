# Binlog QueryEvent 非 UTF-8 字节解析失败分析

## 问题现象

使用 `binlog-file-rs` 解析某个 MySQL binlog 文件时，程序报错：

```text
IoError(Error { kind: InvalidData, message: "stream did not contain valid UTF-8" })
```

该错误可以稳定复现。

## 定位结果

第一处触发错误的位置如下：

```text
binlog position: 85788372
end_log_pos:     85789408
event type:      Query
schema:          <database_name>
```

对应的是一条建表 DDL。为避免暴露业务表名，表名已脱敏：

```sql
CREATE TABLE `<masked_table_name>` (
  ...
  `user` int(11) DEFAULT NULL COMMENT 'ÓÃ»§ID',
  ...
) ENGINE=InnoDB AUTO_INCREMENT=307995 DEFAULT CHARSET=utf8
```

触发 UTF-8 解析失败的原始字节位于字段注释中：

```text
d3 c3 bb a7 49 44
```

其中 `d3 c3 bb a7` 不是合法 UTF-8 字节序列，但按 GBK/GB2312 语境看，很像中文“用户”。因此该 DDL 很可能来自历史客户端、导入工具或连接字符集配置不一致的场景：SQL 文本中包含了非 UTF-8 编码字节，但最终被原样记录进了 binlog 的 `QueryEvent`。

## 为什么 mysqlbinlog 可以解析

使用如下命令检查该位置：

```bash
mysqlbinlog -vvv \
  --start-position=85788372 \
  --stop-position=85789408 \
  <BINARY_LOG_FILE>
```

`mysqlbinlog` 可以正常输出该事件，说明 binlog 文件结构本身大概率是合法的。`mysqlbinlog` 主要校验和解析的是：

- magic number
- event header
- event length
- event type
- CRC32 checksum
- binlog event 边界

但 `QueryEvent` 中的 SQL payload 本质上是字节流，不保证一定是 UTF-8。MySQL 历史上支持多种字符集，binlog 中的 SQL 文本不能简单等同于 UTF-8 字符串。

因此：

```text
mysqlbinlog 能解析成功，不代表 QueryEvent 中的 SQL 文本一定是合法 UTF-8。
```

## Rust 解析失败原因

当前使用的 `mysql-binlog-connector-rust` 在 `QueryEvent::parse` 中直接将剩余 payload 读取为 Rust `String`：

```rust
let mut query = String::new();
cursor.read_to_string(&mut query)?;
```

`read_to_string` 要求输入字节必须是合法 UTF-8。一旦 QueryEvent 的 SQL payload 中存在非 UTF-8 字节，就会返回：

```text
ErrorKind::InvalidData
```

这正是当前报错的来源。

## 结论

该问题不是 binlog 文件损坏导致的，而是解析库对 `QueryEvent` 的文本编码做了过强假设。

更准确地说：

- binlog 文件结构合法。
- 出错 event 是一个 `QueryEvent`。
- `QueryEvent` 的 SQL payload 中包含非 UTF-8 字节。
- `mysql-binlog-connector-rust` 使用 `read_to_string` 强制按 UTF-8 解析。
- 因此遇到非 UTF-8 字节时解析失败。

## 建议修复方向

`QueryEvent` 不应直接使用 `read_to_string`。可以改为先读取原始 bytes，再根据需求进行处理。

兼容性较好的修复方式：

```rust
let mut buf = Vec::new();
cursor.read_to_end(&mut buf)?;
let query = String::from_utf8_lossy(&buf).to_string();
```

更严谨的方案是保留原始字节：

```rust
query_raw: Vec<u8>
```

然后根据 binlog 中的字符集信息或业务侧配置决定如何解码。对于 CDC 主链路，如果不需要处理 DDL，也可以将 `QueryEvent` 作为可跳过事件，避免非核心事件阻断 row event 解析。
