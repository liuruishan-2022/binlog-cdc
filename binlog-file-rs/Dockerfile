FROM ubuntu:24.10

# 从上一级目录的 target/release 复制编译好的二进制文件
COPY ../target/release/binlog-file-rs /opt/binlog-file-rs

WORKDIR /opt

ENTRYPOINT ["/opt/binlog-file-rs"]
