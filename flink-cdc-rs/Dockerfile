FROM ubuntu:24.10

COPY ./target/release/binlog-cdc /opt

WORKDIR /opt

ENTRYPOINT ["/opt/binlog-cdc"]