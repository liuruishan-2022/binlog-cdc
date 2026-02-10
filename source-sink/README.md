# Source-Sink

A flexible data streaming library that provides source and sink abstractions for building data pipelines in Rust.

## Overview

This library offers a unified interface for:
- **Sources**: Data ingestion from various systems (File, Kafka, MySQL Binlog, etc.)
- **Sinks**: Data output to various systems (Console, File, Kafka, MySQL, etc.)

## Features

### Source Implementations
- **File**: Read data from files
- **Kafka**: High-performance consumer using [rskafka](https://github.com/influxdata/rskafka) v0.6
- **MySQL**: Poll-based query source using [sqlx](https://github.com/launchbadge/sqlx)

### Sink Implementations
- **Console**: Output data to console (useful for debugging)
- **File**: Write data to files
- **Kafka**: High-performance producer using [rskafka](https://github.com/influxdata/rskafka) v0.6
- **MySQL**: Execute SQL queries using [sqlx](https://github.com/launchbadge/sqlx)

## Usage

### Configuration

The library uses YAML configuration files to define data pipelines. The configuration consists of three main sections:

- **source**: Data source configuration (Kafka, MySQL, File, Console)
- **sink**: Data sink configuration (Kafka, MySQL, File, Console)
- **pipeline**: Optional pipeline settings (parallelism, metrics, error handling)

#### Example Configuration

```yaml
source:
  type: kafka
  name: kafka_source
  properties.bootstrap.servers:
    - localhost:9092
  properties.group.id: consumer-group
  topic: input-topic
  partition: 0

sink:
  type: mysql
  name: mysql_sink
  hostname: localhost
  port: 3306
  username: root
  password: password
  database: target_db

pipeline:
  name: My Pipeline
  parallelism: 4
  metrics:
    enabled: true
  error_handling:
    mode: retry
    retry.max: 3
```

#### Loading Configuration

```rust
use source_sink::config::load_config;

let config = load_config("config.yaml");

// Access configuration
let source_config = config.source();
let sink_config = config.sink();
let pipeline_config = config.pipeline();
```

### As a Library

Add to your `Cargo.toml`:

```toml
[dependencies]
source-sink = { path = "../source-sink" }
```

Example usage:

```rust
use source_sink::config::load_config;
use source_sink::source::Source;
use source_sink::sink::Sink;

// Load configuration
let config = load_config("config.yaml");

// Use the Source and Sink traits
```

## Module Structure

```
source-sink/
├── src/
│   ├── main.rs          # Binary entry point
│   ├── lib.rs           # Library entry point
│   ├── common/          # Common utilities
│   │   ├── mod.rs       # Module exports
│   │   └── debezium.rs  # Debezium format support
│   ├── config/          # Configuration
│   │   ├── mod.rs       # Config module, Route, loader
│   │   ├── source.rs    # Source configurations
│   │   ├── sink.rs      # Sink configurations
│   │   └── pipeline.rs  # Pipeline configuration
│   ├── pipeline/        # Pipeline implementation
│   │   ├── mod.rs       # Module exports
│   │   ├── message.rs   # PipelineMessage and RouteInfo
│   │   └── pipeline.rs  # Pipeline structure
│   ├── source/          # Data source implementations
│   │   ├── mod.rs       # Source trait and exports
│   │   ├── file.rs      # File source
│   │   ├── kafka.rs     # Kafka source
│   │   └── mysql.rs     # MySQL query source
│   └── sink/            # Data sink implementations
│       ├── mod.rs       # Sink trait and exports
│       ├── console.rs   # Console sink
│       ├── file.rs      # File sink
│       ├── kafka.rs     # Kafka sink
│       └── mysql.rs     # MySQL sink
├── config.yaml          # Example configuration (Kafka to MySQL)
├── mysql-to-kafka.yaml  # Example configuration (MySQL to Kafka with routes)
├── debug.yaml           # Example configuration (Kafka to Console)
└── Cargo.toml
```

## Development

### Building

```bash
cargo build
```

### Running Tests

```bash
cargo test
```

### Running Examples

```bash
cargo run --package source-sink
```

## License

[Add your license here]
