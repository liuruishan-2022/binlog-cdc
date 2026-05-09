# Loki 部署模式选择：单体、读写分离与微服务模式

上一篇文档已经跑通了 `Agent -> Loki -> Grafana` 的最小日志闭环。接下来要考虑 Loki 的部署方式。

本文基于 Loki 官方部署模式文档整理：

```text
https://grafana.com/docs/loki/latest/get-started/deployment-modes/
```

无论 Loki 选择哪种部署方式，都需要先考虑两个问题：

1. Loki 自身如何扩展：写入、查询、索引、压缩这些组件是否能按压力独立扩容。
2. Loki 需要的存储如何扩展：日志数据量很大，后端存储是否能解决容量、IO、单点和长期保留问题。

所以 Loki 部署不是只选组件模式，还要同步选择日志底层存储。

## 1. Loki 的三种部署模式

官方文档中主要介绍三种部署模式：

这里先给一个经验估算口径，后面表格里的原始日志量和日志行数都按这个口径换算：

- Loki 日志压缩率大约是 `5-7` 倍。
- `1GB` 原始日志大约是 `500w` 行日志。
- 因此 `20GB/day` 压缩后日志，大致对应 `100-140GB/day` 原始日志，也就是 `5-7亿行/day`。
- `1TB/day` 压缩后日志，大致对应 `5-7TB/day` 原始日志，也就是 `2.5-3.5万亿行/day`。

| 部署模式 | 官方名称 | 典型 target | 压缩后日志量参考 | 原始日志量估算 | 日志行数估算 | 适用场景 |
| --- | --- | --- | --- | --- | --- | --- |
| 单体模式 | Monolithic mode | `-target=all` | 约 `20GB/day` 以内 | 约 `100-140GB/day` | 约 `5-7亿行/day` | 入门、测试、小规模日志 |
| 读写分离模式 | Simple Scalable Deployment，简称 SSD | `write`、`read`、`backend` | 可接近 `1TB/day` | 约 `5-7TB/day` | 约 `2.5-3.5万亿行/day` | 中等规模、需要分别扩展读写能力 |
| 微服务模式 | Microservices mode / Distributed deployment | `distributor`、`ingester`、`querier` 等 | TB 级及以上 | 按压缩比和实际写入量估算 | 按 `1GB≈500w行` 估算 | 大规模生产、需要精细化扩展和运维控制 |

需要特别注意：官方文档说明 Simple Scalable Deployment，也就是 SSD 模式正在被废弃，具体时间待定，但会发生在 Loki `4.0` 发布之前。因此新生产环境更推荐直接考虑微服务模式。

## 2. 先选择 Loki 存储

Loki 的日志数据主要包括两部分：

| 数据类型 | 说明 |
| --- | --- |
| chunks | 真实日志内容经过切分、压缩后形成的数据块 |
| index | 用于按 label 和时间范围定位日志块的索引数据 |

在小规模本地测试中，可以直接使用文件系统存储。但一旦进入生产环境，就不能只考虑“能不能写入”，还要考虑几个问题：

- 日志每天持续增长，本地磁盘容量很快会成为瓶颈。
- 写入和查询都会访问存储，单机磁盘 IO 容易成为瓶颈。
- 单机文件系统有单点故障风险。
- 节点迁移、扩容、重建时，本地数据处理复杂。
- 日志通常需要保留几十天甚至更久，生命周期管理很重要。

因此，Loki 生产环境通常不建议长期使用单机本地磁盘存储，而是使用对象存储，例如 S3、MinIO、GCS、Azure Blob 等。

### 2.1 常见存储方式对比

| 存储方式 | 优点 | 缺点 | 适用场景 |
| --- | --- | --- | --- |
| 本地文件系统 | 配置简单，上手快 | 单点、容量有限、磁盘 IO 瓶颈明显 | 本地测试、单体入门 |
| NFS | 接入简单，多节点可共享 | IO 性能和稳定性容易成为瓶颈，故障影响面大 | 早期过渡方案，不建议长期承载大日志量 |
| MinIO | S3 兼容，适合私有化部署，可扩展 | 需要维护 MinIO 集群和容量规划 | 私有化、内网环境、Kubernetes 场景 |
| 公有云对象存储 | 高可用、容量弹性、生命周期能力成熟 | 成本和云厂商绑定需要评估 | 云上生产环境 |

### 2.2 为什么生产环境更推荐对象存储

对象存储更适合 Loki 的原因：

- 容量扩展比单机磁盘和 NFS 更自然。
- 可以规避单台 Loki 节点本地磁盘故障导致的数据风险。
- 更适合 chunks 和 index 这类大量对象数据。
- 可以和保留周期、生命周期策略结合使用。
- 微服务模式下多个组件可以共享同一份后端存储。

### 2.3 对象存储成本和日志保留量估算

以华为云 OBS 单中心对象存储为例，如果按 `1TB` 存储空间约 `1200元/年` 估算，那么平均下来大约是：

```text
1200元 / 年 ≈ 100元 / 月
```

如果 Loki 使用这 `1TB` 对象存储，并且日志保留 `40天`，那么每天可使用的压缩后日志空间大约是：

```text
1TB / 40天 ≈ 25GB/day
```

按照 Loki 日志压缩率 `5-7` 倍估算：

```text
25GB/day 压缩后日志 * 5-7 = 125-175GB/day 原始日志
```

再按 `1GB` 原始日志约 `500w` 行日志估算：

```text
125GB/day * 500w行 ≈ 6.25亿行/day
175GB/day * 500w行 ≈ 8.75亿行/day
```

也就是说，`1TB` OBS 存储用于 Loki，保留 `40天` 时，大致可以保存：

| 维度 | 估算结果 |
| --- | --- |
| 压缩后日志总量 | `1TB` |
| 原始日志总量 | `5-7TB` |
| 40 天总日志行数 | 约 `250-350亿行` |
| 每天原始日志量 | 约 `125-175GB/day` |
| 每天日志行数 | 约 `6.25-8.75亿行/day` |
| 存储成本 | 约 `1200元/年` |

日志通常是由外部请求直接或间接产生的。为了把日志容量换算成业务请求量，可以再做一个均摊假设：

```text
70 行日志 ≈ 1 次外部请求
```

那么每天可支撑的请求量大约是：

```text
6.25亿行 / 70 ≈ 893w 次请求/day
8.75亿行 / 70 ≈ 1250w 次请求/day
```

40 天总请求量大约是：

```text
250亿行 / 70 ≈ 3.57亿次请求
350亿行 / 70 ≈ 5亿次请求
```

换算成业务侧指标：

| 维度 | 估算结果 |
| --- | --- |
| 每天可支撑请求量 | 约 `900-1250w 次/day` |
| 40 天总请求量 | 约 `3.5-5亿次` |
| 单次请求日志行数 | 按 `70行/request` 估算 |

这个估算只计算对象存储容量费用，不包含请求费用、流量费用、跨 AZ/跨区域费用、MinIO 自建机器成本、查询缓存成本等。实际成本还要结合云厂商计费项和查询访问频率评估。

### 2.4 当前 Kubernetes 示例的存储配置

在当前 Kubernetes 微服务部署示例中，Loki 使用的是 S3 兼容存储，后端是 MinIO：

```yaml
loki:
  schemaConfig:
    configs:
      - from: "2026-03-10"
        store: tsdb
        object_store: s3
        schema: v13
        index:
          prefix: loki_index_
          period: 24h
  storage:
    bucketNames:
      chunks: loki-v3.5.0
      ruler: loki-v3.5.0
    s3:
      endpoint: http://minio.logs:9000
      region: us-east-1
      s3ForcePathStyle: true
      insecure: true
      accessKeyId: <ACCESS_KEY>
      secretAccessKey: <SECRET_KEY>
```

这个配置的核心含义是：

- 使用 TSDB schema v13。
- `object_store: s3` 表示 Loki 把数据写入 S3 兼容对象存储。
- MinIO 通过 `endpoint` 暴露 S3 兼容接口。
- chunks 和 ruler 数据使用同一个 bucket。

所以，部署 Loki 的顺序应该是：

```text
先确定存储方案 -> 再选择部署模式 -> 最后根据日志量和查询压力调整组件副本数
```

## 3. 单体模式

单体模式通过 `-target=all` 启动。所有 Loki 微服务组件都在一个进程里运行。

官方图如下：

![Loki monolithic mode](assets/loki-monolithic-mode.png)

官方对单体模式的定位是：快速开始、实验、以及每天大约 `20GB` 以内的小规模读写量。

| 维度 | 说明 |
| --- | --- |
| 启动方式 | `-target=all` |
| 日志采集量 | 官方建议约 `20GB/day` 以内 |
| 查询能力 | 查询并行度受实例数量和 `max_query_parallelism` 限制 |
| 优点 | 部署简单，适合快速验证 |
| 缺点 | 读写、查询、压缩、索引等能力都集中在一个进程中，资源隔离弱 |
| 适用场景 | 本地开发、测试环境、小团队、小日志量系统 |

如果需要水平扩展单体模式，可以让多个 Loki 实例共享对象存储，并通过 ring 配置共享状态。但官方建议：如果已经需要明显扩展，优先考虑微服务模式。

## 4. 读写分离模式 SSD

Simple Scalable Deployment 会把 Loki 拆成三类执行路径：

- `write`
- `read`
- `backend`

官方图如下：

![Loki simple scalable mode](assets/loki-simple-scalable-mode.png)

官方文档说明，SSD 模式把执行路径拆成读、写、后台任务三类，可以分别扩展，以便根据日志写入量和查询量调整基础设施成本。

| target | 包含组件 | 说明 |
| --- | --- | --- |
| `write` | Distributor、Ingester | 负责写入路径，通常是有状态组件 |
| `read` | Query Frontend、Querier | 负责查询路径，主要提升查询并发 |
| `backend` | Compactor、Index Gateway、Query Scheduler、Ruler、Bloom 组件 | 负责压缩、索引、调度、规则等后台任务 |

容量上，官方描述 SSD 模式可以扩展到接近 `1TB/day` 的日志量。但官方也明确说明：即使继续扩展可能可行，到这个量级时，微服务模式在可扩展性和运维便利性上会是更好的选择。

| 维度 | 说明 |
| --- | --- |
| 日志采集量 | 可接近 `1TB/day` |
| 查询能力 | `read` 节点可以独立扩容 |
| 优点 | 比单体更容易扩展，读写路径分离 |
| 缺点 | SSD 正在被废弃；需要 gateway 或反向代理路由读写请求 |
| 适用场景 | 中等规模日志系统、历史存量部署 |

由于 SSD 已经进入废弃路径，新系统不建议把它作为长期目标架构。

## 5. 微服务模式

微服务模式也叫 Distributed deployment。它会把 Loki 的各个组件作为独立进程运行，每个组件通过独立 `target` 启动。

官方图如下：

![Loki microservices mode](assets/loki-microservices-mode.png)

官方文档列出的微服务组件包括：

- Distributor
- Ingester
- Querier
- Query Frontend
- Query Scheduler
- Index Gateway
- Compactor
- Ruler
- Bloom Planner / Bloom Builder / Bloom Gateway
- Overrides Exporter

微服务模式的核心价值是：每个组件都可以按实际压力独立扩容。

| 压力类型 | 主要扩容组件 |
| --- | --- |
| 写入压力高 | Distributor、Ingester |
| 查询并发高 | Query Frontend、Querier、Query Scheduler |
| 索引访问压力高 | Index Gateway |
| 压缩、保留、删除压力高 | Compactor |
| 告警和规则计算压力高 | Ruler |

官方对微服务模式的定位是：适合非常大的 Loki 集群，或者需要对扩展和集群运维有更精细控制的团队。它也是为 Kubernetes 部署设计的模式。

| 维度 | 说明 |
| --- | --- |
| 日志采集量 | TB 级及以上，取决于组件副本数、对象存储、缓存和限流配置 |
| 查询能力 | 可独立扩展 query-frontend、querier、query-scheduler |
| 优点 | 扩展最灵活，组件职责清晰，适合生产高可用 |
| 缺点 | 组件最多，部署和维护复杂度最高 |
| 适用场景 | 大规模生产、多团队、多业务线、高查询并发场景 |

## 6. 三种模式如何选择

可以按日志量、团队规模和运维复杂度大致选择：

| 场景 | 推荐模式 |
| --- | --- |
| 本地学习、功能验证 | 单体模式 |
| 每天几十 GB 日志以内 | 单体模式 |
| 接近 `1TB/day`，已有 SSD 存量部署 | SSD 可继续维护，但要规划迁移 |
| 新生产环境 | 微服务模式 |
| 多团队、多租户、高查询并发 | 微服务模式 |
| 需要对写入、查询、压缩、索引分别扩容 | 微服务模式 |

一个简单判断是：

```text
能用单体跑通，就先用单体理解链路；
需要生产化和长期扩展，就直接考虑微服务模式；
SSD 不建议作为新系统的长期目标。
```

## 7. Kubernetes 中使用 Helm 部署微服务模式

下面以实际部署在 Kubernetes 中的 Loki 为例说明微服务模式。

当前集群中的 release 信息：

```text
NAME: loki
NAMESPACE: logs
STATUS: deployed
REVISION: 1
LAST DEPLOYED: 2026-03-11 19:00:16 +0800
Chart: loki-6.54.0
Loki version: 3.6.7
```

Helm status 显示安装的组件包括：

```text
gateway
compactor
index gateway
query scheduler
ruler
distributor
ingester
querier
query frontend
```

这说明它不是单体，也不是 SSD，而是微服务模式。

### 7.1 官方 Helm 部署步骤

官方微服务 Helm 文档地址：

```text
https://grafana.com/docs/loki/latest/setup/install/helm/install-microservices/
```

官方部署步骤大致是：

```bash
helm repo add grafana-community https://grafana-community.github.io/helm-charts
helm repo update
helm install --values values.yaml loki grafana-community/loki
```

如果是升级：

```bash
helm upgrade --values values.yaml loki grafana-community/loki
```

官方还强调：微服务模式不建议使用 filesystem 存储，应该使用对象存储。测试环境可以使用 MinIO，生产环境建议使用 S3、Azure Blob、GCS 或兼容对象存储。

### 7.2 当前生产示例 values

当前 release 的关键 values 如下，已对对象存储访问密钥做脱敏处理：

```yaml
# 使用微服务 / 分布式部署模式。
deploymentMode: Distributed

loki:
  # 是否启用 Loki 自身多租户鉴权。
  # 这里关闭，通常由 gateway / Grafana / 外部网关负责访问控制。
  auth_enabled: false

  ingester:
    # chunk 压缩算法。snappy 压缩和解压速度较快，适合日志写入场景。
    chunk_encoding: snappy

  pattern_ingester:
    # Loki pattern ingester，支持 pattern 相关能力。
    enabled: true

  querier:
    # 单个 querier 同时执行的最大查询数量。
    max_concurrent: 4

  limits_config:
    # 允许结构化元数据。
    allow_structured_metadata: true
    # 启用 volume API，用于 Grafana 中部分日志 volume 能力。
    volume_enabled: true
    # 每个租户平均写入速率限制，单位 MB/s。
    ingestion_rate_mb: 100
    # 每个租户写入突发限制，单位 MB。
    ingestion_burst_size_mb: 150
    # 单个 stream 的写入速率限制。
    per_stream_rate_limit: 100MB
    # 单个 stream 的写入突发限制。
    per_stream_rate_limit_burst: 300MB
    # 每个租户允许的全局 stream 数量上限。
    max_global_streams_per_user: 500000
    # 每个日志序列允许的 label 名称数量上限。
    max_label_names_per_series: 30
    # 是否拒绝过旧日志。
    reject_old_samples: true
    # 超过这个时间窗口的旧日志会被拒绝。
    reject_old_samples_max_age: 168h
    # 日志保留周期。
    retention_period: 60d
    # 查询切分间隔，降低大范围查询压力。
    split_queries_by_interval: 15m
    # 查询结果缓存的新鲜度限制。
    max_cache_freshness_per_query: 10m
    shard_streams:
      # 自动 stream 分片，缓解超大 stream 写入压力。
      enabled: true

  schemaConfig:
    # Loki schema 配置，决定索引结构、对象存储类型和生效时间。
    configs:
      # schema 生效起始日期。新集群可以设置为部署当天或未来不会回退的日期。
      - from: "2026-03-10"
        # 索引存储类型。Loki 2.8+ 推荐使用 tsdb。
        store: tsdb
        # 对象存储类型。这里使用 s3，MinIO 也通过 S3 兼容协议接入。
        object_store: s3
        # schema 版本。v13 是当前 TSDB 推荐 schema。
        schema: v13
        index:
          # 索引文件前缀。
          prefix: loki_index_
          # 索引周期。TSDB 通常使用 24h。
          period: 24h

  storage:
    # bucket 名称配置。
    bucketNames:
      # chunks bucket，存放真实日志块数据。
      chunks: loki-v3.5.0
      # ruler bucket，存放 ruler 规则相关数据。
      ruler: loki-v3.5.0
    s3:
      # S3 兼容对象存储 endpoint。这里以 logs 命名空间内的 MinIO 为例。
      endpoint: http://minio.logs:9000
      # S3 region。MinIO 场景可使用兼容值，例如 us-east-1。
      region: us-east-1
      # 是否强制使用 path-style URL。MinIO 通常需要设置为 true。
      s3ForcePathStyle: true
      # 是否使用 http 非 TLS 访问。内网 MinIO 常见为 true；生产公网访问建议使用 TLS。
      insecure: true
      # 对象存储访问 AK。文档中必须脱敏。
      accessKeyId: <ACCESS_KEY>
      # 对象存储访问 SK。文档中必须脱敏。
      secretAccessKey: <SECRET_KEY>

gateway:
  # gateway 是 Loki 对外统一入口，负责把读写请求路由到不同组件。
  basicAuth:
    enabled: false
  service:
    type: ClusterIP

# 关闭单体模式。
singleBinary:
  replicas: 0

# 关闭 SSD 读路径。
read:
  replicas: 0

# 关闭 SSD 写路径。
write:
  replicas: 0

# 关闭 SSD backend 路径。
backend:
  replicas: 0

distributor:
  # 写入入口，负责接收 push 请求并分发给 ingester。
  replicas: 3
  maxUnavailable: 2

ingester:
  # 写入核心组件，负责接收日志并生成 chunk。
  replicas: 3
  zoneAwareReplication:
    enabled: false

querier:
  # 查询执行组件。
  replicas: 3
  maxUnavailable: 2

queryFrontend:
  # 查询入口，负责查询拆分、排队、缓存等。
  replicas: 2
  maxUnavailable: 1

queryScheduler:
  # 查询调度组件，协调 query-frontend 和 querier。
  replicas: 2

indexGateway:
  # 索引访问组件，降低 querier 直接访问对象存储索引的压力。
  replicas: 2
  maxUnavailable: 1

compactor:
  # 后台压缩、retention、delete 等任务。
  replicas: 1

ruler:
  # 规则计算组件。当前示例未启用副本，可按告警需求开启。
  replicas: 0

chunksCache:
  # chunks 缓存，降低对象存储读取压力。
  enabled: true

resultsCache:
  # 查询结果缓存，降低重复查询压力。
  enabled: true

bloomPlanner:
  replicas: 0
bloomBuilder:
  replicas: 0
bloomGateway:
  replicas: 0

# 不启用 chart 内置 MinIO，使用外部已有对象存储。
minio:
  enabled: false
```

几个关键点：

- `deploymentMode: Distributed`：明确使用微服务模式。
- `singleBinary/read/write/backend replicas: 0`：关闭单体和 SSD 三目标模式。
- `distributor.replicas: 3`：写入入口可横向扩展。
- `ingester.replicas: 3`：写入数据会进入多个 ingester，提升写入能力和可靠性。
- `querier.replicas: 3`、`queryFrontend.replicas: 2`、`queryScheduler.replicas: 2`：查询链路独立扩容。
- `indexGateway.replicas: 2`：索引访问独立扩展。
- `compactor.replicas: 1`：负责压缩、保留和后台整理。
- `object_store: s3`：使用 S3 兼容对象存储。
- `retention_period: 60d`：日志保留 60 天。
- `ingestion_rate_mb: 100`、`ingestion_burst_size_mb: 150`：提高租户写入限流。

### 7.3 安装命令

假设 values 文件名为 `values-loki-distributed.yaml`，安装到 `logs` 命名空间：

```bash
helm upgrade --install loki grafana-community/loki \
  -n logs \
  --create-namespace \
  -f values-loki-distributed.yaml
```

如果使用已有 chart 包或私有 Helm 仓库，把 `grafana-community/loki` 替换成实际 chart 地址即可。

### 7.4 验证组件

查看 Helm release：

```bash
helm status loki -n logs
```

查看 Pod：

```bash
kubectl get pods -n logs -l app.kubernetes.io/instance=loki
```

当前集群中的实际 Pod 示例：

```text
loki-chunks-cache-0                     2/2     Running
loki-compactor-0                        1/1     Running
loki-distributor-...                    1/1     Running
loki-gateway-...                        1/1     Running
loki-index-gateway-0                    1/1     Running
loki-index-gateway-1                    1/1     Running
loki-ingester-0                         1/1     Running
loki-ingester-1                         1/1     Running
loki-ingester-2                         1/1     Running
loki-querier-...                        1/1     Running
loki-query-frontend-...                 1/1     Running
loki-query-scheduler-...                1/1     Running
loki-results-cache-0                    2/2     Running
```

查看 Service：

```bash
kubectl get svc -n logs -l app.kubernetes.io/instance=loki
```

当前集群中 gateway 地址是：

```text
loki-gateway.logs
```

Helm notes 中也提示，集群内写入地址是：

```text
http://loki-gateway.logs/loki/api/v1/push
```

Grafana 配置 Loki 数据源时，可以使用：

```text
http://loki-gateway.logs/
```

### 7.5 写入和查询测试

可以先通过 port-forward 暴露 gateway：

```bash
kubectl port-forward --namespace logs svc/loki-gateway 3100:80
```

写入一条测试日志：

```bash
curl -H "Content-Type: application/json" \
  -XPOST "http://127.0.0.1:3100/loki/api/v1/push" \
  --data-raw "{\"streams\": [{\"stream\": {\"job\": \"test\"}, \"values\": [[\"$(date +%s)000000000\", \"fizzbuzz\"]]}]}"
```

查询验证：

```bash
curl "http://127.0.0.1:3100/loki/api/v1/query_range" \
  --data-urlencode 'query={job="test"}'
```

如果返回结果中能看到 `fizzbuzz`，说明 gateway、distributor、ingester、query-frontend、querier 等链路可以正常工作。

## 8. 生产环境观测数据

下面是一组生产环境中的实际观测数据。该环境使用的是 Loki 微服务模式，Loki 版本为 `2.9.4`。这些数据可以作为选择部署模式和评估资源消耗时的参考，但不是 Loki 官方承诺的固定性能指标。

| 维度 | 生产观测值 |
| --- | --- |
| 部署模式 | 微服务模式 |
| Loki 版本 | `2.9.4` |
| 每天日志行数 | 约 `40-60亿行/day` |
| 每天原始日志大小 | 约 `1TB/day` |
| 写入高峰速度 | 约 `17w-22w 行/s` |
| 高峰持续时间 | 约 `1小时20分钟` |
| 短时异常突发 | 曾出现约 `44w 行/s`，持续约 `20分钟` |
| 写入延迟 | `30w 行/s` 基本无延迟；`44w 行/s` 时约 `1-3s` |
| Loki 所有组件总内存 | 约 `13-27GB` 之间浮动 |

这组数据和前面的估算口径基本一致：

```text
1TB 原始日志 ≈ 1024GB 原始日志
1024GB * 500w行/GB ≈ 51.2亿行日志
```

也就是说，生产中每天 `40-60亿行` 日志，对应的原始日志量大致就是 `1TB/day` 左右。

如果再按 `70行日志 ≈ 1次外部请求` 均摊估算：

```text
40亿行 / 70 ≈ 5714w 次请求/day
60亿行 / 70 ≈ 8571w 次请求/day
```

这个规模大致可以理解为每天承载 `5700-8600w` 次外部请求产生的日志量。高峰期 `17w-22w 行/s`，换算成请求量约为：

```text
17w 行/s / 70 ≈ 2428 次请求/s
22w 行/s / 70 ≈ 3142 次请求/s
```

生产中也出现过一次短时异常突发：某些服务模块出现 bug，多个副本持续打印异常堆栈，导致日志写入速度短时间冲到约 `44w 行/s`，持续约 `20分钟`。这类流量不是正常业务请求带来的日志增长，而是服务异常导致的日志放大。从当时观测看，Loki 整体内存基本稳定，没有出现明显失控增长。

从写入延迟看，日志采集并写入 Loki 的链路在 `30w 行/s` 左右基本没有明显延迟；在 `44w 行/s` 的异常突发下，写入延迟大约为 `1-3s`。这说明当前生产配置在 `30w 行/s` 附近还有比较充足的吞吐余量，到了 `44w 行/s` 这种异常放大的短时场景时，链路开始出现轻微堆积，但仍然保持在秒级延迟。

这说明在 `1TB/day` 原始日志、常规峰值 `17w-22w 行/s`，以及短时异常突发 `44w 行/s` 的生产压力下，Loki 微服务模式可以通过组件拆分、对象存储和缓存支撑较大的写入流量。这里的 `13-27GB` 是 Loki 所有组件合计内存观测值，不是单个 Pod 的内存，也不是固定资源需求。实际资源还会受到 label 基数、日志行大小、查询频率、缓存命中率、保留周期和限流配置影响。

## 9. 小结

Loki 三种部署模式可以按规模和复杂度理解：

- 单体模式最简单，适合入门和约 `20GB/day` 以内的小规模日志。
- SSD 模式读写分离，可接近 `1TB/day`，但官方已经标记将废弃，不建议作为新系统长期目标。
- 微服务模式最复杂，但也是最适合大规模生产的模式，可以对写入、查询、索引、压缩等组件分别扩容。

对于新生产环境，如果已经明确日志量会持续增长，或者有多团队、多业务线、多租户、高查询并发需求，建议直接选择微服务模式，并使用 Helm 在 Kubernetes 中部署。

参考资料：

- Loki deployment modes：https://grafana.com/docs/loki/latest/get-started/deployment-modes/
- Install microservice Loki with Helm：https://grafana.com/docs/loki/latest/setup/install/helm/install-microservices/
- Loki Helm chart components：https://grafana.com/docs/loki/latest/setup/install/helm/concepts/
- Loki storage configuration：https://grafana.com/docs/loki/latest/setup/install/helm/configure-storage/
