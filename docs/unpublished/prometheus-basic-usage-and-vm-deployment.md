# Prometheus 入门：从 VM 部署到 Java 应用指标采集，再到 Grafana 展示

在可观测性体系里，日志、指标、链路追踪、持续剖析各自解决不同问题。日志适合看具体事件和异常堆栈，链路追踪适合看一次请求经过了哪些服务，而 Prometheus 主要解决的是 metrics，也就是指标监控。

指标监控关注的是系统状态和趋势，比如：服务是否存活、接口 QPS、接口耗时、错误率、CPU、内存、JVM GC、线程数、连接池、数据库响应时间等。

这篇文章先从最基础的链路开始：

```text
Java 应用暴露 /actuator/prometheus -> Prometheus 定时抓取 -> Grafana 查询和展示
```

目标不是一次讲完 Prometheus 的所有高级能力，而是先把“服务怎么暴露指标、Prometheus 怎么采集、Grafana 怎么查看”这个最小闭环跑通。

## 1. Prometheus 是什么

Prometheus 是一个开源监控和告警系统，最核心的能力包括：

1. 按固定时间间隔主动抓取指标。
2. 使用时间序列存储指标数据。
3. 使用 PromQL 查询和计算指标。
4. 支持通过 Alertmanager 做告警。
5. 可以和 Grafana 结合做 Dashboard 展示。

Prometheus 采用的是 pull 模型。也就是说，不是应用主动把指标推给 Prometheus，而是应用先暴露一个 HTTP 接口，Prometheus 再按配置定时去抓。

例如一个 Java 服务暴露：

```text
http://<app-ip>:8080/actuator/prometheus
```

Prometheus 配置好这个地址之后，就会周期性访问这个接口，把里面的指标采集回来。

## 2. Prometheus 的基本链路

一个最小 Prometheus 监控链路通常包含三部分：

| 组件 | 作用 |
| --- | --- |
| 应用程序 | 暴露 metrics 接口，例如 `/actuator/prometheus` |
| Prometheus | 定时抓取 metrics，存储时间序列数据，提供 PromQL 查询 |
| Grafana | 连接 Prometheus 数据源，展示图表和 Dashboard |

链路可以理解为：

```text
应用产生指标 -> 暴露 HTTP metrics 接口 -> Prometheus scrape -> PromQL 查询 -> Grafana 展示
```

这里有一个关键点：Prometheus 本身不是业务埋点框架。它负责采集、存储和查询指标。业务服务必须先把指标暴露出来，Prometheus 才能采集到。

## 3. 在虚拟机上部署 Prometheus

下面以 Linux VM 为例，使用官方二进制包部署 Prometheus。

### 3.1 下载 Prometheus

进入部署目录：

```bash
mkdir -p /opt/observability
cd /opt/observability
```

从 Prometheus 官方下载页面选择 Linux amd64 版本：

```text
https://prometheus.io/download/
```

示例下载命令如下，版本号可以按官方下载页替换：

```bash
wget https://github.com/prometheus/prometheus/releases/download/v3.7.3/prometheus-3.7.3.linux-amd64.tar.gz
```

解压：

```bash
tar -zxvf prometheus-3.7.3.linux-amd64.tar.gz
cd prometheus-3.7.3.linux-amd64
```

目录里常见文件包括：

```text
prometheus                 # Prometheus 主程序
promtool                   # 配置检查和规则检查工具
prometheus.yml             # 默认配置文件
consoles/                  # 控制台模板
console_libraries/         # 控制台模板依赖
```

### 3.2 编写 Prometheus 配置

先使用一个最小配置，采集 Prometheus 自己：

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: prometheus
    static_configs:
      - targets:
          - localhost:9090
```

保存为：

```text
/opt/observability/prometheus-3.7.3.linux-amd64/prometheus.yml
```

配置含义：

1. `scrape_interval: 15s` 表示每 15 秒抓取一次指标。
2. `evaluation_interval: 15s` 表示每 15 秒计算一次告警规则。
3. `scrape_configs` 表示抓取目标列表。
4. `job_name` 是这一类抓取任务的名称。
5. `targets` 是具体的抓取地址。

### 3.3 启动 Prometheus

直接前台启动：

```bash
./prometheus \
  --config.file=./prometheus.yml \
  --storage.tsdb.path=./data \
  --web.listen-address=0.0.0.0:9090
```

启动后访问：

```text
http://<vm-ip>:9090
```

如果能打开 Prometheus 页面，说明服务已经启动。

### 3.4 检查 Prometheus 是否采集成功

进入 Prometheus 页面后，可以打开：

```text
Status -> Targets
```

如果看到 `prometheus` 这个 target 状态是 `UP`，说明 Prometheus 已经成功采集自己。

也可以在 Prometheus 查询框里输入：

```promql
up
```

如果返回：

```text
up{job="prometheus", instance="localhost:9090"} 1
```

说明采集正常。`up` 是 Prometheus 自动生成的指标，值为 `1` 表示抓取成功，值为 `0` 表示抓取失败。

## 4. Java 应用如何暴露 Prometheus 指标

Prometheus 能采集指标的前提是：应用要先暴露 metrics 接口。

在 Spring Boot 应用里，最常见的方式是引入 Spring Boot Actuator 和 Micrometer Prometheus registry。

### 4.1 Maven 依赖

示例依赖：

```xml
<dependencies>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-actuator</artifactId>
    </dependency>

    <dependency>
        <groupId>io.micrometer</groupId>
        <artifactId>micrometer-registry-prometheus</artifactId>
    </dependency>
</dependencies>
```

`spring-boot-starter-actuator` 负责提供应用健康检查、运行时信息和 metrics 能力。

`micrometer-registry-prometheus` 负责把 Micrometer 指标转换成 Prometheus 能识别的文本格式。

### 4.2 application.yml 配置

示例配置：

```yaml
server:
  port: 8080

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
  endpoint:
    health:
      show-details: always
  metrics:
    tags:
      application: demo-service
```

配置说明：

1. `management.endpoints.web.exposure.include` 控制哪些 actuator endpoint 可以通过 HTTP 访问。
2. `prometheus` 表示开启 Prometheus 指标接口。
3. `management.metrics.tags.application` 给指标统一加上应用名称标签，后续在 PromQL 和 Grafana 里更方便区分服务。

应用启动后访问：

```text
http://<app-ip>:8080/actuator/prometheus
```

如果能看到类似下面的内容，就说明应用已经暴露了 Prometheus 指标：

```text
# HELP jvm_memory_used_bytes The amount of used memory
# TYPE jvm_memory_used_bytes gauge
jvm_memory_used_bytes{application="demo-service",area="heap",id="G1 Eden Space"} 1.2345678E8

# HELP http_server_requests_seconds
# TYPE http_server_requests_seconds summary
http_server_requests_seconds_count{application="demo-service",method="GET",status="200",uri="/api/users"} 1024
```

这些指标里会包含 JVM、HTTP、线程、GC、进程等基础运行时数据。

## 5. 让 Prometheus 采集 Java 应用

应用暴露指标之后，还需要把应用地址加入 Prometheus 配置。

修改 `prometheus.yml`：

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: prometheus
    static_configs:
      - targets:
          - localhost:9090

  - job_name: demo-java-app
    metrics_path: /actuator/prometheus
    static_configs:
      - targets:
          - 192.168.1.10:8080
        labels:
          env: test
          app: demo-service
```

这里新增了一个 `demo-java-app` 抓取任务：

1. `metrics_path` 指定指标路径是 `/actuator/prometheus`。
2. `targets` 指定 Java 应用地址。
3. `labels` 可以给这个 target 补充环境、应用等标签。

修改配置后，重启 Prometheus，或者发送 reload 信号。如果启动时开启了 `--web.enable-lifecycle`，可以通过 HTTP reload：

```bash
curl -X POST http://localhost:9090/-/reload
```

为了简单起见，本地验证时也可以直接重启 Prometheus。

然后再次进入：

```text
Status -> Targets
```

如果 `demo-java-app` 状态为 `UP`，说明 Prometheus 已经成功采集 Java 应用。

## 6. 常用 PromQL 查询

采集成功后，可以在 Prometheus 页面直接查询指标。

### 6.1 查看服务是否在线

```promql
up{job="demo-java-app"}
```

值为 `1` 表示采集成功，值为 `0` 表示采集失败。

### 6.2 查看接口请求速率

Spring Boot Actuator 暴露的 HTTP 指标通常是 `http_server_requests_seconds_count`。

查询最近 5 分钟的请求速率：

```promql
rate(http_server_requests_seconds_count{job="demo-java-app"}[5m])
```

如果想按接口维度聚合：

```promql
sum by (uri, method, status) (
  rate(http_server_requests_seconds_count{job="demo-java-app"}[5m])
)
```

### 6.3 查看 JVM 内存使用

```promql
jvm_memory_used_bytes{job="demo-java-app"}
```

按 heap / nonheap 聚合：

```promql
sum by (area) (jvm_memory_used_bytes{job="demo-java-app"})
```

### 6.4 查看 GC 次数

```promql
rate(jvm_gc_pause_seconds_count{job="demo-java-app"}[5m])
```

### 6.5 查看接口耗时

如果应用暴露了 histogram 指标，可以使用 `histogram_quantile` 计算 P95：

```promql
histogram_quantile(
  0.95,
  sum by (le, uri, method) (
    rate(http_server_requests_seconds_bucket{job="demo-java-app"}[5m])
  )
)
```

如果当前只有 summary 指标，没有 bucket，就需要根据实际暴露的指标名称调整查询方式。

## 7. Grafana 如何接入 Prometheus

Prometheus 自带页面适合验证和临时查询，但日常使用通常会接入 Grafana。

### 7.1 部署 Grafana

如果还没有 Grafana，可以和 Prometheus 一样使用 Linux 二进制包部署。

进入部署目录：

```bash
cd /opt/observability
```

从 Grafana 下载页面选择 Linux 版本：

```text
https://grafana.com/grafana/download/
```

下载并解压后启动：

```bash
cd /opt/observability/grafana-<version>
./bin/grafana server
```

默认访问地址：

```text
http://<vm-ip>:3000
```

默认账号密码：

```text
admin / admin
```

首次登录后按提示修改密码。

### 7.2 添加 Prometheus 数据源

进入 Grafana 后：

```text
Connections -> Data sources -> Add data source -> Prometheus
```

URL 填写：

```text
http://<prometheus-ip>:9090
```

如果 Grafana 和 Prometheus 在同一台 VM 上，也可以填写：

```text
http://localhost:9090
```

保存后点击 `Save & test`。如果提示成功，说明 Grafana 已经可以访问 Prometheus。

### 7.3 在 Grafana 里查询指标

进入：

```text
Explore -> 选择 Prometheus 数据源
```

输入：

```promql
up{job="demo-java-app"}
```

如果能看到数据，说明 Grafana、Prometheus、Java 应用这条链路已经打通。

### 7.4 做一个简单 Dashboard

可以先做几个最基础的面板：

| 面板 | PromQL |
| --- | --- |
| 服务存活 | `up{job="demo-java-app"}` |
| 请求速率 | `sum(rate(http_server_requests_seconds_count{job="demo-java-app"}[5m]))` |
| JVM Heap 使用 | `sum(jvm_memory_used_bytes{job="demo-java-app",area="heap"})` |
| GC 频率 | `sum(rate(jvm_gc_pause_seconds_count{job="demo-java-app"}[5m]))` |
| P95 耗时 | `histogram_quantile(0.95, sum by (le) (rate(http_server_requests_seconds_bucket{job="demo-java-app"}[5m])))` |

刚开始不需要追求 Dashboard 很复杂。先把服务是否在线、请求量、错误率、耗时、JVM 内存、GC 这些基础指标看清楚，就已经能覆盖很多日常排查场景。

## 8. 常见问题

### 8.1 Targets 页面显示 DOWN

优先检查：

1. Prometheus 机器能否访问应用端口。
2. `metrics_path` 是否写对。
3. 应用是否暴露了 `/actuator/prometheus`。
4. 防火墙、安全组、Nginx 或网关是否拦截。
5. 应用是否需要鉴权，如果需要，要在 Prometheus 里配置认证。

可以在 Prometheus 机器上直接执行：

```bash
curl http://<app-ip>:8080/actuator/prometheus
```

如果 curl 都访问不了，Prometheus 也采集不到。

### 8.2 `/actuator/prometheus` 访问不到

检查 Java 应用：

1. 是否引入 `spring-boot-starter-actuator`。
2. 是否引入 `micrometer-registry-prometheus`。
3. `management.endpoints.web.exposure.include` 是否包含 `prometheus`。
4. Actuator endpoint 是否被安全配置拦截。
5. Spring Boot 版本和配置项是否匹配。

### 8.3 指标太多怎么办

Prometheus 的指标不是越多越好，尤其要注意 label 的数量。

不要把下面这些字段作为 label：

1. `request_id`
2. `trace_id`
3. 手机号
4. 用户 ID
5. 订单号
6. 完整 URL 参数

这些字段基数太高，会导致时间序列数量暴涨，增加 Prometheus 内存、磁盘和查询压力。

比较适合做 label 的字段是：

1. 应用名
2. 环境
3. 接口模板，例如 `/api/users/{id}`
4. HTTP method
5. HTTP status
6. 实例地址

## 9. 总结

Prometheus 的入门链路并不复杂，关键是理解它的工作方式：

```text
应用暴露指标，Prometheus 定时抓取，Grafana 负责展示。
```

对于 Java Spring Boot 应用来说，接入成本很低：引入 `spring-boot-starter-actuator` 和 `micrometer-registry-prometheus`，打开 `/actuator/prometheus`，再把地址配置到 Prometheus 里，就能采集到 JVM、HTTP、GC、线程等基础指标。

后续真正需要深入的是：指标如何设计、PromQL 如何写、告警规则如何制定、Prometheus 如何高可用、Kubernetes 环境如何自动发现服务。这些内容可以继续拆成后续几篇文章单独展开。
