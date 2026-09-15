# Kaflow

> **世界上最轻量、最小的 Kafka 客户端之一。** 单个可执行文件约 8 MB，启动即用，专注连接、查看和发送 Kafka 消息。

[![CI](https://github.com/peterzhou12345/Kaflow/actions/workflows/ci.yml/badge.svg)](https://github.com/peterzhou12345/Kaflow/actions/workflows/ci.yml) [![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE) [![Platforms](https://img.shields.io/badge/platforms-macOS%20%7C%20Windows%20%7C%20Ubuntu-blue.svg)](操作说明.md)

轻量、本地运行的 Kafka 图形客户端，适用于 macOS、Windows 和 Ubuntu/Linux。Kaflow 的目标是成为世界上最轻、最小、最容易部署的 Kafka 客户端：无需 JVM、Node、Docker 或安装向导，下载后直接运行。

## 开始使用

从 Release 下载对应平台压缩包即可直接使用；从源码仓库克隆后，也可以按下面方式构建：

- macOS：双击 **启动 Kaflow.command**。
- Windows：双击 **启动 Kaflow.bat**，使用 64 位 Windows 版本。
- Ubuntu/Linux：运行 `sh 启动 Kaflow.sh`，或直接运行对应 `dist/kaflow-linux-*`。

也可在终端运行 macOS 版本：

```sh
./dist/kaflow
```

自动打开浏览器。点击「添加连接」，填写名称与 Bootstrap servers，例如 `localhost:9092` 或多个 `broker1:9092,broker2:9092`，选择认证方式后连接。需要连接一个已有 Kafka 集群；工具本身不是 Kafka Broker。

终端窗口保持打开，按 Ctrl+C 退出。默认只监听 `127.0.0.1:17893`，浏览器会话使用每次启动新生成的 token。若端口占用，先关闭原实例，或运行：

```sh
./dist/kaflow -addr 127.0.0.1:17894
./dist/kaflow -no-open
```

地址中的 token 位于 URL fragment，不作为 HTTP 查询参数发送。重启后使用新启动地址打开页面。浏览器保存的非敏感连接配置按端口隔离，因此建议保持默认端口。

## 已实现

- 保存多个连接名称、Broker 地址与认证方式；重新选择连接后输入凭据。
- PLAINTEXT、TLS、SASL/PLAIN、SCRAM-SHA-256、SCRAM-SHA-512；自定义 CA、PEM 客户端证书及私钥（mTLS）。TLS 默认校验证书及主机名，也兼容显式关闭主机名校验的内部 Kafka 证书。
- Topic 列表与搜索，内部 Topic 开关，分区/副本/ISR/Leader。
- 创建 Topic，输入完整名称确认删除。
- 按最早、最近、指定 Offset 读取；全部分区或单分区；50–1000 条限制。
- 文本/JSON 消息详情、Key、Headers、时间戳、精确字符串 Offset、原始 Key/Value Base64，结果内搜索和 JSON 导出。
- 发送文本/JSON、Key、Headers，自动或指定分区，支持 null Value（Tombstone）。收到 Broker 确认才报告发送成功。
- 消费组状态、成员数量、提交位点和分区 Lag；Broker 列表及 Controller。

## 读取语义与边界

- 使用直接分区分配，不加入消费组，不提交业务消费位点。
- 「最近」从**每个分区末尾往前最多 N 个 Offset**开始读取，再受总条数 N 限制；跨分区没有全局顺序，也不保证选出全局最新 N 条。要精确查看一个分区，选择分区后读取。
- 一次读取捕获当时的 end offsets，后续新消息不进入该次快照。结果按消息时间降序展示。指定 Offset 低于 retention 起点时会从当前最早可用位置读起。
- 单次读取最多 8 秒、1000 条、8 MiB 原始 Key/Value/Header 内容；编码后的响应会更大。到达时间或大小限制会显示结果可能不完整。压缩空洞、事务控制记录也可能导致等待超时提示。
- Offset 跨度不等于消息条数。Offset、Lag 经字符串传输，避免 JavaScript 64 位整数精度丢失。
- 支持文本/JSON；Avro、Protobuf、Schema Registry、OAuth、Kerberos、消费位点重置、实时 tail 尚未实现。二进制 Key/Value 可查看或导出 Base64，Headers 目前按 UTF-8 显示。
- 事务消息目前按 Kafka 默认 `read_uncommitted` 读取，可能包含已中止事务的用户记录。
- 单条发送限制为 Key/Value/Header 内容合计 1 MiB；Broker 的 `max.message.bytes` 仍可能更低。网络超时不代表 Broker 一定没有写入，重新发送前可先查看分区。

## 安全与配置

密码、用户名和证书仅保留在当前进程会话；不写配置文件、不上传到第三方。浏览器仅保存名称、地址、TLS/SASL 选择。连接上限为 16，切换连接会关闭旧会话。关闭页面不会自动退出程序，请用 Ctrl+C 结束。

所有 API 要求随机 Bearer token，检查 Host 和 Origin，限制请求体，界面使用 CSP 和文本转义。此程序是单用户本机工具，不要以反向代理方式暴露到公网。

TLS/SASL 对接还需在你的目标认证集群确认；本项目已验证配置校验和客户端构造，真实联调使用 Apache Kafka 3.9.1 PLAINTEXT。

## 从源码构建

Go 1.20 或以上：

```sh
go mod download
sh scripts/build-all.sh
```

输出 `dist/kaflow-darwin-arm64`、`dist/kaflow-darwin-amd64`、`dist/kaflow-windows-amd64.exe`、`dist/kaflow-linux-amd64` 和 `dist/kaflow-linux-arm64`。Go 版本兼容性决定当前锁定的 franz-go 依赖版本，详见 go.mod/go.sum。

构建脚本会把平台执行文件放在 `dist/`。发布时建议把 `Kaflow-macos-arm64.zip`、`Kaflow-macos-amd64.zip`、`Kaflow-windows-amd64.zip`、`Kaflow-ubuntu-amd64.zip` 和 `Kaflow-ubuntu-arm64.zip` 作为 Release assets 上传；每个压缩包应包含对应执行文件、启动脚本、README 和中英文操作说明。

分发时只需把对应平台二进制和启动脚本放在同一目录。本地构建无需安装；macOS 产物没有 Apple Developer ID 签名或公证，通过网络下载的副本可能触发 Gatekeeper，需在系统「隐私与安全性」中允许打开。Ubuntu 需要给启动脚本执行权限：`chmod +x 启动\\ Kaflow.sh`。

## 验证

```sh
go test -race ./...
go vet ./...
# 仅对隔离测试集群运行：会创建唯一 Topic / Group，结束时清理
KAFLOW_TEST_BROKERS=127.0.0.1:19092 go test -race -run TestKafkaIntegration -v .
```

单元/API：Topic 与连接参数、TLS/SASL 校验、Kafka client 构造、null/empty、Headers 大小预算、64 位 Offset、Host/Origin/token、方法与请求体、安全头、内嵌页面。

真实集成：创建 → metadata → 指定分区发送 Unicode/空字符串/Tombstone/Headers → 三种读取模式 → 空分区和尾部单条 → offsets → 提交测试组位点 → Lag 为 3 且浏览不改变提交位点 → 删除确认 → 删除 → 断开。

技术选型：[franz-go](https://github.com/twmb/franz-go) 提供 Kafka 协议实现；Go 标准库提供本地 HTTP 服务，原生 HTML/CSS/JavaScript 提供界面。没有前端构建依赖或遥测 SDK。

## Project Metadata

- Citation: see [`CITATION.cff`](CITATION.cff)
- License: MIT
- Contributions: see [`CONTRIBUTING.md`](CONTRIBUTING.md)
- Continuous integration: GitHub Actions builds/tests all supported targets
