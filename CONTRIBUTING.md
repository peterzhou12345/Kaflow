# Contributing to Kaflow

感谢你为 Kaflow 提交问题、建议或代码。Kaflow 的目标是保持体积小、启动快、界面直接、连接行为可解释。

## 报告问题

请尽量提供：操作系统和架构、Kaflow 版本、Kafka 版本、连接协议（不要粘贴密码或私钥）、复现步骤、完整错误文本以及预期行为。涉及真实集群时，请先删除 Broker 地址、用户名、证书和消息内容中的敏感信息。

## 提交代码

```sh
go test -race ./...
go vet ./...
go build ./...
```

前端是内嵌的原生 HTML/CSS/JavaScript，不需要 Node 构建。涉及 UI 的改动请同时验证桌面和窄屏布局；涉及 Kafka 行为的改动请增加 Go 测试，真实 Broker 测试使用 `KAFLOW_TEST_BROKERS`。

提交信息建议使用 `feat:`, `fix:`, `docs:`, `test:`, `build:` 前缀。请保持改动聚焦，并在 Pull Request 中说明验证命令和兼容性影响。

## 安全问题

不要在公开 Issue 中披露密码、Token、私钥、完整证书链或生产消息。请通过仓库管理员的私下渠道报告安全问题。

## English

Please include the OS/architecture, Kaflow and Kafka versions, protocol (never credentials or private keys), reproduction steps, full error text, and expected behavior. Run `go test -race ./...`, `go vet ./...`, and `go build ./...` before opening a pull request. Keep changes focused and describe validation and compatibility impact.
