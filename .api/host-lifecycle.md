# Go 库生命周期与示例健康检查

## 摘要

- 状态：active
- 库入口：`backend.go`、`node.go`、`sender.go`
- 示例宿主：`example/node/main.go`、`example/node/health.go`、`example/gateway/main.go`、`example/internal/host/server.go`
- 库不启动 HTTP 服务、不安装信号处理、不注册 `/healthz` / `/readyz`，不规定健康检查响应格式。

## Go 调用

业务项目导入 `github.com/yinjiaming666/wskit`，通过 Go 包名 `socketserver` 调用库接口。

| 接口 | 语义 |
|---|---|
| `node.Run(ctx) error` | 每个 Node 只能运行一次；启动 gRPC 和 Redis 订阅后注册节点，阻塞至取消或故障，清理连接、路由、租约、监听和后台工作后返回。 |
| `node.Ready() bool` | 节点运行且可以接收新连接时为 true；启动前、排空中、停止后为 false。不执行 Redis Ping。 |
| `node.ConnCount() int64` | 本地连接数。 |
| `backend.Ready(ctx) error` | 检查 Redis 连通性；未初始化或 Redis 不可用时返回错误。 |
| `node.BeginDrain(ctx) error` | 停止新连接准入与分配，已有连接继续服务；未运行或依赖操作失败时返回错误。 |
| `node.GRPCFailure() <-chan struct{}` / `GRPCFailureCause() error` | gRPC 监听意外失败的通知和原因；正常停止不报告故障。 |
| `sender.Close()` / `backend.Close()` | 分别释放发送端 gRPC 连接和共享 Redis 池；宿主应等 Node 和使用这些资源的工作结束后关闭。 |

宿主负责鉴权与 HTTP 路由，使用 `signal.NotifyContext` 或自己的生命周期框架。示例在 Node 或 HTTP 退出时取消共同上下文，等待二者结束后释放 Sender / Backend。HTTP Shutdown 最多等待 10 秒后强制关闭，已升级的 WebSocket 连接由 Node 清理。无需新增库级 HTTP 辅助接口。

## 示例 Node 的 HTTP 探针（宿主约定）

- 方法：`GET`；其他方法返回 HTTP 405。
- 路径：`/healthz`、`/readyz`，默认地址 `http://127.0.0.1:8081`。
- 鉴权：示例不加鉴权；生产宿主自行决定探针访问控制。
- 请求：无 body、query 或额外 header 要求。
- Content-Type：`application/json; charset=utf-8`。

`/healthz` 在宿主运行时返回 HTTP 200，包括节点排空和 Redis 不可用时：

```json
{"code":0,"message":"ok","data":{"status":"ok","ready":false,"conn_count":0}}
```

其中 `ready` 反映 `Node.Ready()`，不包含 Redis 检查。`/readyz` 需 `Node.Ready()` 为 true 且 `Backend.Ready(ctx)` 成功才返回 HTTP 200：

```json
{"code":0,"message":"ok","data":{"status":"ready","ready":true,"conn_count":7}}
```

未就绪或依赖失败时返回 HTTP 503，响应不包含 `data`：

```json
{"code":503,"message":"node is not ready"}
```

Redis 检查失败时 `message` 为 `websocket dependency unavailable`。不支持的方法返回 `{"code":405,"message":"method not allowed"}`。这些路径和格式可以由业务宿主替换，不属于库的固定 HTTP API。
