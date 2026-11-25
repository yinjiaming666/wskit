# wskit

`wskit` 是可嵌入 Go 项目的分布式 WebSocket 框架。使用 `CreateGateway` 分配连接节点，`CreateNode` 管理长连接和频道订阅，`NewSender` 通过 Redis 广播或 gRPC 向指定用户投递。

## 作为 Go 库接入

框架要求 Go 1.25.0 或更高版本，并使用 Redis。在业务项目的模块目录安装根包。

```bash
go get github.com/yinjiaming666/wskit@latest
```

```go
import "github.com/yinjiaming666/wskit"
```

Go 包名是 `socketserver`。本地联调可以在业务项目的 `go.mod` 中使用 `replace` 指向框架目录。[示例模块](example/go.mod)

每个进程调用 `socketserver.NewRedisBackend(socketserver.RedisConfig{Addr: redisAddr, KeyPrefix: "myapp:"})`，同一进程内的 Node、Gateway 和 Sender 可以共用这个实例。调用 `backend.Ready(ctx)` 可验证 Redis 连接。一个项目的各个进程使用同一 Redis 数据库和前缀。现有集群升级时，发送服务与 WebSocket 节点需使用兼容的 gRPC 和 Redis 消息格式，建议统一版本。多个项目共用 Redis 时各设唯一的 `KeyPrefix`，因为 Pub/Sub 频道跨逻辑数据库可见。每个运行中的 Node 使用唯一的 `EndpointID`，并将 `GRPCAddr` 配置为发送方能访问的监听地址。

库负责 WebSocket 握手、连接生命周期、节点路由、频道扇出和逐连接回执。业务项目负责鉴权、订阅权限、应用消息编码与解析。`socketserver.Frame` 带有 `Type` 和 `Data`，支持 UTF-8 文本和任意二进制数据。WebSocket 协议级 Ping/Pong 由节点处理。

下面的 Node 把收到的数据帧原样返回，浏览器连接时默认订阅 `lobby`。省略 `ConnID` 时，框架会为每次连接生成唯一 ID。[加入、退出和推送示例](example/README.md)

```go
func runNode(ctx context.Context, mux *http.ServeMux, backend *socketserver.Backend) error {
    node, err := socketserver.CreateNode(socketserver.NodeConfig{
        EndpointID: "node-a",
        GRPCAddr:   "127.0.0.1:9001",
        OnMessage: func(ctx context.Context, s *socketserver.Session, frame socketserver.Frame) error {
            return s.Send(frame)
        },
    }, backend)
    if err != nil {
        return err
    }
    mux.HandleFunc("/ws/node-a", func(w http.ResponseWriter, r *http.Request) {
        node.ServeWS(w, r, socketserver.SessionInfo{
            UserID: r.URL.Query().Get("user"), Channels: []string{"lobby"},
        })
    })
    return node.Run(ctx)
}
```

宿主启动 HTTP 服务并处理 `node.Run(ctx)` 的返回值。节点在 gRPC 监听和 Redis 订阅就绪后注册。`node.BeginDrain(ctx)` 让已有连接继续收消息，同时停止新连接分配。取消运行上下文后，等待 `Run` 返回再关闭 Backend。`OnMessage` 返回错误会结束该连接，业务可以调用 `Session.Send` 回复自定义错误并返回 `nil`。

| 业务操作 | Go 接口 |
|---|---|
| 分配连接节点 | `socketserver.CreateGateway(socketserver.GatewayConfig{PublicURL: "wss://ws.example.com"}, backend)` 创建网关。`gateway.Route(ctx, userID)` 返回节点和 WebSocket 地址。 |
| 向频道在线订阅者广播 | `sender.SendToChannel(ctx, "group:42", msg)` 通过 Redis Pub/Sub 发布，返回发布过程的错误。客户端按原有格式接收 `msg.Frame`，无需回执。 |
| 一对一私聊 | `sender.Send(ctx, toUserID, msg)` 通过 gRPC 投递到收件人的所有在线设备，结果列出已入队和失败的连接。消息 ID 由业务生成，包含在应用协议中。客户端回执由业务解析后调用 `Session.Ack(ctx, id)`。 |

业务负责消息存储。聊天记录可以在发送前保存，投递流水可以在发送后按同步结果记录。私聊用 `Delivery.AcceptedConnIDs` 和 `FailedConnIDs` 区分入队情况，频道广播用返回的 `error` 判断发布结果。[Go 发送接口契约](.api/websocket-sender.md)

已有服务请将 `NewSenderWithStore` 改为 `NewSender`，将 `PublishStream` 调用改为 `SendToChannel`。

业务创建一个 Sender 复用到进程退出。发送文本时指定 `socketserver.TextFrame`，发送 Protobuf 等序列化结果时指定 `socketserver.BinaryFrame`。

```go
sender, err := socketserver.NewSender(backend)
if err != nil {
    return err
}
defer sender.Close()

msg := socketserver.Message{
    ID: "m42",
    Frame: socketserver.Frame{Type: socketserver.TextFrame, Data: []byte(`{"id":"m42","text":"你好"}`)},
}
result, err := sender.Send(ctx, "alice", msg)
```

消息 ID 用于库内跟踪投递，客户端看到的字段由 `Frame.Data` 决定。发送服务可以运行 `sender.RunAckTimeouts(ctx)` 扫描超时回执，退出时调用 `sender.Close()`。

### 公开 API 与迁移

业务仅需导入根包 `socketserver`。仓储、Redis 封装、节点间传输及其适配器位于 `internal/`，不能作为独立公共组件使用。`Frame`、`FrameType`、`RedisConfig` 和 `DeliveryResult` 在根包提供稳定名称；内部共用类型定义，不反向依赖根包。

| 原入口 | 迁移方式 |
|---|---|
| `domain.SendResult`、`domain.PushResponse` | 统一使用 `socketserver.DeliveryResult`。`Node.PushToConns`、`Node.PushToUsers` 返回 `*DeliveryResult`。protobuf 的 `PushResponse` 仅用于 gRPC 边界。 |
| `Delivery` 中重复的结果字段 | 改为嵌入 `DeliveryResult`，保留 `MessageID`。读取 `result.Accepted` 等字段的代码不变；结构体字面量改为 `Delivery{MessageID: id, DeliveryResult: socketserver.DeliveryResult{Accepted: true}}`。 |
| `backend.Endpoints()`、`Routes()`、`Channels()` | 删除仓储访问器。分配节点用 `Gateway.Route`，投递用 `Sender.Send` / `SendToChannel`，本地订阅用 `Node` / `Session` 的 `Subscribe`、`Unsubscribe`。节点注册和路由租约由 `Node.Run` 维护，不提供原始仓储读写替代接口。 |
| `backend.Acks()` | 查询用 `Sender.PendingConnections`、`AckedConnections`、`FailedConnections`；登记客户端回执用 `Session.Ack` / `Node.Ack`，超时扫描用 `Sender.RunAckTimeouts`。不公开底层状态写入接口。 |
| `bus`、`push`、`sender`、`redisclient` 等子包 | 改用根包 `NewRedisBackend`、`NewSender` 和 `CreateNode`；删除无调用的传输包装层与 `message.EnsureMessageID`，消息 ID 由业务生成。 |
| `NewClient`、`NewRoomHub` | 本地连接和频道订阅映射改为私有实现，业务通过 `Session` / `Node` 操作。 |
| `httpx.ServeHTTP` / `ServeHTTPContext`、`HTTPServerConfig` | 宿主创建 `http.Server` 并管理信号和优雅退出，参见示例。 |
| `Node.RegisterHealthRoutes`、`HealthResponse` | 宿主注册路由并定义响应，结合 `Node.Ready()`、`Node.ConnCount()` 和 `Backend.Ready(ctx)` 查询状态。 |

`DeliveryResult.Accepted` 表示所有已解析目标连接均入队成功；无在线连接时仍为 `true`，两组连接列表为空。它不表示 WebSocket 写入完成或客户端确认。`FailedConnections` 查询回执跟踪的失败状态，与同步结果中的 `FailedConnIDs`（入队失败）分开使用。

### 宿主生命周期

库不启动 HTTP 服务、不注册固定健康路由，也不监听进程信号。宿主用 `signal.NotifyContext` 协调 HTTP 服务与 `Node.Run`：任一运行失败就取消共同上下文，等待 HTTP 关闭和 `Node.Run` 清理完成后，再关闭 Sender 和 Backend。HTTP 关闭保留示例原有的 10 秒宽限期；WebSocket 连接、节点注册、连接路由和频道租约仍由 `Node.Run` 清理。

`Node.Ready()` 表示节点正在运行且可接收新连接；启动前、排空中和停止后返回 `false`。宿主可另外调用 `Backend.Ready(ctx)` 检查 Redis 连通性，使用 `Node.ConnCount()` 获取本地连接数。示例选择 `/healthz` 作为存活探针、`/readyz` 作为就绪探针，路径、HTTP 状态码和 JSON 均属于示例宿主。[生命周期与健康检查契约](.api/host-lifecycle.md)

## 可运行的示例

独立模块 `example/` 演示 Gateway 和 Node 启动、HTML + JavaScript 客户端连接与断开、加入和退出频道，以及客户端发消息和服务端主动推送。可以连接 Alice 和 Bob，验证一对一私聊和频道广播的接收范围。[示例说明](example/README.md)

## 测试

所有测试文件都在 `test/`。框架内部测试和示例节点的 HTTP 健康检查测试通过 Go overlay 在各自的包内执行。在仓库根目录运行测试，`-count=1` 确保修改示例代码后重新执行测试。设置 `WS_TEST_REDIS_ADDR` 会额外运行双节点 Redis/gRPC 集成测试。

```bash
go test -count=1 ./...
WS_TEST_REDIS_ADDR=127.0.0.1:6379 go test -count=1 -race ./...
go -C example build ./...
```
