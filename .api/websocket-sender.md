# Go 消息发送

业务服务导入 `github.com/yinjiaming666/wskit`，通过 `wskit.NewSender(backend)` 创建发送器，复用至进程退出并调用 `Close()`。消息正文和投递流水由业务保存。

## 私聊

`sender.Send(ctx, toUserID, msg) (wskit.Delivery, error)` 向该用户的所有在线连接投递。

- `toUserID` 和 `msg.ID` 必填。`msg.Frame` 是业务编码的文本或二进制帧。要让客户端确认消息，业务协议需在帧中携带消息 ID。
- 参数校验、路由查询或登记待确认连接出错时返回 `error`。业务拿到 `nil` 错误后应继续读取 `Delivery`。
- `Delivery.MessageID` 是传入的消息 ID（返回校验或依赖错误时也保留）。`Delivery` 嵌入共享的 `wskit.DeliveryResult`。`AcceptedConnIDs` 列出已入队的连接，`FailedConnIDs` 列出投递失败的连接。部分成功时两个列表都有值，`Accepted` 为 `false`，`Reason` 提供失败原因。用户没有在线连接时 `Accepted=true`，两个列表都为空。节点间传输错误或缺失响应按失败连接汇总到结果，`error` 仍可为 `nil`；它不表示 WebSocket 写入成功或客户端确认。
- `sender.PendingConnections(ctx, messageID)`、`AckedConnections`、`FailedConnections` 均返回 `([]string, error)`，分别查询待回执、已确认、回执跟踪失败的连接。查询失败时返回存储错误；各查询独立读取，不构成原子快照。回执跟踪失败与同步投递结果的 `FailedConnIDs` 含义不同。
- 客户端实际确认由 `Session.Ack(ctx, messageID)` 单独登记，业务可通过 `sender.AckedConnections(ctx, messageID)` 查询。[节点 gRPC 契约](websocket-push-grpc.md)

## 频道订阅

宿主授权后，通过 `Session.Subscribe(ctx, channel)` 或 `Node.Subscribe(ctx, connID, channel)` 添加订阅，通过对应的 `Unsubscribe` 取消订阅。`SessionInfo.Channels` 可指定已授权的初始订阅。

- 频道 ID 长度为 1 至 128 字节，无效时返回 `invalid channel`。每条连接最多订阅 64 个频道，重复订阅同一频道不会占用额外名额。
- 订阅超出上限时返回 `channel limit reached or connection closed`。连接状态、上下文和 Redis 租约操作失败时返回相应错误。成功时返回 `nil`。

## 频道广播

`sender.SendToChannel(ctx, channel, msg) error` 通过 Redis Pub/Sub 通知当前承载频道的节点。

- `channel` 长度为 1 至 128 字节。`msg.Frame.Type` 支持 `TextFrame` 和 `BinaryFrame`，文本数据须为有效 UTF-8。订阅者收到原始帧的类型和字节。
- 频道消息可省略 `msg.ID`。业务填写该字段也不会产生客户端回执要求。
- 返回 `nil` 表示发布流程完成，频道没有在线订阅者时也可返回 `nil`。参数、路由查询或 Redis 发布失败时返回 `error`。频道发送结果的粒度是一次发布。

框架在键名和 Pub/Sub 频道名前加上配置的 `KeyPrefix`。

| 用途 | 名称 |
| --- | --- |
| 频道的节点租约 | `live:channel:<channelID>:nodes` |
| 节点承载的频道索引 | `live:node:<endpointID>:channels` |
| 发往指定节点的频道广播 | `live:node:<endpointID>:channel:<channelID>` |

## 共享投递结果与本地推送

```go
type DeliveryResult struct {
    Accepted        bool
    AcceptedConnIDs []string
    FailedConnIDs   []string
    Reason          string
}

type Delivery struct {
    MessageID string
    DeliveryResult
}
```

`DeliveryResult` 在根包公开，内部节点入队、gRPC 客户端和跨节点聚合共用同一 Go 类型。只有 gRPC 协议边界与 protobuf `PushResponse` 转换。旧 `domain.SendResult` / `domain.PushResponse` 不再公开，子包实现已移入 `internal`。

- `node.PushToConns(ctx, connIDs, frame)` 与 `node.PushToUsers(ctx, userIDs, frame)` 返回 `(*wskit.DeliveryResult, error)`，仅向该节点本地连接入队，不附加消息 ID。宿主负责目标授权。
- 本地推送复制调用方的帧数据，保留文本/二进制类型。上下文已取消、数据帧无效时返回错误；连接不存在或队列无法接受时计入 `FailedConnIDs`。用户在该节点没有连接时为空目标，结果仍为接受成功。
- 直接读取 `delivery.Accepted` 等字段保持不变；构造字面量时需显式填写嵌入字段 `DeliveryResult`。JSON 编码仍将这些字段展平。
- 原 `backend.Acks()` 删除，客户端回执和超时语义不变，业务使用根包 Sender 的查询和超时扫描能力。
