# API：WebSocket Node 内部推送 gRPC

## 摘要
- 用途：发送器通过 gRPC 将消息投递到目标 WebSocket Node。
- 后端文件：`api/proto/websocket.proto`、`internal/grpcpush/protocol.go`、`grpc_server.go`
- 状态：active

## 调用方式
- 方法/协议：gRPC
- Go protobuf 包路径为 `github.com/yinjiaming666/wskit/api/proto/websocket`。
- Procedure：`websocket.v1.WebSocketPushService/PushToConns`、`PushToUsers`、`Health`
- 鉴权：当前实现使用内网/防火墙边界，gRPC 本身未启用 mTLS 或 token。
- Content-Type：`application/grpc`

## 请求
```proto
message PushMessage {
  string message_id = 1;
  bytes payload = 4;
  uint32 frame_type = 11;
  reserved 2, 3, 5 to 10;
}

message PushToConnsRequest {
  repeated string conn_ids = 1;
  PushMessage message = 2;
}

message PushToUsersRequest {
  repeated string user_ids = 1;
  PushMessage message = 2;
}

message HealthRequest {
  string endpoint_id = 1;
}
```

## 成功响应
```proto
message PushResponse {
  bool accepted = 1;
  repeated string accepted_conn_ids = 2;
  repeated string failed_conn_ids = 3;
  string reason = 4;
}

message HealthResponse {
  string endpoint_id = 1;
  string status = 2;
  int64 conn_count = 3;
}
```

## 错误响应
gRPC 调用错误直接返回为 RPC error；业务失败通过 `PushResponse.accepted=false`、`failed_conn_ids` 和 `reason` 表达。

## 备注
- `payload` 是业务编码的原始字节，`frame_type` 指定文本或二进制帧。目标连接或用户由外层请求指定。
- Node 接受推送后将数据帧写入本地连接发送队列，由连接异步写入 WebSocket。
- Go 内部统一使用 `DeliveryResult`，protobuf `PushResponse` 保留在协议边界。
