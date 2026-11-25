# WebSocket 示例

`example/` 是一个独立的 Go 模块，演示如何在应用中启动 Gateway 和 Node，以及通过浏览器连接 WebSocket、加入或退出频道、收发私聊和频道消息。模块也提供命令行示例，演示如何从服务端主动推送消息。

## 用法

1. 启动 Redis，默认地址为 `127.0.0.1:6379`。使用其他地址时，在各终端设置相同的 `REDIS_ADDR`。
2. 在 `example/` 目录下开两个终端，分别运行 `go run ./node` 和 `go run ./gateway`。
3. 分别打开 [Alice](http://127.0.0.1:8080/?user=alice) 和 [Bob](http://127.0.0.1:8080/?user=bob) 的页面，点击 **连接**。Alice 可以向 `bob` 发送私聊，或向两人默认加入的 `lobby` 频道发消息。
4. 保持页面在线，在第三个终端进入 `example/` 并执行以下命令，分别向 Bob 和 `lobby` 推送消息。

   ```bash
   go run ./push-user -user bob -text '你好'
   go run ./push-channel -channel lobby -text '大家好'
   ```
