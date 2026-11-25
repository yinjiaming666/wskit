package example

import (
	"encoding/json"

	"github.com/yinjiaming666/wskit"
)

// TextMessage 把示例文本编码为 WebSocket 帧。私聊传入消息 ID，浏览器用它确认收件。
func TextMessage(text, id string) wskit.Message {
	payload := struct {
		Text string `json:"text"`
		ID   string `json:"id,omitempty"`
	}{Text: text, ID: id}
	data, _ := json.Marshal(payload) // 该结构只含字符串，编码不会失败。
	return wskit.Message{
		ID:    id,
		Frame: wskit.Frame{Type: wskit.TextFrame, Data: data},
	}
}
