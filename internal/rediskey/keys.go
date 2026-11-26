package rediskey

import (
	"fmt"
	"strings"
)

var globEscaper = strings.NewReplacer(`\`, `\\`, "*", `\*`, "?", `\?`, "[", `\[`, "]", `\]`)

// EscapePatternLiteral preserves a literal namespace or node ID in a Redis glob.
func EscapePatternLiteral(value string) string { return globEscaper.Replace(value) }

// EndpointsKey 返回 WebSocket endpoint 列表 Key。
func EndpointsKey() string {
	return "ws:endpoints"
}

// UserConnsKey 返回用户多端连接集合 Key。
func UserConnsKey(userID string) string {
	return fmt.Sprintf("ws:user:%s:conns", userID)
}

// ConnKey 返回单个连接详情 Key。
func ConnKey(connID string) string {
	return fmt.Sprintf("ws:conn:%s", connID)
}

// EndpointKey 返回 endpoint 详情 Key。
func EndpointKey(endpointID string) string {
	return fmt.Sprintf("ws:endpoint:%s", endpointID)
}

// EndpointConnsKey 返回节点连接索引 Key。
func EndpointConnsKey(endpointID string) string {
	return fmt.Sprintf("ws:endpoint:%s:conns", endpointID)
}

// LastEndpointKey 返回用户最近连接节点 Key。
func LastEndpointKey(userID string) string {
	return fmt.Sprintf("route:last_endpoint:%s", userID)
}

// ChannelNodesKey 返回频道承载节点租约有序集合 Key。
func ChannelNodesKey(channelID string) string {
	return fmt.Sprintf("live:channel:%s:nodes", channelID)
}

// NodeChannelsKey 返回节点承载频道反向索引集合 Key。
func NodeChannelsKey(endpointID string) string {
	return fmt.Sprintf("live:node:%s:channels", endpointID)
}

// NodeChannelTopic 返回指定节点接收频道广播时使用的 Redis Pub/Sub 频道名。
func NodeChannelTopic(endpointID string, channelID string) string {
	return fmt.Sprintf("live:node:%s:channel:%s", endpointID, channelID)
}

// NodeChannelTopicPattern 返回指定节点订阅频道广播时使用的 Redis Pub/Sub 匹配模式。
func NodeChannelTopicPattern(endpointID string) string {
	return NodeChannelTopic(EscapePatternLiteral(endpointID), "*")
}
