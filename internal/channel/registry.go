// Package channel 维护频道到 WebSocket 节点的短租约路由。
package channel

import (
	"context"
	"sort"
	"time"
)

// DefaultLeaseTTL 是节点频道租约的默认有效期；30 秒可缩短故障节点残留窗口。
const DefaultLeaseTTL = 30 * time.Second

// Registry 定义频道节点路由的注册、刷新、清理与查询能力。
type Registry interface {
	// RegisterNode 在节点首次承载频道时创建短租约路由。
	RegisterNode(ctx context.Context, channelID string, endpointID string) error
	// UnregisterNode 在节点不再承载频道时删除路由。
	UnregisterNode(ctx context.Context, channelID string, endpointID string) error
	// RefreshNodeChannels 按节点本地频道快照刷新租约并清理过期关联。
	RefreshNodeChannels(ctx context.Context, endpointID string, channelIDs []string) error
	// RemoveNode 在节点优雅停止时清理它承载的全部频道路由。
	RemoveNode(ctx context.Context, endpointID string, channelIDs []string) error
	// ListChannelNodes 返回租约仍然有效的频道承载节点。
	ListChannelNodes(ctx context.Context, channelID string) ([]string, error)
}

// uniqueSorted 去重并排序非空标识，保证 Redis 命令和测试结果稳定。
func uniqueSorted(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value != "" {
			seen[value] = struct{}{}
		}
	}
	result := make([]string, 0, len(seen))
	for value := range seen {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
