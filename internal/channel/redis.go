package channel

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"time"

	"github.com/yinjiaming666/wskit/internal/redisclient"
	"github.com/yinjiaming666/wskit/internal/rediskey"
)

// RedisRegistry 使用带过期分数的 ZSET 保存频道节点租约。
// 分数按节点独立过期，避免一个活跃节点刷新整个集合后让故障节点永久残留。
type RedisRegistry struct {
	client *redisclient.Client
	ttl    time.Duration
}

// NewRedisRegistry 创建使用默认租约时长的 Redis 频道路由表。
func NewRedisRegistry(client *redisclient.Client) *RedisRegistry {
	return &RedisRegistry{client: client, ttl: DefaultLeaseTTL}
}

// RegisterNode 注册一条频道到节点的短租约，并维护节点反向频道索引。
func (r *RedisRegistry) RegisterNode(ctx context.Context, channelID string, endpointID string) error {
	if r.client == nil {
		return errors.New("channel registry redis client is nil")
	}
	if channelID == "" || endpointID == "" {
		return errors.New("channel route is incomplete")
	}
	expiresAt := strconv.FormatInt(time.Now().Add(r.ttl).Unix(), 10)
	keyTTL := strconv.Itoa(int((r.ttl * 3).Seconds()))
	channelKey := r.client.Key(rediskey.ChannelNodesKey(channelID))
	nodeKey := r.client.Key(rediskey.NodeChannelsKey(endpointID))
	return r.client.Pipeline(ctx,
		[]string{"ZADD", channelKey, expiresAt, endpointID},
		[]string{"EXPIRE", channelKey, keyTTL},
		[]string{"SADD", nodeKey, channelID},
		[]string{"EXPIRE", nodeKey, keyTTL},
	)
}

// UnregisterNode 删除一条频道节点租约和对应反向索引。
func (r *RedisRegistry) UnregisterNode(ctx context.Context, channelID string, endpointID string) error {
	if r.client == nil {
		return errors.New("channel registry redis client is nil")
	}
	if channelID == "" || endpointID == "" {
		return nil
	}
	return r.client.Pipeline(ctx,
		[]string{"ZREM", r.client.Key(rediskey.ChannelNodesKey(channelID)), endpointID},
		[]string{"SREM", r.client.Key(rediskey.NodeChannelsKey(endpointID)), channelID},
	)
}

// RefreshNodeChannels 用节点当前本地频道快照刷新每个频道的独立租约。
// Redis 中存在但本地已不存在的频道会在同一 pipeline 内移除。
func (r *RedisRegistry) RefreshNodeChannels(ctx context.Context, endpointID string, channelIDs []string) error {
	if r.client == nil {
		return errors.New("channel registry redis client is nil")
	}
	if endpointID == "" {
		return errors.New("channel route endpoint is empty")
	}
	nodeKey := r.client.Key(rediskey.NodeChannelsKey(endpointID))
	stored, err := r.client.Strings(ctx, "SMEMBERS", nodeKey)
	if err != nil {
		return err
	}

	current := uniqueSorted(channelIDs)
	currentSet := make(map[string]struct{}, len(current))
	for _, channelID := range current {
		currentSet[channelID] = struct{}{}
	}
	stale := make([]string, 0)
	for _, channelID := range stored {
		if _, ok := currentSet[channelID]; !ok && channelID != "" {
			stale = append(stale, channelID)
		}
	}
	sort.Strings(stale)

	expiresAt := strconv.FormatInt(time.Now().Add(r.ttl).Unix(), 10)
	keyTTL := strconv.Itoa(int((r.ttl * 3).Seconds()))
	commands := make([][]string, 0, len(current)*2+len(stale)+3)
	for _, channelID := range current {
		channelKey := r.client.Key(rediskey.ChannelNodesKey(channelID))
		commands = append(commands,
			[]string{"ZADD", channelKey, expiresAt, endpointID},
			[]string{"EXPIRE", channelKey, keyTTL},
		)
	}
	for _, channelID := range stale {
		commands = append(commands, []string{
			"ZREM",
			r.client.Key(rediskey.ChannelNodesKey(channelID)),
			endpointID,
		})
	}

	// 重建反向索引，确保异常中断留下的旧频道不会被后续停止流程继续使用。
	commands = append(commands, []string{"DEL", nodeKey})
	if len(current) > 0 {
		addChannels := []string{"SADD", nodeKey}
		addChannels = append(addChannels, current...)
		commands = append(commands, addChannels, []string{"EXPIRE", nodeKey, keyTTL})
	}
	return r.client.Pipeline(ctx, commands...)
}

// RemoveNode 合并本地快照与 Redis 反向索引，尽可能完整地清理节点租约。
func (r *RedisRegistry) RemoveNode(ctx context.Context, endpointID string, channelIDs []string) error {
	if r.client == nil {
		return errors.New("channel registry redis client is nil")
	}
	if endpointID == "" {
		return nil
	}
	nodeKey := r.client.Key(rediskey.NodeChannelsKey(endpointID))
	stored, err := r.client.Strings(ctx, "SMEMBERS", nodeKey)
	if err != nil {
		return err
	}
	allChannels := uniqueSorted(append(append([]string(nil), channelIDs...), stored...))
	commands := make([][]string, 0, len(allChannels)+1)
	for _, channelID := range allChannels {
		commands = append(commands, []string{
			"ZREM",
			r.client.Key(rediskey.ChannelNodesKey(channelID)),
			endpointID,
		})
	}
	commands = append(commands, []string{"DEL", nodeKey})
	return r.client.Pipeline(ctx, commands...)
}

// ListChannelNodes 清理已过期分数并返回当前仍有效的频道承载节点。
func (r *RedisRegistry) ListChannelNodes(ctx context.Context, channelID string) ([]string, error) {
	if r.client == nil {
		return nil, errors.New("channel registry redis client is nil")
	}
	if channelID == "" {
		return nil, nil
	}
	channelKey := r.client.Key(rediskey.ChannelNodesKey(channelID))
	now := strconv.FormatInt(time.Now().Unix(), 10)
	results, err := r.client.StringLists(ctx,
		[]string{"ZREMRANGEBYSCORE", channelKey, "-inf", now},
		[]string{"ZRANGEBYSCORE", channelKey, "(" + now, "+inf"},
	)
	if err != nil {
		return nil, err
	}
	if len(results) < 2 {
		return nil, errors.New("channel route query result is incomplete")
	}
	nodes := uniqueSorted(results[1])
	return nodes, nil
}
