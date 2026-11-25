package sender

import (
	"context"
	"errors"

	"github.com/yinjiaming666/wskit/internal/domain"
	"github.com/yinjiaming666/wskit/internal/rediskey"
	"github.com/yinjiaming666/wskit/internal/route"
)

// ConnPusher is the direct transport capability needed by the routing sender.
type ConnPusher interface {
	PushToConns(context.Context, string, []string, *domain.Message) (*domain.DeliveryResult, error)
}

// PubSubClient 定义 Redis Pub/Sub 发送能力接口。
type PubSubClient interface {
	// Publish 向指定频道发布消息。
	Publish(ctx context.Context, channel string, payload []byte) error
}

// BatchPubSubClient 定义把同一消息批量发布到多个频道的可选能力。
type BatchPubSubClient interface {
	// PublishMany 使用一次批处理发布多个节点频道。
	PublishMany(ctx context.Context, channels []string, payload []byte) error
}

// ChannelNodeResolver 定义频道承载节点查询能力。
type ChannelNodeResolver interface {
	// ListChannelNodes 返回当前仍持有频道租约的节点编号。
	ListChannelNodes(ctx context.Context, channelID string) ([]string, error)
}

// GRPCSender 使用内部推送客户端向指定 WebSocket Node 精准推送。
type GRPCSender struct {
	// client 表示内部推送客户端。
	client ConnPusher
}

// NewGRPCSender 创建 gRPC 精准推送发送器。
func NewGRPCSender(client ConnPusher) *GRPCSender {
	return &GRPCSender{client: client}
}

// groupMessageRoutes groups resolved connections by their owning node.
func groupMessageRoutes(routes []*domain.ConnInfo) (route.EndpointGroup, error) {
	groups := make(route.EndpointGroup)
	seen := make(map[string]struct{}, len(routes))
	for _, info := range routes {
		if info == nil {
			continue
		}
		if info.ConnID == "" || info.EndpointID == "" {
			return nil, errors.New("delivery route is incomplete")
		}
		if _, ok := seen[info.ConnID]; ok {
			continue
		}
		seen[info.ConnID] = struct{}{}
		groups[info.EndpointID] = append(groups[info.EndpointID], info.ConnID)
	}
	return groups, nil
}

// Send 按 endpoint_id 分组后批量推送连接消息。
func (s *GRPCSender) Send(ctx context.Context, msg *domain.Message) (*domain.DeliveryResult, error) {
	if msg == nil {
		return nil, errors.New("message is nil")
	}

	groups, err := groupMessageRoutes(msg.DeliveryRoutes)
	if err != nil {
		return nil, err
	}

	type outcome struct {
		connIDs []string
		resp    *domain.DeliveryResult
		err     error
	}
	outcomes := make(chan outcome, len(groups))
	sem := make(chan struct{}, 8)
	for endpointID, connIDs := range groups {
		endpointID := endpointID
		connIDs := append([]string(nil), connIDs...)
		go func() {
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				outcomes <- outcome{connIDs: connIDs, err: ctx.Err()}
				return
			}
			resp, err := s.client.PushToConns(ctx, endpointID, connIDs, msg)
			outcomes <- outcome{connIDs: connIDs, resp: resp, err: err}
		}()
	}

	result := &domain.DeliveryResult{Accepted: true}
	for range groups {
		current := <-outcomes
		if current.err != nil {
			result.Accepted = false
			result.FailedConnIDs = append(result.FailedConnIDs, current.connIDs...)
			result.Reason = current.err.Error()
			continue
		}
		// 防御异常客户端实现返回 nil 响应，避免聚合阶段触发空指针崩溃。
		if current.resp == nil {
			result.Accepted = false
			result.FailedConnIDs = append(result.FailedConnIDs, current.connIDs...)
			result.Reason = "push client returned nil response"
			continue
		}
		result.AcceptedConnIDs = append(result.AcceptedConnIDs, current.resp.AcceptedConnIDs...)
		result.FailedConnIDs = append(result.FailedConnIDs, current.resp.FailedConnIDs...)
		if !current.resp.Accepted {
			result.Accepted = false
			result.Reason = current.resp.Reason
		}
	}
	return result, nil
}

// RedisPubSubSender 使用 Redis Pub/Sub 向承载频道的节点发布消息。
type RedisPubSubSender struct {
	// client 表示 Pub/Sub 发布客户端。
	client PubSubClient
	// channelNodes 查找当前承载频道的节点。
	channelNodes ChannelNodeResolver
}

// NewRoutedRedisPubSubSender 创建按频道节点租约精准扇出的 Pub/Sub 发送器。
func NewRoutedRedisPubSubSender(client PubSubClient, channelNodes ChannelNodeResolver) *RedisPubSubSender {
	return &RedisPubSubSender{client: client, channelNodes: channelNodes}
}

// Send 通过 Redis Pub/Sub 向承载频道的节点发布消息。
func (s *RedisPubSubSender) Send(ctx context.Context, msg *domain.Message) error {
	if msg == nil {
		return errors.New("message is nil")
	}

	body, err := msg.Frame.MarshalBinary()
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	nodes, err := s.channelNodes.ListChannelNodes(ctx, msg.Channel)
	if err != nil {
		return err
	}
	topics := make([]string, 0, len(nodes))
	for _, endpointID := range nodes {
		topics = append(topics, rediskey.NodeChannelTopic(endpointID, msg.Channel))
	}

	// Redis 发布器支持批处理时，一次 pipeline 完成所有目标节点扇出。
	if batchClient, ok := s.client.(BatchPubSubClient); ok {
		if err := batchClient.PublishMany(ctx, topics, body); err != nil {
			return err
		}
		return nil
	}
	for _, topic := range topics {
		if err := s.client.Publish(ctx, topic, body); err != nil {
			return err
		}
	}
	return nil
}
