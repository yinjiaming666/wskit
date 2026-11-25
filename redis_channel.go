package wskit

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/yinjiaming666/wskit/internal/domain"
	"github.com/yinjiaming666/wskit/internal/redisclient"
	"github.com/yinjiaming666/wskit/internal/rediskey"
)

// startRedisChannelConsumer 等待本节点的频道广播订阅就绪。
func (n *nodeCore) startRedisChannelConsumer(ctx context.Context, client *redisclient.Client) error {
	if client == nil {
		return errors.New("redis client is required")
	}
	pattern := rediskey.NodeChannelTopicPattern(n.config.EndpointID)
	ready := make(chan error, 1)
	n.workers.Add(1)
	go func() {
		defer n.workers.Done()
		n.runRedisPatternConsumer(ctx, client, pattern, ready)
	}()
	select {
	case err := <-ready:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *nodeCore) runRedisPatternConsumer(ctx context.Context, client *redisclient.Client, pattern string, ready chan<- error) {
	backoff := time.Second
	for {
		err := client.SubscribePatternReady(ctx, pattern, func() {
			if ready != nil {
				ready <- nil
				ready = nil
			}
		}, func(topic string, payload []byte) {
			channelID := channelIDFromRedisTopic(topic, n.config.EndpointID)
			if channelID == "" {
				return
			}
			frame, err := domain.DecodeFrame(payload)
			if err != nil {
				log.Printf("invalid Redis frame for channel %q: %v", channelID, err)
				return
			}
			if _, err := n.BroadcastChannel(ctx, channelID, frame); err != nil && ctx.Err() == nil {
				log.Printf("redis channel broadcast error pattern=%s channel=%s error=%v", pattern, channelID, err)
			}
		})
		if ready != nil {
			ready <- err
			return
		}
		if ctx.Err() != nil {
			return
		}
		log.Printf("redis channel consumer reconnect pattern=%s error=%v backoff=%s", pattern, err, backoff)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

// channelIDFromRedisTopic 从 Redis Pub/Sub 频道名解析业务频道 ID，并校验消息确实发往当前节点。
func channelIDFromRedisTopic(topic string, endpointID string) string {
	channelID, ok := strings.CutPrefix(topic, rediskey.NodeChannelTopic(endpointID, ""))
	if !ok {
		return ""
	}
	return channelID
}
