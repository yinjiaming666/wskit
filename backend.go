// Package socketserver embeds Redis-backed WebSocket routing, nodes and message delivery in a Go application.
// The application supplies authenticated identities and authorized channel subscriptions.
package socketserver

import (
	"context"
	"errors"

	"github.com/yinjiaming666/wskit/internal/ack"
	"github.com/yinjiaming666/wskit/internal/channel"
	"github.com/yinjiaming666/wskit/internal/endpoint"
	"github.com/yinjiaming666/wskit/internal/redisclient"
	"github.com/yinjiaming666/wskit/internal/route"
)

// Backend holds the state shared by gateways, nodes and senders. Close it after they stop.
type Backend struct {
	client    *redisclient.Client
	endpoints *endpoint.RedisRepository
	routes    *route.RedisResolver
	channels  *channel.RedisRegistry
	acks      *ack.RedisStore
}

// RedisConfig identifies a Redis cluster namespace. All components share the DB and KeyPrefix.
type RedisConfig = redisclient.Config

// NewRedisBackend creates a connection pool for this process. Ready checks connectivity.
func NewRedisBackend(cfg RedisConfig) *Backend {
	client := redisclient.New(cfg)
	return &Backend{
		client:    client,
		endpoints: endpoint.NewRedisRepository(client),
		routes:    route.NewRedisResolver(client),
		channels:  channel.NewRedisRegistry(client),
		acks:      ack.NewRedisStore(client),
	}
}

func (b *Backend) validate() error {
	if b == nil || b.client == nil {
		return errors.New("Redis backend is required")
	}
	return nil
}

func (b *Backend) Ready(ctx context.Context) error {
	if err := b.validate(); err != nil {
		return err
	}
	return b.client.Ping(ctx)
}

func (b *Backend) Close() error {
	if b == nil || b.client == nil {
		return nil
	}
	return b.client.Close()
}
