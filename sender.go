package wskit

import (
	"context"
	"errors"

	"github.com/yinjiaming666/wskit/internal/bus"
	"github.com/yinjiaming666/wskit/internal/domain"
	"github.com/yinjiaming666/wskit/internal/push"
	"github.com/yinjiaming666/wskit/internal/sender"
)

// Message contains an application-encoded frame and an optional message ID.
type Message struct {
	ID    string
	Frame Frame
}

// DeliveryResult describes queue acceptance per connection. It does not confirm a
// WebSocket write or client receipt. An empty recipient set is accepted.
type DeliveryResult = domain.DeliveryResult

// Delivery associates a direct message ID with its queue acceptance result.
type Delivery struct {
	MessageID string
	DeliveryResult
}

type Sender struct {
	backend   *Backend
	direct    *sender.GRPCSender
	broadcast *sender.RedisPubSubSender
	grpc      *push.GRPCClient
}

// NewSender creates a channel publisher and direct-delivery client for a backend.
func NewSender(backend *Backend) (*Sender, error) {
	if err := backend.validate(); err != nil {
		return nil, err
	}
	grpc := push.NewGRPCClient(backend.endpoints)
	return &Sender{
		backend:   backend,
		grpc:      grpc,
		direct:    sender.NewGRPCSender(grpc),
		broadcast: sender.NewRoutedRedisPubSubSender(bus.NewRedisPublisher(backend.client), backend.channels),
	}, nil
}

// SendToChannel broadcasts a frame to the current channel subscribers over Redis Pub/Sub.
// It returns the publish error, including nil when the channel has no subscribers.
func (s *Sender) SendToChannel(ctx context.Context, channel string, msg Message) error {
	if channel == "" || len(channel) > maxIDLength {
		return errors.New("invalid channel")
	}
	return s.broadcast.Send(ctx, &domain.Message{Channel: channel, Frame: msg.Frame})
}

// Send delivers a one-to-one message to all online devices of toUserID over gRPC.
func (s *Sender) Send(ctx context.Context, toUserID string, msg Message) (Delivery, error) {
	res := Delivery{MessageID: msg.ID}
	if toUserID == "" {
		return res, errors.New("recipient ID is empty")
	}
	if err := msg.Frame.Validate(); err != nil {
		return res, err
	}
	if err := ctx.Err(); err != nil {
		return res, err
	}
	m := &domain.Message{MessageID: msg.ID, ToUserIDs: []string{toUserID}, Frame: msg.Frame}
	routes, err := s.backend.routes.ResolveUsers(ctx, m.ToUserIDs)
	if err != nil {
		return res, err
	}
	m.DeliveryRoutes = routes
	r, err := s.direct.Send(ctx, m)
	if err != nil {
		return res, err
	}
	res.DeliveryResult = *r
	return res, nil
}

func (s *Sender) Close() error { return s.grpc.Close() }
