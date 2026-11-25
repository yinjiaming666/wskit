package socketserver

import (
	"bytes"
	"context"
	"errors"

	"github.com/yinjiaming666/wskit/internal/domain"
)

// SessionInfo is supplied by the host after authentication. Empty ConnID generates a fresh ID.
// Channels contains initial subscriptions already authorized by the host.
type SessionInfo struct {
	UserID   string
	ConnID   string
	DeviceID string
	Platform string
	Channels []string
}

// Session addresses one connection, including when its ID has subsequently been reused.
type Session struct {
	node   *nodeCore
	client *localClient
}

func (s *Session) UserID() string { return s.client.UserID }
func (s *Session) ConnID() string { return s.client.ConnID }

func (s *Session) Subscribe(ctx context.Context, channel string) error {
	return s.node.withClient(s.client, func() error { return s.node.Subscribe(ctx, s.client.ConnID, channel) })
}

func (s *Session) Unsubscribe(ctx context.Context, channel string) error {
	return s.node.withClient(s.client, func() error { return s.node.Unsubscribe(ctx, s.client.ConnID, channel) })
}

// Frame carries an application data frame with its original text or binary type.
type Frame = domain.Frame
type FrameType = domain.FrameType

const (
	TextFrame   = domain.TextFrame
	BinaryFrame = domain.BinaryFrame
)

// MessageHandler receives one complete application payload. Returning an error closes the session.
// The application can send its own error reply and return nil to keep the session open.
type MessageHandler func(context.Context, *Session, Frame) error

// Send enqueues a copy of a frame encoded by the application.
func (s *Session) Send(frame Frame) error {
	if err := frame.Validate(); err != nil {
		return err
	}
	frame.Data = bytes.Clone(frame.Data)
	if !s.client.Enqueue(frame) {
		return errors.New("connection closed or send queue full")
	}
	return nil
}

// Ack confirms an application message ID for this connection only.
func (s *Session) Ack(ctx context.Context, messageID string) error {
	return s.node.withClient(s.client, func() error { return s.node.HandleClientAck(ctx, messageID, s.ConnID()) })
}

func (n *nodeCore) withClient(client *localClient, fn func() error) error {
	lock := n.connectionLock(client.ConnID)
	lock.Lock()
	defer lock.Unlock()
	n.mu.RLock()
	current := n.clients[client.ConnID]
	n.mu.RUnlock()
	if current != client {
		return errors.New("session is no longer connected")
	}
	return fn()
}
