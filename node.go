package wskit

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yinjiaming666/wskit/internal/domain"
	"github.com/yinjiaming666/wskit/internal/httpx"
)

type Node struct {
	core    *nodeCore
	handler *nodeHandler
	backend *Backend
	started atomic.Bool
	stateMu sync.RWMutex
	active  bool
}

func CreateNode(cfg NodeConfig, backend *Backend) (*Node, error) {
	if err := backend.validate(); err != nil {
		return nil, err
	}
	if cfg.EndpointID == "" {
		return nil, errors.New("endpoint ID is required")
	}
	if cfg.WSPath == "" {
		cfg.WSPath = "/ws/" + url.PathEscape(cfg.EndpointID)
	}
	if _, err := url.ParseRequestURI(cfg.WSPath); err != nil || !strings.HasPrefix(cfg.WSPath, "/") || strings.ContainsAny(cfg.WSPath, "?#") {
		return nil, errors.New("invalid WebSocket path")
	}
	if cfg.GRPCAddr == "" {
		return nil, errors.New("GRPCAddr is required")
	}
	core := newNodeCore(cfg, backend.endpoints, backend.routes, backend.acks)
	core.SetChannelRegistry(backend.channels)
	return &Node{core: core, handler: newNodeHandler(core), backend: backend}, nil
}

// Run starts gRPC and the channel subscription before publishing the endpoint. It blocks until
// cancellation or failure, then cleans up its connections, leases and listeners.
// A Node runs once. Its HTTP server and shared backend belong to the caller.
func (n *Node) Run(parent context.Context) (runErr error) {
	if !n.started.CompareAndSwap(false, true) {
		return errors.New("node has already run")
	}
	if err := parent.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	publishing := false
	defer func() {
		n.stateMu.Lock()
		n.active = false
		n.stateMu.Unlock()
		cancel()
		if publishing {
			stopCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			runErr = errors.Join(runErr, n.core.Stop(stopCtx))
		} else {
			n.core.WaitWorkers()
		}
		runErr = errors.Join(runErr, n.core.GRPCFailureCause())
	}()
	if err := n.core.StartGRPCServer(ctx); err != nil {
		return err
	}
	if err := n.core.startRedisChannelConsumer(ctx, n.backend.client); err != nil {
		return err
	}
	if err := n.core.GRPCFailureCause(); err != nil {
		return err
	}
	publishing = true
	// A gateway can see the first heartbeat before Start returns.
	n.stateMu.Lock()
	err := n.core.Start(ctx)
	n.active = err == nil
	n.stateMu.Unlock()
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
	case <-n.core.GRPCFailure():
	}
	return nil
}

// Ready reports whether this running node can admit new connections.
// Hosts can additionally call Backend.Ready to check Redis connectivity.
func (n *Node) Ready() bool {
	n.stateMu.RLock()
	defer n.stateMu.RUnlock()
	return n.active && n.core.Ready()
}

// BeginDrain stops new admissions. Existing sessions can continue to receive messages.
func (n *Node) BeginDrain(ctx context.Context) error {
	n.stateMu.RLock()
	defer n.stateMu.RUnlock()
	if !n.active {
		return errors.New("node is not running")
	}
	return n.core.BeginDrain(ctx)
}

// ServeWS upgrades a request for the identity established by the application's HTTP handler.
func (n *Node) ServeWS(w http.ResponseWriter, r *http.Request, info SessionInfo) {
	if !n.Ready() {
		httpx.WriteError(w, http.StatusServiceUnavailable, "node is not ready")
		return
	}
	n.handler.ServeWS(w, r, info)
}

// Subscribe and Unsubscribe operate on an existing local connection after the host authorizes it.
func (n *Node) Subscribe(ctx context.Context, connID, channel string) error {
	return n.core.Subscribe(ctx, connID, channel)
}
func (n *Node) Unsubscribe(ctx context.Context, connID, channel string) error {
	return n.core.Unsubscribe(ctx, connID, channel)
}

// Ack records a receipt for a specific local connection.
func (n *Node) Ack(ctx context.Context, connID, messageID string) error {
	return n.core.HandleClientAck(ctx, messageID, connID)
}

// PushToConns and PushToUsers enqueue a host-encoded frame on this node.
func (n *Node) PushToConns(ctx context.Context, ids []string, frame Frame) (*DeliveryResult, error) {
	frame.Data = bytes.Clone(frame.Data)
	return n.core.PushToConns(ctx, ids, &domain.Message{Frame: frame})
}
func (n *Node) PushToUsers(ctx context.Context, ids []string, frame Frame) (*DeliveryResult, error) {
	frame.Data = bytes.Clone(frame.Data)
	return n.core.PushToUsers(ctx, ids, &domain.Message{Frame: frame})
}

// ConnCount returns the number of active connections on this node.
func (n *Node) ConnCount() int64 { return n.core.ConnCount() }

// GRPCFailure signals unexpected listener failure for process supervisors.
func (n *Node) GRPCFailure() <-chan struct{} { return n.core.GRPCFailure() }
func (n *Node) GRPCFailureCause() error      { return n.core.GRPCFailureCause() }
