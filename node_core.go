package socketserver

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/yinjiaming666/wskit/internal/ack"
	"github.com/yinjiaming666/wskit/internal/channel"
	"github.com/yinjiaming666/wskit/internal/domain"
	"github.com/yinjiaming666/wskit/internal/endpoint"
	"github.com/yinjiaming666/wskit/internal/route"
)

const (
	// connRouteTTL 与 Redis 默认值一致，节点每次心跳都会批量续期。
	connRouteTTL           = 30 * time.Second
	channelRefreshInterval = channel.DefaultLeaseTTL / 3
	maxIDLength            = 128
)

// NodeConfig 表示 WebSocket Node 启动配置。
type NodeConfig struct {
	// OnMessage handles raw data frames. A nil handler is suitable for push-only nodes.
	OnMessage         MessageHandler
	EndpointID        string
	WSPath            string
	WSAddr            string
	GRPCAddr          string
	MaxConn           int64
	SendBuffer        int
	HeartbeatInterval time.Duration
}

// nodeCore 表示一个 WebSocket 长连接节点。
type nodeCore struct {
	config           NodeConfig
	endpointRepo     endpoint.Repository
	routeResolver    route.Resolver
	ackStore         ack.Store
	heartbeatMu      sync.Mutex
	workers          sync.WaitGroup
	channelHub       *channelHub
	channelRegistry  channel.Registry // 保存频道到承载节点的短租约路由。
	subscriptionMu   sync.Mutex       // 串行化订阅变更，保证本地状态与租约同步。
	mu               sync.RWMutex
	clients          map[string]*localClient
	userConns        map[string]map[string]struct{}
	stopping         bool
	grpcStarted      bool
	grpcReady        bool
	grpcFailure      chan struct{}
	grpcFailureCause error
	stopCh           chan struct{}
	stopOnce         sync.Once
	stopErr          error
	connLocks        [256]sync.Mutex
}

// newNodeCore 创建 WebSocket Node。
func newNodeCore(config NodeConfig, endpointRepo endpoint.Repository, routeResolver route.Resolver, ackStore ack.Store) *nodeCore {
	if config.MaxConn <= 0 {
		config.MaxConn = 50000
	}
	if config.SendBuffer <= 0 {
		config.SendBuffer = 128
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = 15 * time.Second
	}
	return &nodeCore{config: config, endpointRepo: endpointRepo, routeResolver: routeResolver, ackStore: ackStore, channelHub: newChannelHub(), clients: make(map[string]*localClient), userConns: make(map[string]map[string]struct{}), stopCh: make(chan struct{}), grpcFailure: make(chan struct{})}
}

// SetChannelRegistry 在 Start 之前设置频道节点租约表。
func (n *nodeCore) SetChannelRegistry(registry channel.Registry) {
	n.channelRegistry = registry
}

// Start 启动节点注册、连接路由续期和频道租约刷新。
func (n *nodeCore) Start(ctx context.Context) error {
	if err := n.HeartbeatOnce(ctx, "online"); err != nil {
		return err
	}
	n.workers.Add(1)
	go func() {
		defer n.workers.Done()
		heartbeatTicker := time.NewTicker(n.config.HeartbeatInterval)
		defer heartbeatTicker.Stop()
		// 连接绑定 TTL 为 30 秒，独立每 10 秒续期，避免业务心跳配置影响路由存活。
		routeRefreshTicker := time.NewTicker(connRouteTTL / 3)
		defer routeRefreshTicker.Stop()

		// 未设置频道租约表时禁用该定时分支。
		var channelLeaseTick <-chan time.Time
		var channelLeaseTicker *time.Ticker
		if n.channelRegistry != nil {
			channelLeaseTicker = time.NewTicker(channelRefreshInterval)
			channelLeaseTick = channelLeaseTicker.C
			defer channelLeaseTicker.Stop()
		}

		for {
			select {
			case <-heartbeatTicker.C:
				heartbeatCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				if err := n.HeartbeatOnce(heartbeatCtx, "online"); err != nil {
					log.Printf("websocket heartbeat failed endpoint=%s error=%v", n.config.EndpointID, err)
				}
				cancel()
			case <-routeRefreshTicker.C:
				refreshCtx, cancel := context.WithTimeout(context.Background(), connRouteTTL/3)
				if err := n.routeResolver.RefreshEndpointConns(refreshCtx, n.config.EndpointID, connRouteTTL); err != nil {
					log.Printf("websocket route refresh failed endpoint=%s error=%v", n.config.EndpointID, err)
				}
				cancel()
			case <-channelLeaseTick:
				refreshCtx, cancel := context.WithTimeout(context.Background(), channelRefreshInterval/2)
				if err := n.refreshChannelLeases(refreshCtx); err != nil {
					log.Printf("websocket channel lease refresh failed endpoint=%s error=%v", n.config.EndpointID, err)
				}
				cancel()
			case <-n.stopCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

// refreshChannelLeases 用本地频道快照刷新节点短租约，并与订阅变更串行化。
func (n *nodeCore) refreshChannelLeases(ctx context.Context) error {
	if n.channelRegistry == nil {
		return nil
	}
	n.subscriptionMu.Lock()
	defer n.subscriptionMu.Unlock()
	return n.channelRegistry.RefreshNodeChannels(ctx, n.config.EndpointID, n.channelHub.ListChannels())
}

// WaitWorkers waits for goroutines started with a canceled run context.
func (n *nodeCore) WaitWorkers() { n.workers.Wait() }

// Stop 将节点标记为 draining 并批量清理本节点连接路由。
func (n *nodeCore) Stop(ctx context.Context) error {
	n.stopOnce.Do(func() {
		n.mu.Lock()
		n.stopping = true
		close(n.stopCh)
		n.mu.Unlock()
		n.workers.Wait()

		// 等待所有正在注册或删除的连接完成，防止停止清理后又写回 Redis 路由。
		for index := range n.connLocks {
			n.connLocks[index].Lock()
		}
		defer func() {
			for index := len(n.connLocks) - 1; index >= 0; index-- {
				n.connLocks[index].Unlock()
			}
		}()

		n.mu.Lock()
		clients := make([]*localClient, 0, len(n.clients))
		for _, client := range n.clients {
			clients = append(clients, client)
		}
		n.clients = make(map[string]*localClient)
		n.userConns = make(map[string]map[string]struct{})
		n.mu.Unlock()

		if err := n.HeartbeatOnce(ctx, "draining"); err != nil {
			n.stopErr = err
		}
		// 停止期间阻止新的频道状态变更，并用停止前快照清理节点全部频道租约。
		n.subscriptionMu.Lock()
		channelIDs := n.channelHub.ListChannels()
		for _, client := range clients {
			n.channelHub.RemoveClient(client)
			client.Close()
		}
		if n.channelRegistry != nil {
			if err := n.channelRegistry.RemoveNode(ctx, n.config.EndpointID, channelIDs); err != nil && n.stopErr == nil {
				n.stopErr = err
			}
		}
		n.subscriptionMu.Unlock()

		if err := n.routeResolver.RemoveEndpointConns(ctx, n.config.EndpointID); err != nil && n.stopErr == nil {
			n.stopErr = err
		}
		if err := n.endpointRepo.DeleteEndpoint(ctx, n.config.EndpointID); err != nil && n.stopErr == nil {
			n.stopErr = err
		}
	})
	return n.stopErr
}

// BeginDrain 将节点标记为 draining，Gateway 将停止分配新连接，现有连接继续服务到部署超时。
func (n *nodeCore) BeginDrain(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n.heartbeatMu.Lock()
	defer n.heartbeatMu.Unlock()
	select {
	case <-n.stopCh:
		return errors.New("node has stopped")
	default:
	}
	n.mu.Lock()
	n.stopping = true
	n.mu.Unlock()
	return n.heartbeat(ctx, "draining")
}

// Ready 返回节点是否仍可接收新连接。
func (n *nodeCore) Ready() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return !n.stopping && (n.config.GRPCAddr == "" || n.grpcReady)
}

// HeartbeatOnce 上报一次 endpoint 状态和连接数量。
func (n *nodeCore) HeartbeatOnce(ctx context.Context, status string) error {
	n.heartbeatMu.Lock()
	defer n.heartbeatMu.Unlock()
	return n.heartbeat(ctx, status)
}

func (n *nodeCore) heartbeat(ctx context.Context, status string) error {
	n.mu.RLock()
	connCount := int64(len(n.clients))
	grpcAddr := n.config.GRPCAddr
	stopping := n.stopping || grpcAddr != "" && !n.grpcReady
	n.mu.RUnlock()
	if stopping && status == "online" {
		status = "draining"
	}
	ep := &domain.Endpoint{ID: n.config.EndpointID, Status: status, WSPath: n.config.WSPath, WSAddr: n.config.WSAddr, GRPCAddr: grpcAddr, ConnCount: connCount, MaxConn: n.config.MaxConn, UpdatedAt: time.Now().Unix()}
	return n.endpointRepo.UpsertEndpoint(ctx, ep)
}

// RegisterSession binds the identity supplied by the application to a local connection.
func (n *nodeCore) RegisterSession(ctx context.Context, info SessionInfo) (*localClient, error) {
	connID, userID := info.ConnID, info.UserID
	if connID == "" || userID == "" {
		return nil, errors.New("conn_id and user_id are required")
	}
	if len(connID) > maxIDLength || len(userID) > maxIDLength || len(info.DeviceID) > maxIDLength || len(info.Platform) > 32 {
		return nil, errors.New("connection metadata is too long")
	}

	connLock := n.connectionLock(connID)
	connLock.Lock()
	defer connLock.Unlock()

	client := newClient(connID, userID, info.DeviceID, info.Platform, n.config.SendBuffer)
	var oldClient *localClient

	n.mu.Lock()
	if n.stopping || n.config.GRPCAddr != "" && !n.grpcReady {
		n.mu.Unlock()
		return nil, errors.New("websocket node is draining")
	}
	oldClient = n.clients[connID]
	if oldClient == nil && int64(len(n.clients)) >= n.config.MaxConn {
		n.mu.Unlock()
		return nil, errors.New("websocket node is full")
	}
	if oldClient != nil {
		delete(n.userConns[oldClient.UserID], connID)
		if len(n.userConns[oldClient.UserID]) == 0 {
			delete(n.userConns, oldClient.UserID)
		}
	}
	n.clients[connID] = client
	if _, ok := n.userConns[userID]; !ok {
		n.userConns[userID] = make(map[string]struct{})
	}
	n.userConns[userID][connID] = struct{}{}
	n.mu.Unlock()

	if oldClient != nil {
		if err := n.removeClientFromChannels(context.Background(), oldClient); err != nil {
			log.Printf("replace connection channel lease cleanup failed conn=%s error=%v", connID, err)
		}
		oldClient.Close()
	}

	now := time.Now().Unix()
	binding := &domain.ConnInfo{ConnID: connID, UserID: userID, EndpointID: n.config.EndpointID, DeviceID: info.DeviceID, Platform: info.Platform, GRPCAddr: n.config.GRPCAddr, CreatedAt: now, UpdatedAt: now}
	if err := n.routeResolver.RegisterConn(ctx, binding); err != nil {
		n.removeLocalClient(client)
		_ = n.routeResolver.RemoveConn(context.Background(), connID)
		return nil, err
	}
	return client, nil
}

// RemoveClientConnection 仅在 client 仍是当前 conn_id 对应实例时移除连接。
func (n *nodeCore) RemoveClientConnection(ctx context.Context, client *localClient) error {
	if client == nil {
		return nil
	}
	return n.removeConnection(ctx, client.ConnID, client)
}

// RemoveConnection 按 conn_id 移除当前连接。
func (n *nodeCore) RemoveConnection(ctx context.Context, connID string) error {
	return n.removeConnection(ctx, connID, nil)
}

func (n *nodeCore) removeConnection(ctx context.Context, connID string, expected *localClient) error {
	connLock := n.connectionLock(connID)
	connLock.Lock()
	defer connLock.Unlock()

	n.mu.Lock()
	client := n.clients[connID]
	if expected != nil && client != expected {
		n.mu.Unlock()
		return nil
	}
	if client != nil {
		delete(n.clients, connID)
		delete(n.userConns[client.UserID], connID)
		if len(n.userConns[client.UserID]) == 0 {
			delete(n.userConns, client.UserID)
		}
	}
	n.mu.Unlock()

	var channelErr error
	if client != nil {
		channelErr = n.removeClientFromChannels(ctx, client)
		client.Close()
	}
	if client == nil && expected != nil {
		return nil
	}
	if err := n.routeResolver.RemoveConn(ctx, connID); err != nil {
		return err
	}
	return channelErr
}

func (n *nodeCore) removeLocalClient(expected *localClient) {
	n.mu.Lock()
	if n.clients[expected.ConnID] == expected {
		delete(n.clients, expected.ConnID)
		delete(n.userConns[expected.UserID], expected.ConnID)
		if len(n.userConns[expected.UserID]) == 0 {
			delete(n.userConns, expected.UserID)
		}
	}
	n.mu.Unlock()
	if err := n.removeClientFromChannels(context.Background(), expected); err != nil {
		log.Printf("local connection channel lease cleanup failed conn=%s error=%v", expected.ConnID, err)
	}
	expected.Close()
}

// removeClientFromChannels 删除指定连接实例的订阅，并在频道失去最后一个本地订阅者时撤销节点租约。
func (n *nodeCore) removeClientFromChannels(ctx context.Context, expected *localClient) error {
	if expected == nil {
		return nil
	}
	n.subscriptionMu.Lock()
	defer n.subscriptionMu.Unlock()

	emptyChannels := n.channelHub.RemoveClient(expected)
	if n.channelRegistry == nil {
		return nil
	}
	var firstErr error
	for _, channelID := range emptyChannels {
		if err := n.channelRegistry.UnregisterNode(ctx, channelID, n.config.EndpointID); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (n *nodeCore) connectionLock(connID string) *sync.Mutex {
	var hash uint32 = 2166136261
	for index := 0; index < len(connID); index++ {
		hash ^= uint32(connID[index])
		hash *= 16777619
	}
	return &n.connLocks[hash%uint32(len(n.connLocks))]
}

// Subscribe 为本地连接登记频道订阅，并在首个本地订阅者加入时注册节点租约。
func (n *nodeCore) Subscribe(ctx context.Context, connID string, channelID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if channelID == "" || len(channelID) > maxIDLength {
		return errors.New("invalid channel")
	}

	n.subscriptionMu.Lock()
	defer n.subscriptionMu.Unlock()
	n.mu.RLock()
	client := n.clients[connID]
	stopping := n.stopping
	n.mu.RUnlock()
	if stopping {
		return errors.New("websocket node is draining")
	}
	if client == nil {
		return errors.New("conn not found")
	}

	subscribed, firstLocalSubscriber := n.channelHub.Subscribe(channelID, client)
	if !subscribed {
		return errors.New("channel limit reached or connection closed")
	}
	if firstLocalSubscriber && n.channelRegistry != nil {
		if err := n.channelRegistry.RegisterNode(ctx, channelID, n.config.EndpointID); err != nil {
			// 首次租约注册失败时回滚本地订阅，避免客户端误以为后续可以收到跨节点消息。
			n.channelHub.Unsubscribe(channelID, connID)
			return err
		}
	}
	return nil
}

// Unsubscribe 取消本地连接的频道订阅，并在最后一个本地订阅者离开时撤销节点租约。
func (n *nodeCore) Unsubscribe(ctx context.Context, connID string, channelID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if channelID == "" || len(channelID) > maxIDLength {
		return errors.New("invalid channel")
	}

	n.subscriptionMu.Lock()
	defer n.subscriptionMu.Unlock()
	lastLocalSubscriber := n.channelHub.Unsubscribe(channelID, connID)
	if lastLocalSubscriber && n.channelRegistry != nil {
		return n.channelRegistry.UnregisterNode(ctx, channelID, n.config.EndpointID)
	}
	return nil
}

// BroadcastChannel 向本节点指定频道广播消息。
func (n *nodeCore) BroadcastChannel(ctx context.Context, channelID string, frame Frame) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return n.channelHub.Broadcast(channelID, frame), nil
}

// PushToConns 将客户端消息写入本节点指定连接的发送队列。
func (n *nodeCore) PushToConns(ctx context.Context, connIDs []string, msg *domain.Message) (*domain.DeliveryResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, errors.New("message is nil")
	}
	if err := msg.Frame.Validate(); err != nil {
		return nil, err
	}
	resp := &domain.DeliveryResult{Accepted: true}
	for _, connID := range connIDs {
		n.mu.RLock()
		client := n.clients[connID]
		n.mu.RUnlock()
		if client == nil || !client.Enqueue(msg.Frame) {
			resp.FailedConnIDs = append(resp.FailedConnIDs, connID)
			continue
		}
		resp.AcceptedConnIDs = append(resp.AcceptedConnIDs, connID)
	}
	if len(resp.FailedConnIDs) > 0 {
		resp.Accepted = false
		resp.Reason = "some connections failed"
	}
	return resp, nil
}

// PushToUsers 将消息写入本节点指定用户的发送队列。
func (n *nodeCore) PushToUsers(ctx context.Context, userIDs []string, msg *domain.Message) (*domain.DeliveryResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	connIDs := make([]string, 0)
	n.mu.RLock()
	for _, userID := range userIDs {
		for connID := range n.userConns[userID] {
			connIDs = append(connIDs, connID)
		}
	}
	n.mu.RUnlock()
	return n.PushToConns(ctx, connIDs, msg)
}

// HandleClientAck 处理客户端通过 WebSocket 上报的 ACK。
func (n *nodeCore) HandleClientAck(ctx context.Context, messageID string, connID string) error {
	if messageID == "" || connID == "" {
		return errors.New("ack message_id or conn_id is empty")
	}
	n.mu.RLock()
	clientExists := n.clients[connID] != nil
	n.mu.RUnlock()
	if !clientExists {
		return errors.New("conn not found")
	}
	return n.ackStore.MarkDone(ctx, messageID, connID)
}

// ConnCount 返回当前节点本地连接数量。
func (n *nodeCore) ConnCount() int64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return int64(len(n.clients))
}

// ChannelConns 返回指定频道在本节点的连接编号列表。
func (n *nodeCore) ChannelConns(channelID string) []string { return n.channelHub.ListConns(channelID) }
