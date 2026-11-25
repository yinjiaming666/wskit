package push

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	websocketpb "github.com/yinjiaming666/wskit/api/proto/websocket"
	"github.com/yinjiaming666/wskit/internal/domain"
	"github.com/yinjiaming666/wskit/internal/endpoint"
	"github.com/yinjiaming666/wskit/internal/grpcpush"
)

// GRPCClient 使用 protobuf 强类型 gRPC 调用 WebSocket Node 内部推送服务。
type GRPCClient struct {
	endpoints endpoint.Repository
	mu        sync.Mutex
	conns     map[string]*grpc.ClientConn
}

// NewGRPCClient 创建 gRPC 推送客户端。
func NewGRPCClient(endpoints endpoint.Repository) *GRPCClient {
	return &GRPCClient{endpoints: endpoints, conns: make(map[string]*grpc.ClientConn)}
}

// PushToConns 推送消息到指定 endpoint 上的连接。
func (c *GRPCClient) PushToConns(ctx context.Context, endpointID string, connIDs []string, msg *domain.Message) (*domain.DeliveryResult, error) {
	client, err := c.client(ctx, endpointID)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := client.PushToConns(callCtx, &websocketpb.PushToConnsRequest{ConnIds: connIDs, Message: grpcpush.MessageToProto(msg)})
	if err != nil {
		return nil, err
	}
	return grpcpush.PushResponseFromProto(resp), nil
}

// Close 关闭全部缓存的 gRPC 连接。
func (c *GRPCClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	for key, conn := range c.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(c.conns, key)
	}
	return firstErr
}

// client 返回目标节点 gRPC 客户端，连接按 grpc_addr 复用。
func (c *GRPCClient) client(ctx context.Context, endpointID string) (websocketpb.WebSocketPushServiceClient, error) {
	ep, err := c.endpoints.GetEndpoint(ctx, endpointID)
	if err != nil {
		return nil, err
	}
	addr := strings.TrimSpace(ep.GRPCAddr)
	if addr == "" {
		return nil, errors.New("endpoint grpc_addr is empty")
	}

	conn, err := c.conn(ctx, addr)
	if err != nil {
		return nil, err
	}
	return websocketpb.NewWebSocketPushServiceClient(conn), nil
}

// conn 获取或创建指定地址的 gRPC 连接。
func (c *GRPCClient) conn(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	if conn := c.conns[addr]; conn != nil {
		c.mu.Unlock()
		return conn, nil
	}
	c.mu.Unlock()

	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if old := c.conns[addr]; old != nil {
		c.mu.Unlock()
		_ = conn.Close()
		return old, nil
	}
	c.conns[addr] = conn
	c.mu.Unlock()
	return conn, nil
}
