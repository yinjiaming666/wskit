package route

import (
	"context"
	"time"

	"github.com/yinjiaming666/wskit/internal/domain"
)

// EndpointGroup 表示按 endpoint_id 分组后的连接编号列表。
type EndpointGroup map[string][]string

// Resolver 定义用户连接路由查询和维护接口。
type Resolver interface {
	RegisterConn(ctx context.Context, info *domain.ConnInfo) error
	RemoveConn(ctx context.Context, connID string) error
	GetUserConnIDs(ctx context.Context, userID string) ([]string, error)
	GetUsersConnIDs(ctx context.Context, userIDs []string) ([]string, error)
	// ResolveUsers 一次解析多个用户的完整连接路由，供发送链路直接复用。
	ResolveUsers(ctx context.Context, userIDs []string) ([]*domain.ConnInfo, error)
	GetConnInfo(ctx context.Context, connID string) (*domain.ConnInfo, error)
	GetConnEndpoint(ctx context.Context, connID string) (string, error)
	GroupConnsByEndpoint(ctx context.Context, connIDs []string) (EndpointGroup, error)
	GetLastEndpoint(ctx context.Context, userID string) (string, error)
	SetLastEndpoint(ctx context.Context, userID string, endpointID string, ttl time.Duration) error
	ListEndpointConns(ctx context.Context, endpointID string) ([]string, error)
	RemoveEndpointConns(ctx context.Context, endpointID string) error
	RefreshEndpointConns(ctx context.Context, endpointID string, ttl time.Duration) error
}
