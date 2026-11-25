package endpoint

import (
	"context"

	"github.com/yinjiaming666/wskit/internal/domain"
)

// Repository 定义 WebSocket 节点信息仓储接口。
type Repository interface {
	// UpsertEndpoint 新增或更新 WebSocket 节点信息。
	UpsertEndpoint(ctx context.Context, ep *domain.Endpoint) error
	// GetEndpoint 根据节点编号获取 WebSocket 节点信息。
	GetEndpoint(ctx context.Context, endpointID string) (*domain.Endpoint, error)
	// ListEndpoints 获取全部 WebSocket 节点信息。
	ListEndpoints(ctx context.Context) ([]*domain.Endpoint, error)
	// DeleteEndpoint 删除指定 WebSocket 节点信息。
	DeleteEndpoint(ctx context.Context, endpointID string) error
	// MarkDraining 将指定 WebSocket 节点标记为下线迁移状态。
	MarkDraining(ctx context.Context, endpointID string) error
}
