package ack

import (
	"context"
	"time"
)

// Store 定义消息 ACK 临时状态存储接口。
type Store interface {
	// AddPending 添加待 ACK 的连接编号。
	AddPending(ctx context.Context, messageID string, connIDs []string) error
	// MarkDone 将指定连接标记为已 ACK。
	MarkDone(ctx context.Context, messageID string, connID string) error
	// MarkFailed 将指定连接标记为 ACK 失败。
	MarkFailed(ctx context.Context, messageID string, connID string, reason string) error
	// GetPending 查询指定消息仍未 ACK 的连接编号。
	GetPending(ctx context.Context, messageID string) ([]string, error)
	// GetDone 查询指定消息已经 ACK 的连接编号。
	GetDone(ctx context.Context, messageID string) ([]string, error)
	// GetFailed 查询指定消息 ACK 失败的连接编号。
	GetFailed(ctx context.Context, messageID string) ([]string, error)
}

// PendingItem 表示一条待 ACK 连接记录。
type PendingItem struct {
	// MessageID 表示消息编号。
	MessageID string
	// ConnID 表示连接编号。
	ConnID string
	// CreatedAt 表示进入 pending 的时间。
	CreatedAt time.Time
}

// TimeoutStore 定义支持 ACK 超时扫描的存储接口。
type TimeoutStore interface {
	Store
	// ListExpiredPending 查询已经超时的 pending 记录。
	ListExpiredPending(ctx context.Context, before time.Time, limit int) ([]PendingItem, error)
}
