package framework_test

import (
	"context"
	"errors"
	"github.com/yinjiaming666/wskit/internal/sender"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/yinjiaming666/wskit/internal/domain"
)

// recordingPushClient 记录每个节点收到的连接集合，用于验证发送器路由分组。
type recordingPushClient struct {
	mu    sync.Mutex
	calls map[string][]string
}

// PushToConns 记录一次精准推送并模拟节点全部接收成功。
func (c *recordingPushClient) PushToConns(ctx context.Context, endpointID string, connIDs []string, msg *domain.Message) (*domain.DeliveryResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.calls[endpointID] = append([]string(nil), connIDs...)
	c.mu.Unlock()
	return &domain.DeliveryResult{
		Accepted:        true,
		AcceptedConnIDs: append([]string(nil), connIDs...),
	}, nil
}

// TestGRPCSenderReusesResolvedRoutes 验证完整路由存在时不会依赖 Resolver 再次查询。
func TestGRPCSenderReusesResolvedRoutes(t *testing.T) {
	client := &recordingPushClient{calls: make(map[string][]string)}
	directSender := sender.NewGRPCSender(client)
	msg := &domain.Message{
		DeliveryRoutes: []*domain.ConnInfo{
			{ConnID: "conn-a", EndpointID: "node-1"},
			{ConnID: "conn-b", EndpointID: "node-2"},
			{ConnID: "conn-c", EndpointID: "node-1"},
			// 重复路由用于确认同一连接不会被重复投递。
			{ConnID: "conn-a", EndpointID: "node-1"},
		},
	}

	result, err := directSender.Send(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Accepted || len(result.AcceptedConnIDs) != 3 {
		t.Fatalf("unexpected send result: %#v", result)
	}

	client.mu.Lock()
	node1 := append([]string(nil), client.calls["node-1"]...)
	node2 := append([]string(nil), client.calls["node-2"]...)
	client.mu.Unlock()
	sort.Strings(node1)
	sort.Strings(node2)
	if !reflect.DeepEqual(node1, []string{"conn-a", "conn-c"}) {
		t.Fatalf("unexpected node-1 routes: %#v", node1)
	}
	if !reflect.DeepEqual(node2, []string{"conn-b"}) {
		t.Fatalf("unexpected node-2 routes: %#v", node2)
	}
}

// The aggregate must retain partial acceptance and failure details independently of Go errors.
type pushFunc func(context.Context, string, []string, *domain.Message) (*domain.DeliveryResult, error)

func (f pushFunc) PushToConns(ctx context.Context, endpoint string, ids []string, msg *domain.Message) (*domain.DeliveryResult, error) {
	return f(ctx, endpoint, ids, msg)
}

func TestGRPCSenderDeliveryOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response *domain.DeliveryResult
		err      error
		want     domain.DeliveryResult
	}{
		{name: "all accepted", response: &domain.DeliveryResult{Accepted: true, AcceptedConnIDs: []string{"a", "b"}}, want: domain.DeliveryResult{Accepted: true, AcceptedConnIDs: []string{"a", "b"}}},
		{name: "partial enqueue", response: &domain.DeliveryResult{AcceptedConnIDs: []string{"a"}, FailedConnIDs: []string{"b"}, Reason: "some connections failed"}, want: domain.DeliveryResult{AcceptedConnIDs: []string{"a"}, FailedConnIDs: []string{"b"}, Reason: "some connections failed"}},
		{name: "transport failed", err: errors.New("node unavailable"), want: domain.DeliveryResult{FailedConnIDs: []string{"a", "b"}, Reason: "node unavailable"}},
		{name: "missing response", want: domain.DeliveryResult{FailedConnIDs: []string{"a", "b"}, Reason: "push client returned nil response"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			direct := sender.NewGRPCSender(pushFunc(func(context.Context, string, []string, *domain.Message) (*domain.DeliveryResult, error) {
				return tc.response, tc.err
			}))
			got, err := direct.Send(context.Background(), &domain.Message{DeliveryRoutes: []*domain.ConnInfo{{ConnID: "a", EndpointID: "node"}, {ConnID: "b", EndpointID: "node"}}})
			if err != nil || got == nil || !reflect.DeepEqual(*got, tc.want) {
				t.Fatalf("got %+v, %v; want %+v", got, err, tc.want)
			}
		})
	}
}

func TestGRPCSenderNoOnlineConnections(t *testing.T) {
	direct := sender.NewGRPCSender(pushFunc(func(context.Context, string, []string, *domain.Message) (*domain.DeliveryResult, error) {
		t.Error("empty recipient set reached transport")
		return nil, nil
	}))
	got, err := direct.Send(context.Background(), &domain.Message{})
	if err != nil || got == nil || !got.Accepted || len(got.AcceptedConnIDs) != 0 || len(got.FailedConnIDs) != 0 || got.Reason != "" {
		t.Fatalf("empty delivery = %+v, %v", got, err)
	}
}
