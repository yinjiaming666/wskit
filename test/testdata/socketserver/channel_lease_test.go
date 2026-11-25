package socketserver

import (
	"context"
	"slices"
	"testing"

	"github.com/yinjiaming666/wskit/internal/channel"
)

type leaseChange struct {
	action, channelID, endpoint string
}

type channelLeases struct {
	channel.Registry
	changes []leaseChange
}

func (r *channelLeases) RegisterNode(_ context.Context, channelID, endpoint string) error {
	r.changes = append(r.changes, leaseChange{"register", channelID, endpoint})
	return nil
}

func (r *channelLeases) UnregisterNode(_ context.Context, channelID, endpoint string) error {
	r.changes = append(r.changes, leaseChange{"unregister", channelID, endpoint})
	return nil
}

// TestNodeMaintainsLeaseForActiveChannel 验证频道租约随首个订阅建立并随最后一个订阅结束撤销。
func TestNodeMaintainsLeaseForActiveChannel(t *testing.T) {
	ctx := context.Background()
	registry := &channelLeases{}
	node := newTestNode(NodeConfig{EndpointID: "node-live", MaxConn: 10, SendBuffer: 8})
	node.SetChannelRegistry(registry)

	first, err := node.RegisterSession(ctx, SessionInfo{ConnID: "conn-live-1", UserID: "user-live", DeviceID: "device-1", Platform: "test"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := node.RegisterSession(ctx, SessionInfo{ConnID: "conn-live-2", UserID: "user-live", DeviceID: "device-2", Platform: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Subscribe(ctx, first.ConnID, "channel-live"); err != nil {
		t.Fatal(err)
	}
	joined := []leaseChange{{"register", "channel-live", "node-live"}}
	if !slices.Equal(registry.changes, joined) {
		t.Fatalf("lease changes after first join: %v", registry.changes)
	}
	if err := node.Subscribe(ctx, second.ConnID, "channel-live"); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(registry.changes, joined) {
		t.Fatalf("lease changes after two joins: %v", registry.changes)
	}

	if err := node.Unsubscribe(ctx, first.ConnID, "channel-live"); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(registry.changes, joined) {
		t.Fatalf("lease changed before the last member left: %v", registry.changes)
	}

	if err := node.RemoveClientConnection(ctx, second); err != nil {
		t.Fatal(err)
	}
	want := append(joined, leaseChange{"unregister", "channel-live", "node-live"})
	if !slices.Equal(registry.changes, want) {
		t.Fatalf("lease changes after last member left: %v, want %v", registry.changes, want)
	}
	if err := node.RemoveClientConnection(ctx, first); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(registry.changes, want) {
		t.Fatalf("lease changes after disconnect: %v, want %v", registry.changes, want)
	}
}

// TestChannelIDFromRedisTopic 验证节点频道解析不会接受其它节点的消息。
func TestChannelIDFromRedisTopic(t *testing.T) {
	cases := []struct {
		name     string
		topic    string
		endpoint string
		expected string
	}{
		{
			name:     "pubsub",
			topic:    "live:node:node-a:channel:channel-1",
			endpoint: "node-a",
			expected: "channel-1",
		},
		{
			name:     "channel with namespace",
			topic:    "live:node:node-a:channel:group:42",
			endpoint: "node-a",
			expected: "group:42",
		},
		{
			name:     "other endpoint",
			topic:    "live:node:node-b:channel:channel-3",
			endpoint: "node-a",
			expected: "",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if actual := channelIDFromRedisTopic(testCase.topic, testCase.endpoint); actual != testCase.expected {
				t.Fatalf("expected %q, got %q", testCase.expected, actual)
			}
		})
	}
}
