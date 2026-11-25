package framework_test

import (
	"context"
	"testing"
	"time"

	"github.com/yinjiaming666/wskit"
)

func TestRedisIsRequired(t *testing.T) {
	nodeConfig := wskit.NodeConfig{EndpointID: "redis-required", GRPCAddr: "127.0.0.1:0"}
	gatewayConfig := wskit.GatewayConfig{PublicURL: "wss://ws.example.org"}
	for _, backend := range []*wskit.Backend{nil, new(wskit.Backend)} {
		if _, err := wskit.CreateNode(nodeConfig, backend); err == nil {
			t.Fatal("created a node without a Redis backend")
		}
		if _, err := wskit.CreateGateway(gatewayConfig, backend); err == nil {
			t.Fatal("created a gateway without a Redis backend")
		}
		if _, err := wskit.NewSender(backend); err == nil {
			t.Fatal("created a sender without a Redis backend")
		}
	}

	backend := wskit.NewRedisBackend(wskit.RedisConfig{Addr: "127.0.0.1:0"})
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := backend.Ready(ctx); err == nil {
		t.Fatal("closed Redis backend reported ready")
	}
	node, err := wskit.CreateNode(nodeConfig, backend)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Run(ctx); err == nil || node.Ready() {
		t.Fatalf("node without Redis was ready or started successfully: %v", err)
	}
	sender, err := wskit.NewSender(backend)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	if err := sender.SendToChannel(ctx, "group:42", wskit.Message{Frame: wskit.Frame{Type: wskit.TextFrame, Data: []byte("hello")}}); err == nil {
		t.Fatal("channel publish succeeded without Redis")
	}
}
