package framework_test

import (
	"context"
	"testing"
	"time"

	"github.com/yinjiaming666/wskit"
)

func TestRedisIsRequired(t *testing.T) {
	nodeConfig := socketserver.NodeConfig{EndpointID: "redis-required", GRPCAddr: "127.0.0.1:0"}
	gatewayConfig := socketserver.GatewayConfig{PublicURL: "wss://ws.example.org"}
	for _, backend := range []*socketserver.Backend{nil, new(socketserver.Backend)} {
		if _, err := socketserver.CreateNode(nodeConfig, backend); err == nil {
			t.Fatal("created a node without a Redis backend")
		}
		if _, err := socketserver.CreateGateway(gatewayConfig, backend); err == nil {
			t.Fatal("created a gateway without a Redis backend")
		}
		if _, err := socketserver.NewSender(backend); err == nil {
			t.Fatal("created a sender without a Redis backend")
		}
	}

	backend := socketserver.NewRedisBackend(socketserver.RedisConfig{Addr: "127.0.0.1:0"})
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := backend.Ready(ctx); err == nil {
		t.Fatal("closed Redis backend reported ready")
	}
	node, err := socketserver.CreateNode(nodeConfig, backend)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Run(ctx); err == nil || node.Ready() {
		t.Fatalf("node without Redis was ready or started successfully: %v", err)
	}
	sender, err := socketserver.NewSender(backend)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	if err := sender.SendToChannel(ctx, "group:42", socketserver.Message{Frame: socketserver.Frame{Type: socketserver.TextFrame, Data: []byte("hello")}}); err == nil {
		t.Fatal("channel publish succeeded without Redis")
	}
}
