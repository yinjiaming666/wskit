package wskit

import (
	"context"
	"testing"
)

func TestNodeReadinessReflectsLifecycle(t *testing.T) {
	core := newTestNode(NodeConfig{EndpointID: "node-health", MaxConn: 10, SendBuffer: 8})
	node := &Node{core: core}
	if node.Ready() {
		t.Fatal("node reported ready before Run activated it")
	}
	node.active = true
	if !node.Ready() {
		t.Fatal("active node reported unready")
	}
	if err := node.BeginDrain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if node.Ready() {
		t.Fatal("draining node reported ready")
	}
	node.active = false
	if err := node.BeginDrain(context.Background()); err == nil {
		t.Fatal("inactive node accepted drain")
	}
}
