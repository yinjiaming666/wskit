package socketserver

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	websocketpb "github.com/yinjiaming666/wskit/api/proto/websocket"
)

// The listener runs entirely in memory and lets the test fail Accept deterministically.
type grpcFailureListener struct {
	fail       chan error
	closed     chan struct{}
	accepting  chan struct{}
	closeOnce  sync.Once
	acceptOnce sync.Once
}

func newGRPCFailureListener() *grpcFailureListener {
	return &grpcFailureListener{fail: make(chan error, 1), closed: make(chan struct{}), accepting: make(chan struct{})}
}

func (l *grpcFailureListener) Accept() (net.Conn, error) {
	l.acceptOnce.Do(func() { close(l.accepting) })
	select {
	case err := <-l.fail:
		return nil, err
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *grpcFailureListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}

func (*grpcFailureListener) Addr() net.Addr { return &net.TCPAddr{} }

func newGRPCFailureNode() *nodeCore {
	return newTestNode(NodeConfig{EndpointID: "grpc-failure-node", GRPCAddr: "in-memory", MaxConn: 10})
}

func waitGRPCTestSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for gRPC lifecycle signal")
	}
}

func assertNoGRPCFailure(t *testing.T, node *nodeCore) {
	t.Helper()
	select {
	case <-node.GRPCFailure():
		t.Fatalf("normal shutdown reported failure: %v", node.GRPCFailureCause())
	default:
	}
	if err := node.GRPCFailureCause(); err != nil {
		t.Fatalf("normal shutdown retained failure: %v", err)
	}
}

func assertGRPCReadiness(t *testing.T, node *nodeCore, want bool) {
	t.Helper()
	if got := node.Ready(); got != want {
		t.Fatalf("readiness = %v, want %v", got, want)
	}
}

func TestGRPCServeFailureStopsAdmissionAndPreservesCause(t *testing.T) {
	node := newGRPCFailureNode()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer node.Stop(context.Background())
	listener := newGRPCFailureListener()
	defer listener.Close()

	assertGRPCReadiness(t, node, false)
	if err := node.startGRPCServer(ctx, listener); err != nil {
		t.Fatal(err)
	}
	waitGRPCTestSignal(t, listener.accepting)
	assertGRPCReadiness(t, node, true)
	if _, err := node.RegisterSession(ctx, SessionInfo{ConnID: "conn-before-failure", UserID: "user-1", Platform: "test"}); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("injected Accept failure")
	listener.fail <- sentinel
	waitGRPCTestSignal(t, node.GRPCFailure())

	if !errors.Is(node.GRPCFailureCause(), sentinel) {
		t.Fatalf("failure cause = %v, want wrapped Accept failure", node.GRPCFailureCause())
	}
	assertGRPCReadiness(t, node, false)
	if _, err := node.RegisterSession(ctx, SessionInfo{ConnID: "conn-after-failure", UserID: "user-1", Platform: "test"}); err == nil || !strings.Contains(err.Error(), "draining") {
		t.Fatalf("new connection after gRPC failure: %v", err)
	}
	if err := node.HeartbeatOnce(ctx, "online"); err != nil {
		t.Fatal(err)
	}
	state, err := node.endpointRepo.GetEndpoint(ctx, "grpc-failure-node")
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != "draining" {
		t.Fatalf("endpoint status = %q, want draining", state.Status)
	}
	health, err := (&grpcPushServer{node: node}).Health(ctx, &websocketpb.HealthRequest{EndpointId: "grpc-failure-node"})
	if err != nil || health.GetStatus() != "draining" {
		t.Fatalf("gRPC health after failure = %v, %v", health, err)
	}
	replacement := newGRPCFailureListener()
	defer replacement.Close()
	if err := node.startGRPCServer(ctx, replacement); err == nil {
		t.Fatal("failed node started a second gRPC server")
	}
	if !errors.Is(node.GRPCFailureCause(), sentinel) {
		t.Fatalf("second start replaced original failure: %v", node.GRPCFailureCause())
	}
}

func TestGRPCNormalShutdownDoesNotReportFailure(t *testing.T) {
	for _, mode := range []string{"context", "node-stop"} {
		t.Run(mode, func(t *testing.T) {
			node := newGRPCFailureNode()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			defer node.Stop(context.Background())
			listener := newGRPCFailureListener()
			defer listener.Close()
			if err := node.startGRPCServer(ctx, listener); err != nil {
				t.Fatal(err)
			}
			waitGRPCTestSignal(t, listener.accepting)
			if mode == "context" {
				cancel()
			} else if err := node.Stop(context.Background()); err != nil {
				t.Fatal(err)
			}
			waitGRPCTestSignal(t, listener.closed)
			assertGRPCReadiness(t, node, false)
			assertNoGRPCFailure(t, node)
		})
	}
}

func TestGRPCDrainKeepsListenerAliveAndReportsHealth(t *testing.T) {
	node := newGRPCFailureNode()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer node.Stop(context.Background())
	listener := newGRPCFailureListener()
	defer listener.Close()
	if err := node.startGRPCServer(ctx, listener); err != nil {
		t.Fatal(err)
	}
	waitGRPCTestSignal(t, listener.accepting)
	health, err := (&grpcPushServer{node: node}).Health(ctx, &websocketpb.HealthRequest{})
	if err != nil || health.GetStatus() != "online" {
		t.Fatalf("initial gRPC health = %v, %v", health, err)
	}
	if err := node.BeginDrain(ctx); err != nil {
		t.Fatal(err)
	}
	assertGRPCReadiness(t, node, false)
	select {
	case <-listener.closed:
		t.Fatal("deployment drain stopped gRPC before existing connections were drained")
	default:
	}
	assertNoGRPCFailure(t, node)
	health, err = (&grpcPushServer{node: node}).Health(ctx, &websocketpb.HealthRequest{})
	if err != nil || health.GetStatus() != "draining" {
		t.Fatalf("drained gRPC health = %v, %v", health, err)
	}
}

func TestGRPCServerStartsOnlyOnceAndRejectsCanceledContext(t *testing.T) {
	node := newGRPCFailureNode()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	listener := newGRPCFailureListener()
	defer listener.Close()
	if err := node.startGRPCServer(ctx, listener); !errors.Is(err, context.Canceled) {
		t.Fatalf("start with canceled context = %v", err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	defer node.Stop(context.Background())
	if err := node.startGRPCServer(ctx, listener); err != nil {
		t.Fatal(err)
	}
	waitGRPCTestSignal(t, listener.accepting)
	second := newGRPCFailureListener()
	defer second.Close()
	if err := node.startGRPCServer(ctx, second); err == nil {
		t.Fatal("node allowed duplicate gRPC start")
	}
	if !node.Ready() {
		t.Fatal("duplicate start disabled healthy original server")
	}
	assertNoGRPCFailure(t, node)
}
