package framework_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/websocket"

	"github.com/yinjiaming666/wskit"
	"github.com/yinjiaming666/wskit/internal/ack"
	"github.com/yinjiaming666/wskit/internal/channel"
	"github.com/yinjiaming666/wskit/internal/endpoint"
	"github.com/yinjiaming666/wskit/internal/redisclient"
	"github.com/yinjiaming666/wskit/internal/route"
)

type runningNode struct {
	node   *socketserver.Node
	server *httptest.Server
}

const testChannel = "group:42"

func startNode(t *testing.T, id string, b *socketserver.Backend) runningNode {
	t.Helper()
	node, err := socketserver.CreateNode(socketserver.NodeConfig{EndpointID: id, GRPCAddr: "127.0.0.1:0", OnMessage: testHostProtocol}, b)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		info := socketserver.SessionInfo{ConnID: r.Header.Get("X-Test-Device")}
		switch info.ConnID {
		case "phone", "web":
			info.UserID = "alice"
		case "tablet":
			info.UserID = "bob"
			info.Channels = []string{testChannel}
		default:
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		node.ServeWS(w, r, info)
	}))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error
	go func() { defer close(done); runErr = node.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
			if runErr != nil {
				t.Error(runErr)
			}
		case <-time.After(6 * time.Second):
			t.Error("node did not stop")
		}
		srv.Close()
	})
	deadline := time.Now().Add(5 * time.Second)
	for !node.Ready() {
		select {
		case <-done:
			t.Fatalf("node stopped at startup: %v", runErr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("node not ready")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return runningNode{node: node, server: srv}
}

func (n runningNode) connect(t *testing.T, device string) *websocket.Conn {
	t.Helper()
	cfg, err := websocket.NewConfig("ws"+strings.TrimPrefix(n.server.URL, "http"), n.server.URL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Header.Set("X-Test-Device", device)
	c, err := websocket.DialConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// This host uses plain-text membership commands and binary echo. The library
// must not interpret even an input that looks like the standalone JSON protocol.
func testHostProtocol(ctx context.Context, session *socketserver.Session, f socketserver.Frame) error {
	if f.Type == socketserver.BinaryFrame {
		return session.Send(f)
	}
	cmd, arg, _ := strings.Cut(string(f.Data), " ")
	switch cmd {
	case "follow":
		if session.UserID() != "alice" || arg != testChannel {
			return session.Send(textFrame("denied"))
		}
		if err := session.Subscribe(ctx, arg); err != nil {
			return err
		}
		return session.Send(textFrame("followed"))
	case "leave":
		if err := session.Unsubscribe(ctx, testChannel); err != nil {
			return err
		}
		return session.Send(textFrame("left"))
	case "seen":
		if err := session.Ack(ctx, arg); err != nil {
			return err
		}
		return session.Send(textFrame("recorded"))
	default:
		return session.Send(f)
	}
}

func textFrame(s string) socketserver.Frame {
	return socketserver.Frame{Type: socketserver.TextFrame, Data: []byte(s)}
}

func sendFrame(t *testing.T, c *websocket.Conn, f socketserver.Frame) {
	t.Helper()
	var wire any = f.Data
	if f.Type == socketserver.TextFrame {
		wire = string(f.Data)
	}
	if err := websocket.Message.Send(c, wire); err != nil {
		t.Fatal(err)
	}
}

var frameReader = websocket.Codec{Unmarshal: func(data []byte, opcode byte, v any) error {
	*v.(*socketserver.Frame) = socketserver.Frame{Type: socketserver.FrameType(opcode), Data: append([]byte(nil), data...)}
	return nil
}}

func wantFrame(t *testing.T, c *websocket.Conn, want socketserver.Frame) socketserver.Frame {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(4 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var got socketserver.Frame
	if err := frameReader.Receive(c, &got); err != nil {
		t.Fatal(err)
	}
	if got.Type != want.Type || !bytes.Equal(got.Data, want.Data) {
		t.Fatalf("wire frame = %#v, want %#v", got, want)
	}
	return got
}

func TestChannelAndDirectDelivery(t *testing.T) {
	addr := os.Getenv("WS_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set WS_TEST_REDIS_ADDR to run the two-node Redis/gRPC test")
	}
	config := socketserver.RedisConfig{Addr: addr, KeyPrefix: "ws-test:[app]:" + rand.Text() + ":"}
	newBackend := func() *socketserver.Backend {
		b := socketserver.NewRedisBackend(config)
		t.Cleanup(func() { b.Close() })
		return b
	}
	// Use independent pools, just as separately deployed processes do.
	services := newBackend()
	state := redisclient.New(config)
	t.Cleanup(func() { state.Close() })

	ctx := context.Background()
	gateway, err := socketserver.CreateGateway(socketserver.GatewayConfig{PublicURL: "wss://ws.example.org"}, services)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.Route(ctx, "alice"); err == nil {
		t.Fatal("empty cluster supplied a node")
	}
	first := startNode(t, "region/node[a]", newBackend())
	selected, err := gateway.Route(ctx, "alice")
	if err != nil || selected.EndpointID != "region/node[a]" || selected.WSURL != "wss://ws.example.org/ws/region%2Fnode%5Ba%5D" {
		t.Fatalf("route %+v, %v", selected, err)
	}
	second := startNode(t, "node-b", newBackend())
	endpoints, err := endpoint.NewRedisRepository(state).ListEndpoints(ctx)
	if err != nil || len(endpoints) != 2 {
		t.Fatalf("registered endpoints %v, %v", endpoints, err)
	}
	s, err := socketserver.NewSender(services)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	phone := first.connect(t, "phone")
	web := second.connect(t, "web")
	tablet := second.connect(t, "tablet")

	packet := socketserver.Frame{Type: socketserver.BinaryFrame, Data: []byte{0, 255, 1}}
	sendFrame(t, web, packet)
	wantFrame(t, web, packet)
	looksLikeJSON := textFrame(`{"c":"socket_server","m":"ping","data":{}}`)
	sendFrame(t, web, looksLikeJSON)
	wantFrame(t, web, looksLikeJSON)
	sendFrame(t, phone, textFrame("follow unrelated"))
	wantFrame(t, phone, textFrame("denied"))
	sendFrame(t, phone, textFrame("follow "+testChannel))
	wantFrame(t, phone, textFrame("followed"))
	nodes, err := channel.NewRedisRegistry(state).ListChannelNodes(ctx, testChannel)
	if err != nil || !slices.Equal(nodes, []string{"node-b", "region/node[a]"}) {
		t.Fatalf("channel nodes %v, %v", nodes, err)
	}
	devices, err := route.NewRedisResolver(state).GetUserConnIDs(ctx, "alice")
	slices.Sort(devices)
	if err != nil || !slices.Equal(devices, []string{"phone", "web"}) || second.node.ConnCount() != 2 {
		t.Fatalf("user connections %v, %v", devices, err)
	}
	if err := s.SendToChannel(ctx, testChannel, socketserver.Message{Frame: packet}); err != nil {
		t.Fatal(err)
	}
	wantFrame(t, phone, packet)
	wantFrame(t, tablet, packet)
	sendFrame(t, phone, textFrame("leave"))
	wantFrame(t, phone, textFrame("left"))
	nodes, err = state.Strings(ctx, "ZRANGE", state.Key("live:channel:"+testChannel+":nodes"), "0", "-1")
	if err != nil || !slices.Equal(nodes, []string{"node-b"}) {
		t.Fatalf("channel node leases after unsubscribe %v, %v", nodes, err)
	}
	channels, err := state.Strings(ctx, "SMEMBERS", state.Key("live:node:node-b:channels"))
	if err != nil || !slices.Equal(channels, []string{testChannel}) {
		t.Fatalf("channels indexed for node-b %v, %v", channels, err)
	}
	if err := s.SendToChannel(ctx, testChannel, socketserver.Message{Frame: textFrame("two")}); err != nil {
		t.Fatal(err)
	}
	wantFrame(t, tablet, textFrame("two"))
	notice := socketserver.Message{ID: "notice-id", Frame: socketserver.Frame{Type: socketserver.BinaryFrame, Data: []byte{0, 254, 0}}}
	if err := s.SendToChannel(ctx, testChannel, notice); err != nil {
		t.Fatal(err)
	}
	wantFrame(t, tablet, notice.Frame)
	if pending, err := s.PendingConnections(ctx, notice.ID); err != nil || len(pending) != 0 {
		t.Fatalf("channel broadcast has pending receipts %v, error %v", pending, err)
	}

	if err := first.node.BeginDrain(ctx); err != nil {
		t.Fatal(err)
	}
	selected, err = gateway.Route(ctx, "alice")
	if err != nil || selected.EndpointID != "node-b" {
		t.Fatalf("route after drain %+v, %v", selected, err)
	}
	private := socketserver.Frame{Type: socketserver.BinaryFrame, Data: []byte("priv-1\x00\xff")}
	direct, err := s.Send(ctx, "alice", socketserver.Message{ID: "priv-1", Frame: private})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(direct.AcceptedConnIDs)
	if !direct.Accepted || !slices.Equal(direct.AcceptedConnIDs, []string{"phone", "web"}) {
		t.Fatalf("direct result: %+v", direct)
	}
	for _, conn := range []*websocket.Conn{phone, web} {
		received := wantFrame(t, conn, private)
		id, _, _ := bytes.Cut(received.Data, []byte{0})
		sendFrame(t, conn, textFrame("seen "+string(id)))
		wantFrame(t, conn, textFrame("recorded"))
	}
	acked, err := s.AckedConnections(ctx, direct.MessageID)
	slices.Sort(acked)
	if err != nil || !slices.Equal(acked, direct.AcceptedConnIDs) {
		t.Fatalf("ACKs %v, %v", acked, err)
	}
	// A single user's devices can acknowledge the same direct message independently.
	partial, err := s.Send(ctx, "alice", socketserver.Message{ID: "partial-1", Frame: textFrame("second private message")})
	if err != nil || !partial.Accepted {
		t.Fatalf("partial delivery %+v, %v", partial, err)
	}
	for _, conn := range []*websocket.Conn{phone, web} {
		wantFrame(t, conn, textFrame("second private message"))
	}
	sendFrame(t, web, textFrame("seen "+partial.MessageID))
	wantFrame(t, web, textFrame("recorded"))
	pending, err := s.PendingConnections(ctx, partial.MessageID)
	if err != nil || !slices.Equal(pending, []string{"phone"}) {
		t.Fatalf("pending device ACKs %v, %v", pending, err)
	}

	// A late client receipt must leave a timed-out delivery in its terminal state.
	if err := ack.NewRedisStore(state).MarkFailed(ctx, partial.MessageID, "phone", "timeout"); err != nil {
		t.Fatal(err)
	}
	sendFrame(t, phone, textFrame("seen "+partial.MessageID))
	wantFrame(t, phone, textFrame("recorded"))
	failed, err := s.FailedConnections(ctx, partial.MessageID)
	if err != nil || !slices.Equal(failed, []string{"phone"}) {
		t.Fatalf("failed connections after late ACK %v, %v", failed, err)
	}
	acked, err = s.AckedConnections(ctx, partial.MessageID)
	if err != nil || !slices.Equal(acked, []string{"web"}) {
		t.Fatalf("confirmed connections after late ACK %v, %v", acked, err)
	}
}
