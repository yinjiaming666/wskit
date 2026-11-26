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
	"github.com/yinjiaming666/wskit/internal/channel"
	"github.com/yinjiaming666/wskit/internal/endpoint"
	"github.com/yinjiaming666/wskit/internal/redisclient"
	"github.com/yinjiaming666/wskit/internal/route"
)

type runningNode struct {
	node   *wskit.Node
	server *httptest.Server
}

const testChannel = "group:42"

func startNode(t *testing.T, id string, b *wskit.Backend) runningNode {
	t.Helper()
	node, err := wskit.CreateNode(wskit.NodeConfig{EndpointID: id, GRPCAddr: "127.0.0.1:0", OnMessage: testHostProtocol}, b)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		info := wskit.SessionInfo{ConnID: r.Header.Get("X-Test-Device")}
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
func testHostProtocol(ctx context.Context, session *wskit.Session, f wskit.Frame) error {
	if f.Type == wskit.BinaryFrame {
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
	default:
		return session.Send(f)
	}
}

func textFrame(s string) wskit.Frame {
	return wskit.Frame{Type: wskit.TextFrame, Data: []byte(s)}
}

func sendFrame(t *testing.T, c *websocket.Conn, f wskit.Frame) {
	t.Helper()
	var wire any = f.Data
	if f.Type == wskit.TextFrame {
		wire = string(f.Data)
	}
	if err := websocket.Message.Send(c, wire); err != nil {
		t.Fatal(err)
	}
}

var frameReader = websocket.Codec{Unmarshal: func(data []byte, opcode byte, v any) error {
	*v.(*wskit.Frame) = wskit.Frame{Type: wskit.FrameType(opcode), Data: append([]byte(nil), data...)}
	return nil
}}

func wantFrame(t *testing.T, c *websocket.Conn, want wskit.Frame) wskit.Frame {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(4 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var got wskit.Frame
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
	config := wskit.RedisConfig{Addr: addr, KeyPrefix: "ws-test:[app]:" + rand.Text() + ":"}
	newBackend := func() *wskit.Backend {
		b := wskit.NewRedisBackend(config)
		t.Cleanup(func() { b.Close() })
		return b
	}
	// Use independent pools, just as separately deployed processes do.
	services := newBackend()
	state := redisclient.New(config)
	t.Cleanup(func() { state.Close() })

	ctx := context.Background()
	gateway, err := wskit.CreateGateway(wskit.GatewayConfig{PublicURL: "wss://ws.example.org"}, services)
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
	s, err := wskit.NewSender(services)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	phone := first.connect(t, "phone")
	web := second.connect(t, "web")
	tablet := second.connect(t, "tablet")

	packet := wskit.Frame{Type: wskit.BinaryFrame, Data: []byte{0, 255, 1}}
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
	if err := s.SendToChannel(ctx, testChannel, wskit.Message{Frame: packet}); err != nil {
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
	if err := s.SendToChannel(ctx, testChannel, wskit.Message{Frame: textFrame("two")}); err != nil {
		t.Fatal(err)
	}
	wantFrame(t, tablet, textFrame("two"))
	notice := wskit.Message{ID: "notice-id", Frame: wskit.Frame{Type: wskit.BinaryFrame, Data: []byte{0, 254, 0}}}
	if err := s.SendToChannel(ctx, testChannel, notice); err != nil {
		t.Fatal(err)
	}
	wantFrame(t, tablet, notice.Frame)

	if err := first.node.BeginDrain(ctx); err != nil {
		t.Fatal(err)
	}
	selected, err = gateway.Route(ctx, "alice")
	if err != nil || selected.EndpointID != "node-b" {
		t.Fatalf("route after drain %+v, %v", selected, err)
	}
	private := wskit.Frame{Type: wskit.BinaryFrame, Data: []byte("priv-1\x00\xff")}
	direct, err := s.Send(ctx, "alice", wskit.Message{Frame: private})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(direct.AcceptedConnIDs)
	if !direct.Accepted || !slices.Equal(direct.AcceptedConnIDs, []string{"phone", "web"}) {
		t.Fatalf("direct result: %+v", direct)
	}
	for _, conn := range []*websocket.Conn{phone, web} {
		wantFrame(t, conn, private)
	}
}
