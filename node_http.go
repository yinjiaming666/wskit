package socketserver

import (
	"context"
	"crypto/rand"
	"net/http"
	"strings"
	"time"

	"github.com/yinjiaming666/wskit/internal/httpx"
)

// nodeHandler 提供 WebSocket Node 的 HTTP 和 WebSocket 入口。
type nodeHandler struct {
	node *nodeCore
}

// newNodeHandler 创建 WebSocket Node HTTP 处理器。
func newNodeHandler(node *nodeCore) *nodeHandler {
	return &nodeHandler{node: node}
}

// ServeWS upgrades an HTTP request using the identity and subscriptions supplied by the host.
func (h *nodeHandler) ServeWS(w http.ResponseWriter, r *http.Request, info SessionInfo) {
	if r.Method != http.MethodGet {
		httpx.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		httpx.WriteError(w, http.StatusBadRequest, "websocket upgrade required")
		return
	}
	if err := validateUpgrade(r); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if info.ConnID == "" {
		info.ConnID = rand.Text()
	}
	client, err := h.node.RegisterSession(r.Context(), info)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = h.node.RemoveClientConnection(ctx, client)
	}()
	session := &Session{node: h.node, client: client}
	for _, channel := range info.Channels {
		if err := session.Subscribe(r.Context(), channel); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	h.serveWebSocket(w, r, client)
}

// serveWebSocket 完成 WebSocket 升级并启动读写循环。
func (h *nodeHandler) serveWebSocket(w http.ResponseWriter, r *http.Request, client *localClient) {
	conn, err := upgradeWebSocket(w, r)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	defer conn.close()

	client.AttachWriter(func(frame Frame) error { return conn.writeFrame(byte(frame.Type), frame.Data) })
	session := &Session{node: h.node, client: client}
	done := make(chan struct{})
	go h.writeLoop(conn, client, done)

	for {
		payload, opcode, err := conn.readMessage()
		if err != nil {
			return
		}
		switch opcode {
		case 0x8:
			_ = conn.writeClose()
			return
		case 0x9:
			_ = conn.writePong(payload)
		case 0xA:
			continue
		case 0x1, 0x2:
			if handle := h.node.config.OnMessage; handle != nil {
				if err := handle(r.Context(), session, Frame{Type: FrameType(opcode), Data: payload}); err != nil {
					_ = conn.writeClose()
					return
				}
			}
		}
		select {
		case <-done:
			return
		default:
		}
	}
}

// writeLoop 写消息并定期发送服务端 ping。
func (h *nodeHandler) writeLoop(conn *wsConn, client *localClient, done chan<- struct{}) {
	defer close(done)
	defer conn.close()
	defer client.Close()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-client.Done():
			return
		case payload := <-client.Send:
			if err := client.WriteDirect(payload); err != nil {
				return
			}
		case <-ticker.C:
			if err := conn.writePing([]byte("ping")); err != nil {
				return
			}
		}
	}
}
