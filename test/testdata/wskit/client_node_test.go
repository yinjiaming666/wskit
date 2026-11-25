package wskit

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/yinjiaming666/wskit/internal/domain"
)

func TestClientCloseAndEnqueueAreSafe(t *testing.T) {
	client := newClient("conn-1", "user-1", "", "", 8)
	var group sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := 0; index < 1000; index++ {
				client.Enqueue(Frame{Type: TextFrame, Data: []byte("message")})
			}
		}()
	}
	client.Close()
	group.Wait()
	select {
	case <-client.Done():
	default:
		t.Fatal("client done channel was not closed")
	}
	if client.Enqueue(Frame{Type: TextFrame, Data: []byte("late")}) {
		t.Fatal("closed client accepted a message")
	}
}

func TestOldConnectionCannotRemoveReplacement(t *testing.T) {
	ctx := context.Background()
	node := newTestNode(NodeConfig{EndpointID: "node-1", MaxConn: 10, SendBuffer: 8})
	first, err := node.RegisterSession(ctx, SessionInfo{ConnID: "same-conn", UserID: "user-1", DeviceID: "device-1", Platform: "test"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := node.RegisterSession(ctx, SessionInfo{ConnID: "same-conn", UserID: "user-1", DeviceID: "device-2", Platform: "test"})
	if err != nil {
		t.Fatal(err)
	}
	replacement := &Session{node: node, client: second}
	if err := replacement.Subscribe(ctx, "group:42"); err != nil {
		t.Fatal(err)
	}
	old := &Session{node: node, client: first}
	if old.Subscribe(ctx, "another") == nil || old.Unsubscribe(ctx, "group:42") == nil {
		t.Fatal("replaced session changed the current connection's subscriptions")
	}
	if err := node.RemoveClientConnection(ctx, first); err != nil {
		t.Fatal(err)
	}
	if len(node.channelHub.ListConns("group:42")) != 1 || len(node.channelHub.ListConns("another")) != 0 {
		t.Fatal("replacement subscriptions changed")
	}
	if info, err := node.routeResolver.GetConnInfo(ctx, second.ConnID); err != nil || info.DeviceID != second.DeviceID {
		t.Fatalf("replacement route = %#v, error = %v", info, err)
	}
	response, err := node.PushToConns(ctx, []string{"same-conn"}, &domain.Message{
		MessageID: "msg-1", Frame: Frame{Type: TextFrame, Data: []byte("ok")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !response.Accepted || len(response.AcceptedConnIDs) != 1 {
		t.Fatalf("replacement connection was not active: %#v", response)
	}
	if err := node.RemoveClientConnection(ctx, second); err != nil {
		t.Fatal(err)
	}
}

func TestNodePushReportsQueueAcceptanceBeforeWebSocketWrite(t *testing.T) {
	ctx := context.Background()
	core := newTestNode(NodeConfig{EndpointID: "delivery-node", SendBuffer: 1})
	local, err := core.RegisterSession(ctx, SessionInfo{ConnID: "online", UserID: "user"})
	if err != nil {
		t.Fatal(err)
	}
	defer core.RemoveClientConnection(ctx, local)
	// If a result started representing socket writes, this writer would fail the send.
	writes := 0
	local.AttachWriter(func(Frame) error {
		writes++
		return errors.New("socket write failed")
	})
	node := &Node{core: core}
	frame := Frame{Type: BinaryFrame, Data: []byte{0, 255}}
	var result *DeliveryResult
	result, err = node.PushToConns(ctx, []string{"online", "missing"}, frame)
	if err != nil || result == nil || result.Accepted || !slices.Equal(result.AcceptedConnIDs, []string{"online"}) || !slices.Equal(result.FailedConnIDs, []string{"missing"}) || result.Reason != "some connections failed" {
		t.Fatalf("partial enqueue = %+v, %v", result, err)
	}
	frame.Data[0] = 99
	queued := <-local.Send
	if writes != 0 || !bytes.Equal(queued.Data, []byte{0, 255}) {
		t.Fatalf("push wrote to socket or retained mutable caller data: writes=%d frame=%v", writes, queued)
	}
	result, err = node.PushToUsers(ctx, []string{"user"}, Frame{Type: TextFrame, Data: []byte("queued")})
	if err != nil || !result.Accepted || !slices.Equal(result.AcceptedConnIDs, []string{"online"}) {
		t.Fatalf("user enqueue = %+v, %v", result, err)
	}
	result, err = node.PushToConns(ctx, []string{"online"}, Frame{Type: TextFrame, Data: []byte("overflow")})
	if err != nil || result.Accepted || !slices.Equal(result.FailedConnIDs, []string{"online"}) || writes != 0 {
		t.Fatalf("full queue = %+v, %v; writes=%d", result, err, writes)
	}
}
