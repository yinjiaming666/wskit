package socketserver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"google.golang.org/grpc"

	websocketpb "github.com/yinjiaming666/wskit/api/proto/websocket"
	"github.com/yinjiaming666/wskit/internal/grpcpush"
)

// grpcPushServer 适配 WebSocket Node 到 protobuf gRPC 服务接口。
type grpcPushServer struct {
	websocketpb.UnimplementedWebSocketPushServiceServer
	node *nodeCore
}

// PushToConns 按连接推送消息。
func (s *grpcPushServer) PushToConns(ctx context.Context, req *websocketpb.PushToConnsRequest) (*websocketpb.PushResponse, error) {
	resp, err := s.node.PushToConns(ctx, req.GetConnIds(), grpcpush.MessageFromProto(req.GetMessage()))
	if err != nil {
		return nil, err
	}
	return grpcpush.PushResponseToProto(resp), nil
}

// PushToUsers 按用户推送消息。
func (s *grpcPushServer) PushToUsers(ctx context.Context, req *websocketpb.PushToUsersRequest) (*websocketpb.PushResponse, error) {
	resp, err := s.node.PushToUsers(ctx, req.GetUserIds(), grpcpush.MessageFromProto(req.GetMessage()))
	if err != nil {
		return nil, err
	}
	return grpcpush.PushResponseToProto(resp), nil
}

// Health 返回节点健康状态。
func (s *grpcPushServer) Health(ctx context.Context, req *websocketpb.HealthRequest) (*websocketpb.HealthResponse, error) {
	status := "online"
	if !s.node.Ready() {
		status = "draining"
	}
	return &websocketpb.HealthResponse{EndpointId: req.GetEndpointId(), Status: status, ConnCount: s.node.ConnCount()}, nil
}

// StartGRPCServer 启动 WebSocket Node 内部 gRPC 推送服务。
func (n *nodeCore) StartGRPCServer(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n.mu.RLock()
	addr := n.config.GRPCAddr
	n.mu.RUnlock()
	if addr == "" {
		return nil
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen WebSocket gRPC endpoint=%s address=%s: %w", n.config.EndpointID, addr, err)
	}
	if err := n.startGRPCServer(ctx, listener); err != nil {
		_ = listener.Close()
		return err
	}
	return nil
}

// GRPCFailure 在内部推送监听意外结束时通知主流程；正常关闭不会触发。
func (n *nodeCore) GRPCFailure() <-chan struct{} {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.grpcFailure
}

// GRPCFailureCause 保留首次监听故障，供退出清理与诊断使用。
func (n *nodeCore) GRPCFailureCause() error {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.grpcFailureCause
}

func (n *nodeCore) startGRPCServer(ctx context.Context, listener net.Listener) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if listener == nil {
		return errors.New("WebSocket gRPC listener is nil")
	}
	server := grpc.NewServer()
	websocketpb.RegisterWebSocketPushServiceServer(server, &grpcPushServer{node: n})
	n.mu.Lock()
	if n.grpcStarted {
		n.mu.Unlock()
		return errors.New("WebSocket gRPC server is already started")
	}
	select {
	case <-n.stopCh:
		n.mu.Unlock()
		return errors.New("WebSocket node is stopped")
	default:
	}
	if n.stopping {
		n.mu.Unlock()
		return errors.New("WebSocket node is draining")
	}
	if err := ctx.Err(); err != nil {
		n.mu.Unlock()
		return err
	}
	if n.grpcFailure == nil {
		n.grpcFailure = make(chan struct{})
	}
	// A port of zero asks the OS to allocate a port. Advertise the bound port to senders.
	if tcpAddr, ok := listener.Addr().(*net.TCPAddr); ok {
		if host, port, err := net.SplitHostPort(n.config.GRPCAddr); err == nil && port == "0" {
			n.config.GRPCAddr = net.JoinHostPort(host, strconv.Itoa(tcpAddr.Port))
		}
	}
	n.workers.Add(1)
	n.grpcStarted = true
	n.grpcReady = true
	n.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer n.workers.Done()
		select {
		case <-ctx.Done():
		case <-n.stopCh:
		case <-done:
			// Serve may fail while existing transports are still open.
			server.Stop()
			return
		}
		n.mu.Lock()
		n.grpcReady = false
		n.mu.Unlock()
		graceful := stopGRPCServer(server, 5*time.Second)
		_ = listener.Close()
		if graceful {
			<-done
		}
	}()
	go func() {
		log.Printf("websocket node %s grpc listening on %s", n.config.EndpointID, listener.Addr())
		err := server.Serve(listener)
		n.mu.Lock()
		n.grpcReady = false
		stopping := false
		select {
		case <-n.stopCh:
			stopping = true
		default:
		}
		var failure error
		if ctx.Err() == nil && !stopping {
			if err == nil {
				err = errors.New("gRPC Serve returned without an error")
			}
			failure = fmt.Errorf("WebSocket gRPC serve endpoint=%s address=%s: %w", n.config.EndpointID, n.config.GRPCAddr, err)
			n.stopping = true
			if n.grpcFailureCause == nil {
				n.grpcFailureCause = failure
				close(n.grpcFailure)
			}
		}
		n.mu.Unlock()
		close(done)
		if failure != nil {
			log.Printf("websocket node grpc stopped: %v", failure)
		}
	}()
	return nil
}

// stopGRPCServer reports whether all RPCs finished within the grace period.
func stopGRPCServer(server *grpc.Server, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		// gRPC v1.65 may hold its server mutex in GracefulStop while waiting for a
		// handler after the transports have closed. Stop and Serve can both block
		// on that handler, so the node must not join them on the forced path.
		go server.Stop()
		return false
	}
}
