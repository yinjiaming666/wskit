package socketserver

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestGRPCForcedShutdownDoesNotWaitForHandlerIgnoringCancellation(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	defer listener.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	server := grpc.NewServer(grpc.UnknownServiceHandler(func(_ interface{}, _ grpc.ServerStream) error {
		close(entered)
		<-release
		return nil
	}))
	defer func() {
		close(release)
		server.Stop()
	}()
	go func() { _ = server.Serve(listener) }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	connection, err := grpc.DialContext(ctx, "passthrough:///in-memory", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	go func() { _ = connection.Invoke(ctx, "/test/Block", &emptypb.Empty{}, &emptypb.Empty{}) }()
	waitGRPCTestSignal(t, entered)
	stopped := make(chan struct{})
	go func() { stopGRPCServer(server, 20*time.Millisecond); close(stopped) }()
	waitGRPCTestSignal(t, stopped)
}
