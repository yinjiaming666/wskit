package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yinjiaming666/wskit"
	"github.com/yinjiaming666/wskit/example/internal/host"
)

func main() {
	if err := startNode(); err != nil {
		log.Fatal(err)
	}
}

func startNode() error {
	addr := flag.String("addr", "127.0.0.1:8081", "Node WebSocket 监听地址")
	grpcAddr := flag.String("grpc", "127.0.0.1:9001", "节点间推送的 gRPC 地址")
	flag.Parse()

	backend := wskit.NewRedisBackend(wskit.RedisConfig{Addr: os.Getenv("REDIS_ADDR"), KeyPrefix: "socket-demo:"})
	defer backend.Close()
	sender, err := wskit.NewSender(backend)
	if err != nil {
		return err
	}
	defer sender.Close()

	const wsPath = "/ws/demo"
	node, err := wskit.CreateNode(wskit.NodeConfig{
		EndpointID: "demo",
		WSPath:     wsPath,
		WSAddr:     *addr,
		GRPCAddr:   *grpcAddr,
		OnMessage:  chatHandler(sender),
	}, backend)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	registerHealthRoutes(mux, node, backend.Ready)
	mux.HandleFunc("GET "+wsPath, func(w http.ResponseWriter, r *http.Request) {
		node.ServeWS(w, r, wskit.SessionInfo{
			UserID: r.URL.Query().Get("user"), Channels: []string{"lobby"},
		})
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	nodeDone := make(chan error, 1)
	go func() {
		nodeDone <- node.Run(ctx)
		stop()
	}()
	server := &http.Server{
		Addr: *addr, Handler: mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	log.Printf("node http listening on %s", *addr)
	httpErr := host.ServeHTTP(ctx, server)
	stop()
	return errors.Join(httpErr, <-nodeDone)
}
