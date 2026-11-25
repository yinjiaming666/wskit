package main

import (
	"context"
	"encoding/json"
	"flag"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yinjiaming666/wskit"
	demo "github.com/yinjiaming666/wskit/example"
	"github.com/yinjiaming666/wskit/example/internal/host"
)

func main() {
	if err := startServer(); err != nil {
		log.Fatal(err)
	}
}

func startServer() error {
	addr := flag.String("addr", "127.0.0.1:8080", "Gateway HTTP 监听地址")
	origin := flag.String("ws-origin", "ws://127.0.0.1:8081", "客户端可访问的 WebSocket 地址")
	flag.Parse()

	backend := socketserver.NewRedisBackend(socketserver.RedisConfig{Addr: os.Getenv("REDIS_ADDR"), KeyPrefix: "socket-demo:"})
	defer backend.Close()
	gateway, err := socketserver.CreateGateway(socketserver.GatewayConfig{PublicURL: *origin}, backend)
	if err != nil {
		return err
	}

	assets, err := fs.Sub(demo.ClientFiles, "client")
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("GET /", http.FileServerFS(assets))
	mux.HandleFunc("GET /route", func(w http.ResponseWriter, r *http.Request) {
		user := r.URL.Query().Get("user")
		route, err := gateway.Route(r.Context(), user)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		// 本机演示通过 URL 将测试身份带给 Node。
		route.WSURL += "?user=" + url.QueryEscape(user)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(route)
	})
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server := &http.Server{
		Addr: *addr, Handler: mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	log.Printf("gateway http listening on %s", *addr)
	return host.ServeHTTP(ctx, server)
}
