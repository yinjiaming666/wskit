package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/yinjiaming666/wskit"
	demo "github.com/yinjiaming666/wskit/example"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	user := flag.String("user", "alice", "接收用户")
	text := flag.String("text", "你好，Alice", "私聊内容")
	flag.Parse()

	backend := socketserver.NewRedisBackend(socketserver.RedisConfig{Addr: os.Getenv("REDIS_ADDR"), KeyPrefix: "socket-demo:"})
	defer backend.Close()
	sender, err := socketserver.NewSender(backend)
	if err != nil {
		return err
	}
	defer sender.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := sender.Send(ctx, *user, demo.TextMessage(*text, rand.Text()))
	if err != nil {
		return err
	}
	if !result.Accepted {
		return fmt.Errorf("推送失败 %v: %s", result.FailedConnIDs, result.Reason)
	}
	fmt.Printf("用户 %s，消息 %s，已入队 %d 个连接\n", *user, result.MessageID, len(result.AcceptedConnIDs))
	return nil
}
