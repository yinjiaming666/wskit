package main

import (
	"context"
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
	channel := flag.String("channel", "lobby", "目标频道")
	text := flag.String("text", "大家好", "广播内容")
	flag.Parse()

	backend := wskit.NewRedisBackend(wskit.RedisConfig{Addr: os.Getenv("REDIS_ADDR"), KeyPrefix: "socket-demo:"})
	defer backend.Close()
	sender, err := wskit.NewSender(backend)
	if err != nil {
		return err
	}
	defer sender.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = sender.SendToChannel(ctx, *channel, demo.TextMessage(*text, ""))
	if err != nil {
		return err
	}
	fmt.Printf("已发布到频道 %s\n", *channel)
	return nil
}
