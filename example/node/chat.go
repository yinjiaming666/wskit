package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"

	"github.com/yinjiaming666/wskit"
	demo "github.com/yinjiaming666/wskit/example"
)

type command struct {
	Op     string `json:"op"`
	Target string `json:"target"`
	Text   string `json:"text"`
}

func chatHandler(sender *wskit.Sender) wskit.MessageHandler {
	return func(ctx context.Context, session *wskit.Session, frame wskit.Frame) error {
		respond := func(text string) error { return session.Send(demo.TextMessage(text, "").Frame) }
		var cmd command
		if err := json.Unmarshal(frame.Data, &cmd); err != nil {
			return respond("expected a JSON command")
		}

		var err error
		reply := cmd.Op + " " + cmd.Target
		switch cmd.Op {
		case "join":
			err = session.Subscribe(ctx, cmd.Target)
		case "leave":
			err = session.Unsubscribe(ctx, cmd.Target)
		case "send":
			var delivery wskit.Delivery
			delivery, err = sender.Send(ctx, cmd.Target, demo.TextMessage(cmd.Text, rand.Text()))
			reply = fmt.Sprintf("已入队 %d 个连接，失败 %d 个", len(delivery.AcceptedConnIDs), len(delivery.FailedConnIDs))
		case "sendToChannel":
			err = sender.SendToChannel(ctx, cmd.Target, demo.TextMessage(cmd.Text, ""))
		case "ack":
			return session.Ack(ctx, cmd.Target)
		default:
			return respond("unknown op: " + cmd.Op)
		}
		if err != nil {
			return err
		}
		return respond(reply)
	}
}
