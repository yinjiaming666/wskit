package framework_test

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/yinjiaming666/wskit/internal/domain"
	"github.com/yinjiaming666/wskit/internal/sender"
)

type channelOwners map[string][]string

func (owners channelOwners) ListChannelNodes(_ context.Context, channel string) ([]string, error) {
	return owners[channel], nil
}

type publishedFrames map[string][][]byte

func (p publishedFrames) Publish(ctx context.Context, channel string, body []byte) error {
	return p.PublishMany(ctx, []string{channel}, body)
}

func (p publishedFrames) PublishMany(_ context.Context, channels []string, body []byte) error {
	for _, channel := range channels {
		p[channel] = append(p[channel], bytes.Clone(body))
	}
	return nil
}

func TestRoutedRedisPubSubSenderOnlyPublishesOwningNodes(t *testing.T) {
	owners := channelOwners{"channel-1": {"node-a", "node-b"}, "other-channel": {"node-c"}}
	published := publishedFrames{}
	channelSender := sender.NewRoutedRedisPubSubSender(published, owners)
	frame := domain.Frame{Type: domain.BinaryFrame, Data: []byte{0, 255}}
	err := channelSender.Send(context.Background(), &domain.Message{
		MessageID: "live-1",
		Channel:   "channel-1",
		Frame:     frame,
	})
	if err != nil {
		t.Fatal(err)
	}

	body, err := frame.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	want := publishedFrames{
		"live:node:node-a:channel:channel-1": {body},
		"live:node:node-b:channel:channel-1": {body},
	}
	if !reflect.DeepEqual(published, want) {
		t.Fatalf("published frames = %v, want %v", published, want)
	}
}
