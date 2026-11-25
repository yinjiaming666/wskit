package framework_test

import (
	"reflect"
	"testing"

	"github.com/yinjiaming666/wskit"
	websocketpb "github.com/yinjiaming666/wskit/api/proto/websocket"
	"github.com/yinjiaming666/wskit/internal/grpcpush"
	"google.golang.org/protobuf/proto"
)

func TestDeliveryResultProtocolBoundary(t *testing.T) {
	for _, want := range []socketserver.DeliveryResult{
		{Accepted: true},
		{Accepted: true, AcceptedConnIDs: []string{"a", "b"}},
		{AcceptedConnIDs: []string{"a"}, FailedConnIDs: []string{"b"}, Reason: "some connections failed"},
	} {
		encoded, err := proto.Marshal(grpcpush.PushResponseToProto(&want))
		if err != nil {
			t.Fatal(err)
		}
		wire := new(websocketpb.PushResponse)
		if err := proto.Unmarshal(encoded, wire); err != nil {
			t.Fatal(err)
		}
		var got *socketserver.DeliveryResult = grpcpush.PushResponseFromProto(wire)
		if !reflect.DeepEqual(*got, want) {
			t.Fatalf("result changed across protobuf boundary: %+v, want %+v", got, want)
		}
	}
	if got := grpcpush.PushResponseFromProto(nil); got.Accepted || got.Reason != "nil response" {
		t.Fatalf("missing wire response = %+v", got)
	}
	if got := grpcpush.PushResponseToProto(nil); got.GetAccepted() || got.GetReason() != "nil response" {
		t.Fatalf("missing local response = %+v", got)
	}
}
