package grpcpush

import (
	websocketpb "github.com/yinjiaming666/wskit/api/proto/websocket"
	"github.com/yinjiaming666/wskit/internal/domain"
)

// MessageToProto 将领域消息转换为 protobuf 消息。
func MessageToProto(msg *domain.Message) *websocketpb.PushMessage {
	if msg == nil {
		return nil
	}
	return &websocketpb.PushMessage{MessageId: msg.MessageID, Payload: msg.Frame.Data, FrameType: uint32(msg.Frame.Type)}
}

func MessageFromProto(msg *websocketpb.PushMessage) *domain.Message {
	if msg == nil {
		return nil
	}
	return &domain.Message{MessageID: msg.GetMessageId(), Frame: domain.Frame{Type: domain.FrameType(msg.GetFrameType()), Data: msg.GetPayload()}}
}

// PushResponseToProto 将领域响应转换为 protobuf 响应。
func PushResponseToProto(resp *domain.DeliveryResult) *websocketpb.PushResponse {
	if resp == nil {
		return &websocketpb.PushResponse{Accepted: false, Reason: "nil response"}
	}
	return &websocketpb.PushResponse{
		Accepted:        resp.Accepted,
		AcceptedConnIds: resp.AcceptedConnIDs,
		FailedConnIds:   resp.FailedConnIDs,
		Reason:          resp.Reason,
	}
}

// PushResponseFromProto 将 protobuf 响应转换为领域响应。
func PushResponseFromProto(resp *websocketpb.PushResponse) *domain.DeliveryResult {
	if resp == nil {
		return &domain.DeliveryResult{Accepted: false, Reason: "nil response"}
	}
	return &domain.DeliveryResult{
		Accepted:        resp.GetAccepted(),
		AcceptedConnIDs: resp.GetAcceptedConnIds(),
		FailedConnIDs:   resp.GetFailedConnIds(),
		Reason:          resp.GetReason(),
	}
}
