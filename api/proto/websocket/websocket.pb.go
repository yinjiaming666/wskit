// Code generated manually to match api/proto/websocket.proto. DO NOT EDIT.
package websocketpb

import (
	"reflect"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/runtime/protoimpl"
	"google.golang.org/protobuf/types/descriptorpb"
)

// PushMessage 表示内部推送消息。
type PushMessage struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	MessageId string `protobuf:"bytes,1,opt,name=message_id,json=messageId,proto3" json:"message_id,omitempty"`
	Payload   []byte `protobuf:"bytes,4,opt,name=payload,proto3" json:"payload,omitempty"`
	FrameType uint32 `protobuf:"varint,11,opt,name=frame_type,json=frameType,proto3" json:"frame_type,omitempty"`
}

func (x *PushMessage) Reset() {
	*x = PushMessage{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_proto_websocket_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}
func (x *PushMessage) String() string { return protoimpl.X.MessageStringOf(x) }
func (*PushMessage) ProtoMessage()    {}
func (x *PushMessage) ProtoReflect() protoreflect.Message {
	mi := &file_api_proto_websocket_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}
func (x *PushMessage) GetMessageId() string {
	if x != nil {
		return x.MessageId
	}
	return ""
}
func (x *PushMessage) GetPayload() []byte {
	if x != nil {
		return x.Payload
	}
	return nil
}

func (x *PushMessage) GetFrameType() uint32 {
	if x != nil {
		return x.FrameType
	}
	return 0
}

// PushToConnsRequest identifies the target connections.
type PushToConnsRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ConnIds []string     `protobuf:"bytes,1,rep,name=conn_ids,json=connIds,proto3" json:"conn_ids,omitempty"`
	Message *PushMessage `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
}

func (x *PushToConnsRequest) Reset() {
	*x = PushToConnsRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_proto_websocket_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}
func (x *PushToConnsRequest) String() string { return protoimpl.X.MessageStringOf(x) }
func (*PushToConnsRequest) ProtoMessage()    {}
func (x *PushToConnsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_api_proto_websocket_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}
func (x *PushToConnsRequest) GetConnIds() []string {
	if x != nil {
		return x.ConnIds
	}
	return nil
}
func (x *PushToConnsRequest) GetMessage() *PushMessage {
	if x != nil {
		return x.Message
	}
	return nil
}

// PushToUsersRequest 表示按用户推送请求。
type PushToUsersRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	UserIds []string     `protobuf:"bytes,1,rep,name=user_ids,json=userIds,proto3" json:"user_ids,omitempty"`
	Message *PushMessage `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
}

func (x *PushToUsersRequest) Reset() {
	*x = PushToUsersRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_proto_websocket_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}
func (x *PushToUsersRequest) String() string { return protoimpl.X.MessageStringOf(x) }
func (*PushToUsersRequest) ProtoMessage()    {}
func (x *PushToUsersRequest) ProtoReflect() protoreflect.Message {
	mi := &file_api_proto_websocket_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}
func (x *PushToUsersRequest) GetUserIds() []string {
	if x != nil {
		return x.UserIds
	}
	return nil
}
func (x *PushToUsersRequest) GetMessage() *PushMessage {
	if x != nil {
		return x.Message
	}
	return nil
}

// PushResponse 表示节点推送响应。
type PushResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Accepted        bool     `protobuf:"varint,1,opt,name=accepted,proto3" json:"accepted,omitempty"`
	AcceptedConnIds []string `protobuf:"bytes,2,rep,name=accepted_conn_ids,json=acceptedConnIds,proto3" json:"accepted_conn_ids,omitempty"`
	FailedConnIds   []string `protobuf:"bytes,3,rep,name=failed_conn_ids,json=failedConnIds,proto3" json:"failed_conn_ids,omitempty"`
	Reason          string   `protobuf:"bytes,4,opt,name=reason,proto3" json:"reason,omitempty"`
}

func (x *PushResponse) Reset() {
	*x = PushResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_proto_websocket_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}
func (x *PushResponse) String() string { return protoimpl.X.MessageStringOf(x) }
func (*PushResponse) ProtoMessage()    {}
func (x *PushResponse) ProtoReflect() protoreflect.Message {
	mi := &file_api_proto_websocket_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}
func (x *PushResponse) GetAccepted() bool {
	if x != nil {
		return x.Accepted
	}
	return false
}
func (x *PushResponse) GetAcceptedConnIds() []string {
	if x != nil {
		return x.AcceptedConnIds
	}
	return nil
}
func (x *PushResponse) GetFailedConnIds() []string {
	if x != nil {
		return x.FailedConnIds
	}
	return nil
}
func (x *PushResponse) GetReason() string {
	if x != nil {
		return x.Reason
	}
	return ""
}

// HealthRequest 表示节点健康检查请求。
type HealthRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	EndpointId string `protobuf:"bytes,1,opt,name=endpoint_id,json=endpointId,proto3" json:"endpoint_id,omitempty"`
}

func (x *HealthRequest) Reset() {
	*x = HealthRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_proto_websocket_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}
func (x *HealthRequest) String() string { return protoimpl.X.MessageStringOf(x) }
func (*HealthRequest) ProtoMessage()    {}
func (x *HealthRequest) ProtoReflect() protoreflect.Message {
	mi := &file_api_proto_websocket_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}
func (x *HealthRequest) GetEndpointId() string {
	if x != nil {
		return x.EndpointId
	}
	return ""
}

// HealthResponse 表示节点健康检查响应。
type HealthResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	EndpointId string `protobuf:"bytes,1,opt,name=endpoint_id,json=endpointId,proto3" json:"endpoint_id,omitempty"`
	Status     string `protobuf:"bytes,2,opt,name=status,proto3" json:"status,omitempty"`
	ConnCount  int64  `protobuf:"varint,3,opt,name=conn_count,json=connCount,proto3" json:"conn_count,omitempty"`
}

func (x *HealthResponse) Reset() {
	*x = HealthResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_proto_websocket_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}
func (x *HealthResponse) String() string { return protoimpl.X.MessageStringOf(x) }
func (*HealthResponse) ProtoMessage()    {}
func (x *HealthResponse) ProtoReflect() protoreflect.Message {
	mi := &file_api_proto_websocket_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}
func (x *HealthResponse) GetEndpointId() string {
	if x != nil {
		return x.EndpointId
	}
	return ""
}
func (x *HealthResponse) GetStatus() string {
	if x != nil {
		return x.Status
	}
	return ""
}
func (x *HealthResponse) GetConnCount() int64 {
	if x != nil {
		return x.ConnCount
	}
	return 0
}

var File_api_proto_websocket_proto protoreflect.FileDescriptor
var file_api_proto_websocket_proto_once sync.Once
var file_api_proto_websocket_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_api_proto_websocket_proto_goTypes = []any{(*PushMessage)(nil), (*PushToConnsRequest)(nil), (*PushToUsersRequest)(nil), (*PushResponse)(nil), (*HealthRequest)(nil), (*HealthResponse)(nil)}
var file_api_proto_websocket_proto_depIdxs = []int32{0, 0, 1, 3, 2, 3, 4, 5}

func init() { file_api_proto_websocket_proto_init() }

func file_api_proto_websocket_proto_init() {
	file_api_proto_websocket_proto_once.Do(func() {
		if File_api_proto_websocket_proto != nil {
			return
		}
		if !protoimpl.UnsafeEnabled {
			file_api_proto_websocket_proto_msgTypes[0].Exporter = func(v any, i int) any {
				switch v := v.(*PushMessage); i {
				case 0:
					return &v.state
				case 1:
					return &v.sizeCache
				case 2:
					return &v.unknownFields
				default:
					return nil
				}
			}
			file_api_proto_websocket_proto_msgTypes[1].Exporter = func(v any, i int) any {
				switch v := v.(*PushToConnsRequest); i {
				case 0:
					return &v.state
				case 1:
					return &v.sizeCache
				case 2:
					return &v.unknownFields
				default:
					return nil
				}
			}
			file_api_proto_websocket_proto_msgTypes[2].Exporter = func(v any, i int) any {
				switch v := v.(*PushToUsersRequest); i {
				case 0:
					return &v.state
				case 1:
					return &v.sizeCache
				case 2:
					return &v.unknownFields
				default:
					return nil
				}
			}
			file_api_proto_websocket_proto_msgTypes[3].Exporter = func(v any, i int) any {
				switch v := v.(*PushResponse); i {
				case 0:
					return &v.state
				case 1:
					return &v.sizeCache
				case 2:
					return &v.unknownFields
				default:
					return nil
				}
			}
			file_api_proto_websocket_proto_msgTypes[4].Exporter = func(v any, i int) any {
				switch v := v.(*HealthRequest); i {
				case 0:
					return &v.state
				case 1:
					return &v.sizeCache
				case 2:
					return &v.unknownFields
				default:
					return nil
				}
			}
			file_api_proto_websocket_proto_msgTypes[5].Exporter = func(v any, i int) any {
				switch v := v.(*HealthResponse); i {
				case 0:
					return &v.state
				case 1:
					return &v.sizeCache
				case 2:
					return &v.unknownFields
				default:
					return nil
				}
			}
		}
		type x struct{}
		out := protoimpl.TypeBuilder{
			File:              protoimpl.DescBuilder{GoPackagePath: reflect.TypeOf(x{}).PkgPath(), RawDescriptor: websocketRawDescriptor(), NumEnums: 0, NumMessages: 6, NumExtensions: 0, NumServices: 1},
			GoTypes:           file_api_proto_websocket_proto_goTypes,
			DependencyIndexes: file_api_proto_websocket_proto_depIdxs,
			MessageInfos:      file_api_proto_websocket_proto_msgTypes,
		}.Build()
		File_api_proto_websocket_proto = out.File
	})
}

func websocketRawDescriptor() []byte {
	optional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	repeated := descriptorpb.FieldDescriptorProto_LABEL_REPEATED
	tString := descriptorpb.FieldDescriptorProto_TYPE_STRING
	tBytes := descriptorpb.FieldDescriptorProto_TYPE_BYTES
	tBool := descriptorpb.FieldDescriptorProto_TYPE_BOOL
	tInt64 := descriptorpb.FieldDescriptorProto_TYPE_INT64
	tMessage := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
	pushMessage := msg("PushMessage", field("message_id", 1, optional, tString, ""), field("payload", 4, optional, tBytes, ""), field("frame_type", 11, optional, descriptorpb.FieldDescriptorProto_TYPE_UINT32, ""))
	pushMessage.ReservedRange = []*descriptorpb.DescriptorProto_ReservedRange{
		{Start: proto.Int32(2), End: proto.Int32(4)}, {Start: proto.Int32(5), End: proto.Int32(11)},
	}
	fd := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("api/proto/websocket.proto"),
		Package: proto.String("websocket.v1"),
		Syntax:  proto.String("proto3"),
		Options: &descriptorpb.FileOptions{GoPackage: proto.String("github.com/yinjiaming666/wskit/api/proto/websocket;websocketpb")},
		MessageType: []*descriptorpb.DescriptorProto{
			pushMessage,
			msg("PushToConnsRequest", field("conn_ids", 1, repeated, tString, ""), field("message", 2, optional, tMessage, ".websocket.v1.PushMessage")),
			msg("PushToUsersRequest", field("user_ids", 1, repeated, tString, ""), field("message", 2, optional, tMessage, ".websocket.v1.PushMessage")),
			msg("PushResponse", field("accepted", 1, optional, tBool, ""), field("accepted_conn_ids", 2, repeated, tString, ""), field("failed_conn_ids", 3, repeated, tString, ""), field("reason", 4, optional, tString, "")),
			msg("HealthRequest", field("endpoint_id", 1, optional, tString, "")),
			msg("HealthResponse", field("endpoint_id", 1, optional, tString, ""), field("status", 2, optional, tString, ""), field("conn_count", 3, optional, tInt64, "")),
		},
		Service: []*descriptorpb.ServiceDescriptorProto{{
			Name: proto.String("WebSocketPushService"),
			Method: []*descriptorpb.MethodDescriptorProto{
				{Name: proto.String("PushToConns"), InputType: proto.String(".websocket.v1.PushToConnsRequest"), OutputType: proto.String(".websocket.v1.PushResponse")},
				{Name: proto.String("PushToUsers"), InputType: proto.String(".websocket.v1.PushToUsersRequest"), OutputType: proto.String(".websocket.v1.PushResponse")},
				{Name: proto.String("Health"), InputType: proto.String(".websocket.v1.HealthRequest"), OutputType: proto.String(".websocket.v1.HealthResponse")},
			},
		}},
	}
	raw, _ := proto.Marshal(fd)
	return raw
}

func msg(name string, fields ...*descriptorpb.FieldDescriptorProto) *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{Name: proto.String(name), Field: fields}
}
func field(name string, number int32, label descriptorpb.FieldDescriptorProto_Label, typ descriptorpb.FieldDescriptorProto_Type, typeName string) *descriptorpb.FieldDescriptorProto {
	f := &descriptorpb.FieldDescriptorProto{Name: proto.String(name), Number: proto.Int32(number), Label: &label, Type: &typ}
	if typeName != "" {
		f.TypeName = proto.String(typeName)
	}
	return f
}
