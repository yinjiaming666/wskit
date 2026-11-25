package domain

import (
	"errors"
	"unicode/utf8"
)

// FrameType is the WebSocket opcode for an application data frame.
type FrameType uint32

const (
	TextFrame   FrameType = 1
	BinaryFrame FrameType = 2
)

// Frame carries application bytes unchanged. Control frames are handled by the node.
type Frame struct {
	Type FrameType
	Data []byte
}

func (f Frame) Validate() error {
	switch f.Type {
	case TextFrame:
		if !utf8.Valid(f.Data) {
			return errors.New("invalid UTF-8 in WebSocket text frame")
		}
	case BinaryFrame:
	default:
		return errors.New("invalid WebSocket data frame type")
	}
	return nil
}

// MarshalBinary packs the opcode and application bytes for Redis node notifications.
func (f Frame) MarshalBinary() ([]byte, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}
	b := make([]byte, 1+len(f.Data))
	b[0] = byte(f.Type)
	copy(b[1:], f.Data)
	return b, nil
}

// DecodeFrame unpacks a node notification. Data refers to the input buffer.
func DecodeFrame(data []byte) (Frame, error) {
	if len(data) == 0 {
		return Frame{}, errors.New("missing WebSocket frame type")
	}
	frame := Frame{Type: FrameType(data[0]), Data: data[1:]}
	return frame, frame.Validate()
}
