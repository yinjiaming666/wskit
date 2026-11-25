package framework_test

import (
	"bytes"
	"github.com/yinjiaming666/wskit/internal/domain"
	"testing"
)

func TestFrameWireFormat(t *testing.T) {
	for _, f := range []domain.Frame{
		{Type: domain.TextFrame, Data: []byte("plain text")},
		{Type: domain.BinaryFrame, Data: []byte{0xff, 0}},
		{Type: domain.BinaryFrame},
	} {
		encoded, err := f.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := domain.DecodeFrame(encoded)
		if err != nil || decoded.Type != f.Type || !bytes.Equal(decoded.Data, f.Data) {
			t.Fatalf("frame round trip = %#v, %v", decoded, err)
		}
	}
	for _, data := range [][]byte{nil, {9, 0}, {byte(domain.TextFrame), 0xff}} {
		if _, err := domain.DecodeFrame(data); err == nil {
			t.Fatalf("accepted invalid transport packet %x", data)
		}
	}
}
