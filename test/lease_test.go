package framework_test

import (
	"github.com/yinjiaming666/wskit/internal/channel"
	"testing"
	"time"
)

// TestDefaultLeaseTTLIsThirtySeconds 防止故障节点残留窗口被意外改回较长时间。
func TestDefaultLeaseTTLIsThirtySeconds(t *testing.T) {
	if channel.DefaultLeaseTTL != 30*time.Second {
		t.Fatalf("expected 30s channel lease, got %s", channel.DefaultLeaseTTL)
	}
}
