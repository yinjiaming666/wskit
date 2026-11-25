package wskit

import (
	"testing"
)

// TestChannelHubOldClientCannotRemoveReplacement 验证旧连接延迟清理不会删除相同 conn_id 的新实例。
func TestChannelHubOldClientCannotRemoveReplacement(t *testing.T) {
	hub := newChannelHub()
	oldClient := newClient("same-conn", "user-1", "old-device", "test", 4)
	newClient := newClient("same-conn", "user-1", "new-device", "test", 4)

	if joined, _ := hub.Subscribe("channel-1", oldClient); !joined {
		t.Fatal("old client failed to subscribe to channel")
	}
	if joined, _ := hub.Subscribe("channel-1", newClient); !joined {
		t.Fatal("replacement client failed to subscribe to channel")
	}

	// 频道映射已经指向新实例，此时清理旧实例必须成为空操作。
	if emptyChannels := hub.RemoveClient(oldClient); len(emptyChannels) != 0 {
		t.Fatalf("old client unexpectedly emptied channels: %#v", emptyChannels)
	}
	connIDs := hub.ListConns("channel-1")
	if len(connIDs) != 1 || connIDs[0] != "same-conn" {
		t.Fatalf("replacement channel membership was removed: %#v", connIDs)
	}

	if emptyChannels := hub.RemoveClient(newClient); len(emptyChannels) != 1 || emptyChannels[0] != "channel-1" {
		t.Fatalf("replacement cleanup did not empty channel: %#v", emptyChannels)
	}
}
