package socketserver

import (
	"sort"
	"sync"
)

// channelHub 维护 WebSocket Node 本地频道到连接的映射。
type channelHub struct {
	// mu 保护 channels 并发读写。
	mu sync.RWMutex
	// channels 保存 channel_id 到 conn_id 和连接对象的映射。
	channels map[string]map[string]*localClient
}

// newChannelHub 创建本地频道连接管理器。
func newChannelHub() *channelHub {
	return &channelHub{channels: make(map[string]map[string]*localClient)}
}

// Subscribe 登记连接的频道订阅，并返回其是否为首个本地订阅者。
func (h *channelHub) Subscribe(channelID string, client *localClient) (bool, bool) {
	if channelID == "" || client == nil || !client.Subscribe(channelID) {
		return false, false
	}

	h.mu.Lock()
	firstLocalSubscriber := len(h.channels[channelID]) == 0
	if _, ok := h.channels[channelID]; !ok {
		h.channels[channelID] = make(map[string]*localClient)
	}
	h.channels[channelID][client.ConnID] = client
	h.mu.Unlock()
	return true, firstLocalSubscriber
}

// Unsubscribe 删除连接的频道订阅，并返回其是否为最后一个本地订阅者。
func (h *channelHub) Unsubscribe(channelID string, connID string) bool {
	h.mu.Lock()
	client := h.channels[channelID][connID]
	delete(h.channels[channelID], connID)
	lastLocalSubscriber := client != nil && len(h.channels[channelID]) == 0
	if len(h.channels[channelID]) == 0 {
		delete(h.channels, channelID)
	}
	h.mu.Unlock()

	if client != nil {
		client.Unsubscribe(channelID)
	}
	return lastLocalSubscriber
}

// RemoveClient 仅移除仍指向期望 localClient 实例的频道订阅，并返回因此失去最后一个本地订阅者的频道。
// 相同 conn_id 已被新实例替换时，旧连接的延迟清理不会误删新连接。
func (h *channelHub) RemoveClient(expected *localClient) []string {
	if expected == nil {
		return nil
	}

	h.mu.Lock()
	removed := false
	emptyChannels := make([]string, 0)
	for channelID, clients := range h.channels {
		current, ok := clients[expected.ConnID]
		if !ok || current != expected {
			continue
		}
		removed = true
		delete(clients, expected.ConnID)
		if len(clients) == 0 {
			delete(h.channels, channelID)
			emptyChannels = append(emptyChannels, channelID)
		}
	}
	h.mu.Unlock()

	if removed {
		for _, channelID := range expected.Channels() {
			expected.Unsubscribe(channelID)
		}
	}
	sort.Strings(emptyChannels)
	return emptyChannels
}

// Broadcast 向指定频道内本节点连接广播消息。
func (h *channelHub) Broadcast(channelID string, frame Frame) int {
	h.mu.RLock()
	clients := make([]*localClient, 0, len(h.channels[channelID]))
	for _, client := range h.channels[channelID] {
		clients = append(clients, client)
	}
	h.mu.RUnlock()

	sent := 0
	for _, client := range clients {
		if client.Enqueue(frame) {
			sent++
		}
	}
	return sent
}

// ListConns 返回指定频道在本节点上的连接编号列表。
func (h *channelHub) ListConns(channelID string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	connIDs := make([]string, 0, len(h.channels[channelID]))
	for connID := range h.channels[channelID] {
		connIDs = append(connIDs, connID)
	}
	return connIDs
}

// ListChannels 返回本节点当前承载的频道列表。
func (h *channelHub) ListChannels() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	channelIDs := make([]string, 0, len(h.channels))
	for channelID := range h.channels {
		channelIDs = append(channelIDs, channelID)
	}
	sort.Strings(channelIDs)
	return channelIDs
}
