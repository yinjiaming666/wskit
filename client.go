package socketserver

import (
	"errors"
	"sync"
)

const maxChannelsPerClient = 64

// localClient 表示 WebSocket Node 本地维护的一条客户端连接。
type localClient struct {
	// ConnID 表示连接唯一编号。
	ConnID string
	// UserID 表示连接所属用户编号。
	UserID string
	// DeviceID 表示客户端设备编号。
	DeviceID string
	// Platform 表示客户端平台，例如 ios、android、web。
	Platform string
	// Send 表示连接的异步发送队列。
	Send chan Frame
	// mu 保护 channels、closed、writeFunc 并发读写。
	mu sync.RWMutex
	// channels 表示当前连接加入的频道集合。
	channels map[string]struct{}
	// closed 表示连接是否已经关闭。
	closed bool
	// done 在连接关闭时广播，Send 不关闭以避免并发发送 panic。
	done chan struct{}
	// writeFunc 表示真实 WebSocket 连接的写入函数。
	writeFunc func(Frame) error
}

// newClient 创建本地客户端连接对象。
func newClient(connID string, userID string, deviceID string, platform string, sendBuffer int) *localClient {
	if sendBuffer <= 0 {
		sendBuffer = 128
	}
	return &localClient{
		ConnID:   connID,
		UserID:   userID,
		DeviceID: deviceID,
		Platform: platform,
		Send:     make(chan Frame, sendBuffer),
		done:     make(chan struct{}),
		channels: make(map[string]struct{}),
	}
}

// AttachWriter 绑定真实 WebSocket 写入函数。
func (c *localClient) AttachWriter(writeFunc func(Frame) error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeFunc = writeFunc
}

// Enqueue 将只读消息缓冲区写入连接发送队列。
//
// payload 在入队后必须保持不可变。多个客户端共享同一底层字节数组，
// 可以避免频道广播时为每条连接复制完整消息。队列满时主动关闭慢连接。
func (c *localClient) Enqueue(frame Frame) bool {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return false
	}
	select {
	case c.Send <- frame:
		c.mu.RUnlock()
		return true
	default:
		c.mu.RUnlock()
		c.Close()
		return false
	}
}

// WriteDirect 直接写入真实 WebSocket 连接。
func (c *localClient) WriteDirect(frame Frame) error {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return errors.New("websocket client is closed")
	}
	writeFunc := c.writeFunc
	c.mu.RUnlock()
	if writeFunc == nil {
		return errors.New("websocket writer is not attached")
	}
	return writeFunc(frame)
}

// Done 返回连接关闭通知。
func (c *localClient) Done() <-chan struct{} {
	return c.done
}

// Subscribe 登记当前连接的频道订阅，并限制单连接的订阅数量。
func (c *localClient) Subscribe(channelID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	if _, exists := c.channels[channelID]; exists {
		return true
	}
	if len(c.channels) >= maxChannelsPerClient {
		return false
	}
	c.channels[channelID] = struct{}{}
	return true
}

// Unsubscribe 删除当前连接的频道订阅。
func (c *localClient) Unsubscribe(channelID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.channels, channelID)
}

// Channels 返回当前连接订阅的频道列表。
func (c *localClient) Channels() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	channels := make([]string, 0, len(c.channels))
	for channelID := range c.channels {
		channels = append(channels, channelID)
	}
	return channels
}

// Close 关闭当前连接。Send 保持打开，消费者通过 Done 退出。
func (c *localClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return
	}
	c.closed = true
	close(c.done)
}
