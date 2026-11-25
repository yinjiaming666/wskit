package domain

// Endpoint 表示一个可被 Gateway 分配的 WebSocket 节点。
type Endpoint struct {
	ID        string
	Status    string
	WSPath    string
	WSAddr    string
	GRPCAddr  string
	ConnCount int64
	MaxConn   int64
	UpdatedAt int64
}

// ConnInfo 表示一个用户长连接在系统中的路由信息。
type ConnInfo struct {
	ConnID     string
	UserID     string
	EndpointID string
	DeviceID   string
	Platform   string
	GRPCAddr   string
	CreatedAt  int64
	UpdatedAt  int64
}

// Message contains delivery metadata and the already encoded application frame.
type Message struct {
	MessageID      string
	ToUserIDs      []string
	DeliveryRoutes []*ConnInfo
	Channel        string
	Frame          Frame
	CreatedAt      int64
}

// DeliveryResult describes node queue acceptance, not WebSocket writes or client receipts.
// Accepted is true when every resolved connection was accepted, including an empty set.
type DeliveryResult struct {
	Accepted        bool
	AcceptedConnIDs []string
	FailedConnIDs   []string
	Reason          string
}
