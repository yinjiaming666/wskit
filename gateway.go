package socketserver

import (
	"context"
	"errors"
	"net/url"
	"time"

	"github.com/yinjiaming666/wskit/internal/endpoint"
)

type GatewayConfig struct {
	// PublicURL is the WebSocket origin, for example wss://ws.example.org.
	PublicURL        string
	HeartbeatTimeout time.Duration
	LastEndpointTTL  time.Duration
}

// Route is the unsigned address selected for a connection.
type Route struct {
	EndpointID string `json:"endpoint_id"`
	WSURL      string `json:"ws_url"`
}

type Gateway struct {
	selector *endpoint.DefaultEndpointSelector
	origin   *url.URL
}

func CreateGateway(cfg GatewayConfig, backend *Backend) (*Gateway, error) {
	if err := backend.validate(); err != nil {
		return nil, err
	}
	u, err := url.Parse(cfg.PublicURL)
	if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Hostname() == "" || u.User != nil ||
		(u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || u.ForceQuery {
		return nil, errors.New("PublicURL must be a ws or wss origin")
	}
	selector := endpoint.NewDefaultEndpointSelector(backend.endpoints, backend.routes, cfg.HeartbeatTimeout, cfg.LastEndpointTTL)
	return &Gateway{selector: selector, origin: u}, nil
}

// Route selects a healthy node for the user identified by the host.
func (g *Gateway) Route(ctx context.Context, userID string) (Route, error) {
	if userID == "" {
		return Route{}, errors.New("user ID is required")
	}
	node, err := g.selector.SelectEndpoint(ctx, userID)
	if err != nil {
		return Route{}, err
	}
	u := *g.origin
	path, err := url.ParseRequestURI(node.WSPath)
	if err != nil {
		return Route{}, err
	}
	u.Path, u.RawPath = path.Path, path.RawPath
	return Route{EndpointID: node.ID, WSURL: u.String()}, nil
}
