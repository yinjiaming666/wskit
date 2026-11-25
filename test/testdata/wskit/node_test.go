package wskit

import (
	"context"
	"errors"
	"sync"

	"github.com/yinjiaming666/wskit/internal/domain"
	"github.com/yinjiaming666/wskit/internal/endpoint"
	"github.com/yinjiaming666/wskit/internal/route"
)

func newTestNode(cfg NodeConfig) *nodeCore {
	endpoints := &testEndpoints{byID: make(map[string]domain.Endpoint)}
	routes := &testRoutes{byConn: make(map[string]domain.ConnInfo)}
	return newNodeCore(cfg, endpoints, routes, nil)
}

// The node tests exercise only endpoint heartbeats and connection registration/cleanup.
type testEndpoints struct {
	endpoint.Repository
	mu   sync.Mutex
	byID map[string]domain.Endpoint
}

func (s *testEndpoints) UpsertEndpoint(_ context.Context, ep *domain.Endpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byID[ep.ID] = *ep
	return nil
}

func (s *testEndpoints) GetEndpoint(_ context.Context, id string) (*domain.Endpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ep, ok := s.byID[id]
	if !ok {
		return nil, errors.New("endpoint not found")
	}
	return &ep, nil
}

func (s *testEndpoints) DeleteEndpoint(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.byID, id)
	return nil
}

type testRoutes struct {
	route.Resolver
	mu     sync.Mutex
	byConn map[string]domain.ConnInfo
}

func (s *testRoutes) RegisterConn(_ context.Context, info *domain.ConnInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byConn[info.ConnID] = *info
	return nil
}

func (s *testRoutes) GetConnInfo(_ context.Context, id string) (*domain.ConnInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	info, ok := s.byConn[id]
	if !ok {
		return nil, errors.New("connection not found")
	}
	return &info, nil
}

func (s *testRoutes) RemoveConn(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.byConn, id)
	return nil
}

func (s *testRoutes) RemoveEndpointConns(_ context.Context, endpointID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, info := range s.byConn {
		if info.EndpointID == endpointID {
			delete(s.byConn, id)
		}
	}
	return nil
}
