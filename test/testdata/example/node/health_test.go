package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type testNodeStatus struct{ ready bool }

func (s *testNodeStatus) Ready() bool    { return s.ready }
func (*testNodeStatus) ConnCount() int64 { return 7 }

func TestHostHealthRoutes(t *testing.T) {
	node := &testNodeStatus{}
	var dependencyErr error
	checks := 0
	mux := http.NewServeMux()
	registerHealthRoutes(mux, node, func(context.Context) error { checks++; return dependencyErr })
	probe := func(method, path string, want int, ready bool) {
		t.Helper()
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest(method, path, nil))
		if response.Code != want {
			t.Fatalf("%s %s = %d: %s", method, path, response.Code, response.Body.String())
		}
		var body struct {
			Code    int             `json:"code"`
			Message string          `json:"message"`
			Data    *healthResponse `json:"data"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if want == http.StatusOK {
			if body.Code != 0 || body.Message != "ok" || body.Data == nil || body.Data.Ready != ready || body.Data.ConnCount != 7 {
				t.Fatalf("unexpected health response: %s", response.Body.String())
			}
		} else if body.Code != want || body.Message == "" || body.Data != nil {
			t.Fatalf("unexpected failure response: %s", response.Body.String())
		}
	}
	probe("GET", "/readyz", http.StatusServiceUnavailable, false)
	probe("GET", "/healthz", http.StatusOK, false)
	if checks != 0 {
		t.Fatal("inactive node checked Redis")
	}
	node.ready = true
	probe("GET", "/readyz", http.StatusOK, true)
	dependencyErr = errors.New("redis unavailable")
	probe("GET", "/readyz", http.StatusServiceUnavailable, false)
	probe("GET", "/healthz", http.StatusOK, true)
	dependencyErr = nil
	node.ready = false // Draining preserves liveness but rejects new admissions.
	probe("GET", "/readyz", http.StatusServiceUnavailable, false)
	probe("GET", "/healthz", http.StatusOK, false)
	probe("POST", "/readyz", http.StatusMethodNotAllowed, false)
	probe("POST", "/healthz", http.StatusMethodNotAllowed, false)
}
