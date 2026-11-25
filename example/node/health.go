package main

import (
	"context"
	"encoding/json"
	"net/http"
)

// The host chooses probe routes, status codes and JSON, using only library state.
type nodeStatus interface {
	Ready() bool
	ConnCount() int64
}

type healthResponse struct {
	Status    string `json:"status"`
	Ready     bool   `json:"ready"`
	ConnCount int64  `json:"conn_count"`
}

func registerHealthRoutes(mux *http.ServeMux, node nodeStatus, dependencyReady func(context.Context) error) {
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeHealth(w, http.StatusMethodNotAllowed, "method not allowed", nil)
			return
		}
		writeHealth(w, http.StatusOK, "ok", healthResponse{Status: "ok", Ready: node.Ready(), ConnCount: node.ConnCount()})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeHealth(w, http.StatusMethodNotAllowed, "method not allowed", nil)
			return
		}
		if !node.Ready() {
			writeHealth(w, http.StatusServiceUnavailable, "node is not ready", nil)
			return
		}
		if err := dependencyReady(r.Context()); err != nil {
			writeHealth(w, http.StatusServiceUnavailable, "websocket dependency unavailable", nil)
			return
		}
		writeHealth(w, http.StatusOK, "ok", healthResponse{Status: "ready", Ready: true, ConnCount: node.ConnCount()})
	})
}

func writeHealth(w http.ResponseWriter, status int, message string, data any) {
	code := status
	if status == http.StatusOK {
		code = 0
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    any    `json:"data,omitempty"`
	}{Code: code, Message: message, Data: data})
}
