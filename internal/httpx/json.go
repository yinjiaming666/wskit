package httpx

import (
	"encoding/json"
	"net/http"
)

// WriteError preserves the WebSocket handshake error format.
func WriteError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}{Code: statusCode, Message: message})
}
