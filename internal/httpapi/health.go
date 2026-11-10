package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

func NewHandler(ready func(context.Context) error, timeout time.Duration) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeStatus(w, http.StatusOK, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		if err := ready(ctx); err != nil {
			writeStatus(w, http.StatusServiceUnavailable, "not_ready")
			return
		}
		writeStatus(w, http.StatusOK, "ready")
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { apiError(w, 404, "not_found", "Route not found") })
	return mux
}

func writeStatus(w http.ResponseWriter, code int, status string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": status})
}
