package httpapi

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestLivenessDoesNotCheckDatabase(t *testing.T) {
	h := NewHandler(func(context.Context) error { t.Fatal("database checked for liveness"); return nil }, time.Second)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/healthz", nil))
	if w.Code != 200 || w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("response: %v", w.Result())
	}
}

func TestReadiness(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status int
	}{
		{"ready", nil, 200}, {"unavailable", errors.New("secret database details"), 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHandler(func(ctx context.Context) error {
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("missing deadline")
				}
				return tc.err
			}, time.Second)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("GET", "/readyz", nil))
			if w.Code != tc.status || strings.Contains(w.Body.String(), "secret") {
				t.Fatalf("response: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestReadinessDeadline(t *testing.T) {
	h := NewHandler(func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }, time.Millisecond)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/readyz", nil))
	if w.Code != 503 {
		t.Fatalf("status: %d", w.Code)
	}
}
