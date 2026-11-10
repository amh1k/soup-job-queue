package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/soup?sslmode=disable")
	for _, key := range []string{"HTTP_ADDR", "DB_TIMEOUT", "DB_MAX_CONNS", "SHUTDOWN_TIMEOUT"} {
		t.Setenv(key, "")
	}
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != ":8080" || cfg.DBTimeout != 3*time.Second || cfg.DBMaxConns != 10 || cfg.ShutdownTimeout != 10*time.Second {
		t.Fatalf("unexpected defaults: addr=%s timeout=%s max=%d shutdown=%s", cfg.HTTPAddr, cfg.DBTimeout, cfg.DBMaxConns, cfg.ShutdownTimeout)
	}
}

func TestLoadRejectsInvalidSettings(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"DATABASE_URL", ""}, {"HTTP_ADDR", "bad-address"},
		{"DB_TIMEOUT", "0s"}, {"DB_TIMEOUT", "bad"},
		{"SHUTDOWN_TIMEOUT", "-1s"}, {"DB_MAX_CONNS", "0"}, {"DB_MAX_CONNS", "bad"},
	} {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "postgres://localhost/soup")
			t.Setenv(tc.key, tc.value)
			if _, err := Load(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
