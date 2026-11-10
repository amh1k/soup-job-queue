package config

import (
	"testing"
	"time"
)

func TestLoadWorkerDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/soup")
	for _, key := range []string{"WORKER_ID", "LEASE_DURATION", "HEARTBEAT_INTERVAL", "POLL_INTERVAL", "RECOVERY_INTERVAL", "RECOVERY_BATCH", "DB_TIMEOUT"} {
		t.Setenv(key, "")
	}
	cfg, err := LoadWorker()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Options.ID == "" || cfg.Options.LeaseDuration != 10*time.Second || cfg.Options.HeartbeatInterval != 2*time.Second || cfg.Options.RecoveryBatch != 100 {
		t.Fatalf("unexpected options %+v", cfg.Options)
	}
	other, err := LoadWorker()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Options.ID == other.Options.ID {
		t.Fatal("default IDs collide")
	}
}
func TestLoadWorkerRejectsUnsafeTiming(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/soup")
	t.Setenv("LEASE_DURATION", "4s")
	t.Setenv("HEARTBEAT_INTERVAL", "2s")
	t.Setenv("DB_TIMEOUT", "3s")
	if _, err := LoadWorker(); err == nil {
		t.Fatal("unsafe timing accepted")
	}
}
