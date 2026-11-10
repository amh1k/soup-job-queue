package config

import (
	"crypto/rand"
	"errors"
	"strconv"

	"github.com/amh1k/soup-job-queue/internal/worker"
)

type WorkerConfig struct {
	Config
	Options worker.Options
}

func LoadWorker() (WorkerConfig, error) {
	base, err := Load()
	if err != nil {
		return WorkerConfig{}, err
	}
	options := worker.Options{ID: value("WORKER_ID", "worker-"+rand.Text()), DBTimeout: base.DBTimeout}
	if options.LeaseDuration, err = duration("LEASE_DURATION", "10s"); err != nil {
		return WorkerConfig{}, err
	}
	if options.HeartbeatInterval, err = duration("HEARTBEAT_INTERVAL", "2s"); err != nil {
		return WorkerConfig{}, err
	}
	if options.PollInterval, err = duration("POLL_INTERVAL", "100ms"); err != nil {
		return WorkerConfig{}, err
	}
	if options.RecoveryInterval, err = duration("RECOVERY_INTERVAL", "1s"); err != nil {
		return WorkerConfig{}, err
	}
	if options.RecoveryBatch, err = strconv.Atoi(value("RECOVERY_BATCH", "100")); err != nil {
		return WorkerConfig{}, errors.New("RECOVERY_BATCH must be an integer")
	}
	if err := options.Validate(); err != nil {
		return WorkerConfig{}, err
	}
	return WorkerConfig{Config: base, Options: options}, nil
}
