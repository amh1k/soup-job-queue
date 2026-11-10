package experiment

import (
	"errors"
	"time"
)

type Options struct {
	Output        string        `json:"output"`
	WorkerBinary  string        `json:"worker_binary"`
	Seed          int64         `json:"seed"`
	Workers       int           `json:"workers"`
	Jobs          int           `json:"jobs"`
	KillInterval  time.Duration `json:"kill_interval"`
	FaultDuration time.Duration `json:"fault_duration"`
	DrainTimeout  time.Duration `json:"drain_timeout"`
	Timeout       time.Duration `json:"timeout"`
	RPS           int           `json:"rps"`
	Warmup        time.Duration `json:"warmup"`
	Duration      time.Duration `json:"duration"`
	Mode          string        `json:"mode"`
	MaxInflight   int           `json:"max_inflight"`
}

func DefaultOptions() Options {
	return Options{Output: "results", WorkerBinary: "bin/worker", Seed: 1, Workers: 8, Jobs: 10000, KillInterval: 2 * time.Second, FaultDuration: 30 * time.Second, DrainTimeout: 5 * time.Minute, Timeout: 15 * time.Minute, RPS: 100, Warmup: 30 * time.Second, Duration: 5 * time.Minute, Mode: "mixed", MaxInflight: 256}
}
func (o Options) validate(kind string) error {
	if o.Workers < 1 || o.Workers > 64 || o.Timeout <= 0 || o.DrainTimeout <= 0 || o.Output == "" || o.WorkerBinary == "" {
		return errors.New("invalid worker count, paths, or timeouts")
	}
	if kind == "chaos" && (o.Jobs < 1 || o.Jobs > 1000000 || o.KillInterval <= 0 || o.FaultDuration < o.KillInterval) {
		return errors.New("invalid chaos workload or fault duration")
	}
	if kind == "load" && (o.RPS < 1 || o.RPS > 10000 || o.Warmup < 0 || o.Duration < time.Second || o.Duration > time.Hour || o.Warmup > time.Hour || o.MaxInflight < 1 || o.MaxInflight > 10000 || (o.Mode != "mixed" && o.Mode != "create") || float64(o.RPS)*(o.Warmup+o.Duration).Seconds() > 1000000) {
		return errors.New("invalid load workload")
	}
	return nil
}
