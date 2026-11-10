package experiment

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func Main(kind string) {
	o := DefaultOptions()
	flag.StringVar(&o.Output, "out", o.Output, "directory containing immutable run directories")
	flag.StringVar(&o.WorkerBinary, "worker-bin", o.WorkerBinary, "compiled worker executable")
	flag.Int64Var(&o.Seed, "seed", o.Seed, "recorded workload/random-kill seed")
	flag.IntVar(&o.Workers, "workers", o.Workers, "number of independent worker processes")
	flag.DurationVar(&o.DrainTimeout, "drain-timeout", o.DrainTimeout, "deadline for terminal states after submissions stop")
	flag.DurationVar(&o.Timeout, "timeout", o.Timeout, "overall run deadline")
	if kind == "chaos" {
		flag.IntVar(&o.Jobs, "jobs", o.Jobs, "unique job count")
		flag.DurationVar(&o.KillInterval, "kill-interval", o.KillInterval, "random SIGKILL interval")
		flag.DurationVar(&o.FaultDuration, "fault-duration", o.FaultDuration, "minimum duration of parallel fault/retry/cancel traffic")
	} else {
		flag.IntVar(&o.RPS, "rps", o.RPS, "scheduled HTTP requests per second")
		flag.DurationVar(&o.Warmup, "warmup", o.Warmup, "warm-up duration")
		flag.DurationVar(&o.Duration, "duration", o.Duration, "measurement duration")
		flag.StringVar(&o.Mode, "mode", o.Mode, "mixed or create")
		flag.IntVar(&o.MaxInflight, "max-inflight", o.MaxInflight, "requests allowed concurrently; excess arrivals are recorded as drops")
	}
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := Run(ctx, kind, o); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
