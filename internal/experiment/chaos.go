package experiment

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/amh1k/soup-job-queue/internal/audit"
)

func runChaos(ctx context.Context, o Options, s *session, c *client, entries []audit.Entry) error {
	phase, stop := context.WithCancel(ctx)
	var activity sync.WaitGroup
	var errorMu sync.Mutex
	var failures []error
	record := func(err error) {
		if err != nil {
			errorMu.Lock()
			failures = append(failures, err)
			errorMu.Unlock()
		}
	}
	activity.Add(3)
	go func() {
		defer activity.Done()
		rng := rand.New(rand.NewSource(o.Seed + 1))
		ticker := time.NewTicker(o.KillInterval)
		defer ticker.Stop()
		for {
			select {
			case <-phase.Done():
				return
			case <-ticker.C:
				if err := s.supervisor.killRandom(rng); err != nil {
					s.evidence.event("kill_error", err.Error())
					record(err)
				}
			}
		}
	}()
	go func() {
		defer activity.Done()
		rng := rand.New(rand.NewSource(o.Seed + 2))
		for phase.Err() == nil {
			err := c.create(phase, "duplicate", entries[rng.Intn(len(entries))])
			if phase.Err() == nil {
				record(err)
			}
			if !sleep(phase, 5*time.Millisecond) {
				return
			}
		}
	}()
	go func() {
		defer activity.Done()
		rng := rand.New(rand.NewSource(o.Seed + 3))
		for phase.Err() == nil {
			if id := c.id(rng.Intn(1 << 30)); id != "" {
				c.cancelJob(phase, "cancellation", id)
			}
			if !sleep(phase, 5*time.Millisecond) {
				return
			}
		}
	}()
	started := time.Now()
	var producers sync.WaitGroup
	work := make(chan audit.Entry)
	for range 32 {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for entry := range work {
				record(c.create(ctx, "creation", entry))
			}
		}()
	}
	for _, entry := range entries {
		if ctx.Err() != nil {
			break
		}
		select {
		case work <- entry:
		case <-ctx.Done():
		}
	}
	close(work)
	producers.Wait()
	sleep(ctx, time.Until(started.Add(o.FaultDuration)))
	stop()
	activity.Wait()
	deadline := time.Now().Add(5 * time.Second)
	for s.supervisor.live() != o.Workers && time.Now().Before(deadline) && ctx.Err() == nil {
		sleep(ctx, 10*time.Millisecond)
	}
	if s.supervisor.live() != o.Workers {
		record(errors.New("worker count below configured count at fault cutoff"))
	}
	// Resolve POSTs whose responses were lost at fault cutoff with the same keys.
	for _, entry := range entries {
		if ctx.Err() != nil {
			record(context.Cause(ctx))
			break
		}
		c.mu.Lock()
		_, known := c.ids[entry.Key]
		c.mu.Unlock()
		if !known {
			record(c.create(ctx, "reconcile", entry))
		}
	}
	minimumKills := max(int64(1), int64(o.FaultDuration/o.KillInterval)-1)
	if s.supervisor.Kills.Load() < minimumKills {
		record(errors.New("insufficient worker kills for fault duration"))
	}
	c.mu.Lock()
	duplicateCount := c.counts["duplicate"]
	c.mu.Unlock()
	if duplicateCount == 0 {
		record(errors.New("no repeated POSTs exercised"))
	}
	if len(c.cancellations()) == 0 {
		record(errors.New("no cancellation acknowledged; chaos coverage incomplete"))
	}
	return errors.Join(failures...)
}
