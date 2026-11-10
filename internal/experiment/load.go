package experiment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/amh1k/soup-job-queue/internal/audit"
	"github.com/amh1k/soup-job-queue/internal/jobs"
)

type operation struct {
	Kind        string
	Entry, Pick int
}
type loadSample struct {
	Phase, Kind               string
	Scheduled, Started, Ended time.Time
	Status                    int
	Error                     string
	Dropped                   bool
}
type LoadReport struct {
	Successful                               int
	SuccessfulRPS                            float64
	ExecutionCompletionsPerSecond            float64
	Pass                                     bool
	Scheduled, Sent, Dropped, Errors         int
	ActualRPS                                float64
	LatencyP50MS, LatencyP95MS, LatencyP99MS float64
	ScheduledLatencyP95MS, ScheduleLagP95MS  float64
	BacklogSlopePerSecond                    float64
	FirstBacklog, LastBacklog                int
	MetricsSamples                           int
	Failures                                 []string
}

func loadPlan(o Options) ([]audit.Entry, []operation) {
	entries := manifest(32, o.Seed, 1, 1, false)
	random := rand.New(rand.NewSource(o.Seed))
	count := int((o.Warmup + o.Duration).Seconds() * float64(o.RPS))
	operations := make([]operation, count)
	creations := 0
	previous := 0
	for i := range operations {
		op := operation{Kind: "create", Pick: random.Intn(1 << 30)}
		if o.Mode == "mixed" {
			switch i % 10 {
			case 5, 6, 7:
				op.Kind = "read"
			case 8:
				op.Kind = "list"
			case 9:
				op.Kind = "cancel"
			}
		}
		if op.Kind == "create" {
			creations++
			if o.Mode == "mixed" && creations%10 == 0 {
				op.Entry = previous
			} else {
				op.Entry = len(entries)
				previous = op.Entry
				entries = append(entries, audit.Entry{Key: fmt.Sprintf("load-%d-%08d", o.Seed, i), Input: jobs.CreateInput{DurationMS: 50 + random.Intn(101)}})
			}
		}
		operations[i] = op
	}
	return entries, operations
}
func runLoad(ctx context.Context, o Options, s *session, c *client, entries []audit.Entry, operations []operation) (*LoadReport, error) {
	for _, entry := range entries[:32] {
		if err := c.create(ctx, "bootstrap", entry); err != nil {
			return nil, err
		}
	}
	start := time.Now()
	measureStart := start.Add(o.Warmup)
	measureEnd := measureStart.Add(o.Duration)
	var mu sync.Mutex
	var samples []loadSample
	var wg sync.WaitGroup
	slots := make(chan struct{}, o.MaxInflight)
	record := func(row loadSample) {
		mu.Lock()
		samples = append(samples, row)
		mu.Unlock()
		c.evidence.event("load_sample", row)
	}
	interval := time.Second / time.Duration(o.RPS)
	for i, op := range operations {
		scheduled := start.Add(time.Duration(i) * interval)
		if !sleep(ctx, time.Until(scheduled)) {
			break
		}
		phase := "measurement"
		if scheduled.Before(measureStart) {
			phase = "warmup"
		}
		select {
		case slots <- struct{}{}:
			wg.Add(1)
			go func(op operation, scheduled time.Time, phase string) {
				defer wg.Done()
				defer func() { <-slots }()
				result := response{}
				entry := audit.Entry{}
				switch op.Kind {
				case "create":
					entry = entries[op.Entry]
					result = c.request(ctx, phase, "POST", "/jobs", entry.Key, &entry.Input)
					if err := c.accept(entry, result); err != nil && result.Error == "" {
						result.Error = err.Error()
					}
				case "read":
					id := c.id(op.Pick)
					result = c.request(ctx, phase, "GET", "/jobs/"+id, "", nil)
					if result.Error == "" && result.Status == 200 && (result.Job.ID != id || !jobs.ValidStatus(result.Job.Status)) {
						result.Error = "invalid job read response"
					}
				case "list":
					result = c.request(ctx, phase, "GET", "/jobs?limit=10", "", nil)
					var page struct {
						Jobs []jobs.Job `json:"jobs"`
					}
					if result.Error == "" && result.Status == 200 && (json.Unmarshal([]byte(result.Body), &page) != nil || page.Jobs == nil) {
						result.Error = "invalid job list response"
					}
				case "cancel":
					id := c.id(op.Pick)
					result = c.request(ctx, phase, "POST", "/jobs/"+id+"/cancel", "", nil)
					if result.Error == "" && result.Status == 200 {
						if !c.acceptCancel(result, id) {
							result.Error = "invalid cancellation response"
						}
					}
				}
				if result.Error == "" {
					valid := result.Status == 200
					if op.Kind == "create" {
						valid = result.Status == 200 || result.Status == 201
					}
					if op.Kind == "cancel" {
						valid = result.Status == 200 || result.Status == 409
					}
					if !valid {
						result.Error = fmt.Sprintf("unexpected HTTP %d", result.Status)
					}
				}
				record(loadSample{Phase: phase, Kind: op.Kind, Scheduled: scheduled, Started: result.Started, Ended: result.Ended, Status: result.Status, Error: result.Error})
			}(op, scheduled, phase)
		default:
			record(loadSample{Phase: phase, Kind: op.Kind, Scheduled: scheduled, Dropped: true, Error: "inflight limit exceeded"})
		}
	}
	sleep(ctx, time.Until(measureEnd))
	wg.Wait()
	s.mu.Lock()
	metrics := append([]sample(nil), s.samples...)
	s.mu.Unlock()
	report := assessLoad(samples, metrics, o, measureStart, measureEnd)
	if err := c.evidence.json("load-report.json", report); err != nil {
		return &report, err
	}
	// Retries are outside the measured arrival stream. Preserve the measurement's
	// failures while reconciling ambiguous/missed creations with their same keys.
	var reconcileErr error
	for _, entry := range entries {
		if ctx.Err() != nil {
			reconcileErr = errors.Join(reconcileErr, context.Cause(ctx))
			break
		}
		c.mu.Lock()
		_, exists := c.ids[entry.Key]
		c.mu.Unlock()
		if !exists {
			if err := c.create(ctx, "post-load-reconcile", entry); err != nil {
				reconcileErr = errors.Join(reconcileErr, err)
			}
		}
	}
	if !report.Pass {
		reconcileErr = errors.Join(reconcileErr, errors.New("load acceptance targets failed"))
	}
	return &report, reconcileErr
}
func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	index := max(0, int(math.Ceil(p*float64(len(values))))-1)
	return values[min(index, len(values)-1)]
}
func assessLoad(rows []loadSample, metrics []sample, o Options, start, end time.Time) LoadReport {
	result := LoadReport{Failures: []string{}}
	var latency, scheduledLatency, lag []float64
	for _, row := range rows {
		if row.Phase != "measurement" {
			continue
		}
		result.Scheduled++
		if row.Dropped {
			result.Dropped++
			continue
		}
		if !row.Started.Before(start) && row.Started.Before(end) {
			result.Sent++
			if row.Error == "" {
				result.Successful++
			}
		}
		if row.Error != "" {
			result.Errors++
		}
		latency = append(latency, float64(row.Ended.Sub(row.Started))/float64(time.Millisecond))
		scheduledLatency = append(scheduledLatency, float64(row.Ended.Sub(row.Scheduled))/float64(time.Millisecond))
		lag = append(lag, float64(row.Started.Sub(row.Scheduled))/float64(time.Millisecond))
	}
	result.ActualRPS = float64(result.Sent) / o.Duration.Seconds()
	result.SuccessfulRPS = float64(result.Successful) / o.Duration.Seconds()
	result.LatencyP50MS = percentile(latency, .5)
	result.LatencyP95MS = percentile(latency, .95)
	result.LatencyP99MS = percentile(latency, .99)
	result.ScheduledLatencyP95MS = percentile(scheduledLatency, .95)
	result.ScheduleLagP95MS = percentile(lag, .95)
	var points []sample
	for _, row := range metrics {
		if !row.Time.Before(start) && row.Time.Before(end) {
			points = append(points, row)
		}
	}
	result.MetricsSamples = len(points)
	if len(points) >= 2 {
		result.FirstBacklog = points[0].Queued
		result.LastBacklog = points[len(points)-1].Queued
		first, last := points[0], points[len(points)-1]
		if span := last.Time.Sub(first.Time).Seconds(); span > 0 {
			result.ExecutionCompletionsPerSecond = float64(last.Succeeded+last.Failed-first.Succeeded-first.Failed) / span
		}
		var x, y, xx, xy float64
		for _, row := range points {
			t := row.Time.Sub(start).Seconds()
			q := float64(row.Queued)
			x += t
			y += q
			xx += t * t
			xy += t * q
		}
		n := float64(len(points))
		denominator := n*xx - x*x
		if denominator != 0 {
			result.BacklogSlopePerSecond = (n*xy - x*y) / denominator
		}
	}
	fail := func(condition bool, message string) {
		if condition {
			result.Failures = append(result.Failures, message)
		}
	}
	expected := int(o.Duration.Seconds() * float64(o.RPS))
	fail(result.Scheduled != expected, "scheduled measurement count differs from target")
	fail(result.Dropped > 0, "generator dropped scheduled requests")
	fail(result.Errors > 0, "unexpected HTTP/transport errors")
	fail(math.Abs(result.ActualRPS-float64(o.RPS)) > float64(o.RPS)*.05, "achieved send rate outside 5% of target")
	fail(result.LatencyP95MS >= 250, "p95 response latency is at least 250ms")
	fail(result.ScheduleLagP95MS >= 25, "p95 generator scheduling lag is at least 25ms")
	fail(result.MetricsSamples < 2, "insufficient queue metrics")
	fail(result.BacklogSlopePerSecond > .1 || result.LastBacklog > result.FirstBacklog+10, "sustained backlog growth")
	result.Pass = len(result.Failures) == 0
	return result
}
