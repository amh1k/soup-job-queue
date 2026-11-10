package experiment

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type child struct {
	process    *os.Process
	generation int
	planned    bool
}
type exited struct {
	slot, generation int
	state            *os.ProcessState
	err              error
}
type supervisor struct {
	mu                sync.Mutex
	slots             []child
	exits             chan exited
	binary, url       string
	evidence          *evidence
	Kills, Unexpected atomic.Int64
}

func newSupervisor(binary, url string, n int, e *evidence) *supervisor {
	return &supervisor{slots: make([]child, n), exits: make(chan exited, n*2), binary: binary, url: url, evidence: e}
}
func environment(extra map[string]string) []string {
	result := []string{}
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if _, ok := extra[key]; !ok {
			result = append(result, entry)
		}
	}
	for key, value := range extra {
		result = append(result, key+"="+value)
	}
	return result
}
func (s *supervisor) start(slot int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	generation := s.slots[slot].generation + 1
	logFile, err := os.OpenFile(filepath.Join(s.evidence.dir, fmt.Sprintf("worker-%d-%d.log", slot, generation)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	cmd := exec.Command(s.binary)
	cmd.Env = environment(map[string]string{"DATABASE_URL": s.url, "WORKER_ID": fmt.Sprintf("experiment-%d-%d", slot, generation), "DB_MAX_CONNS": "4", "DB_TIMEOUT": "3s", "LEASE_DURATION": "10s", "HEARTBEAT_INTERVAL": "2s", "POLL_INTERVAL": "100ms", "RECOVERY_INTERVAL": "1s", "RECOVERY_BATCH": "100", "HTTP_ADDR": ":8080"})
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return err
	}
	s.slots[slot] = child{process: cmd.Process, generation: generation}
	s.evidence.event("worker_started", map[string]any{"slot": slot, "generation": generation, "pid": cmd.Process.Pid})
	go func() {
		err := cmd.Wait()
		_ = logFile.Close()
		s.exits <- exited{slot: slot, generation: generation, state: cmd.ProcessState, err: err}
	}()
	return nil
}
func (s *supervisor) run(ctx context.Context, ready chan<- error) error {
	for i := range s.slots {
		if err := s.start(i); err != nil {
			ready <- err
			s.stop()
			return err
		}
	}
	ready <- nil
	for {
		select {
		case <-ctx.Done():
			s.stop()
			return nil
		case exit := <-s.exits:
			planned := s.record(exit)
			if !planned {
				s.Unexpected.Add(1)
				s.stop()
				return fmt.Errorf("worker slot %d exited unexpectedly; inspect its log", exit.slot)
			}
			if ctx.Err() != nil {
				s.stop()
				return nil
			}
			if err := s.start(exit.slot); err != nil {
				s.stop()
				return err
			}
		}
	}
}
func (s *supervisor) record(exit exited) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.slots[exit.slot]
	planned := c.planned
	if c.generation == exit.generation {
		s.slots[exit.slot].process = nil
	}
	event := map[string]any{"slot": exit.slot, "generation": exit.generation, "planned_kill": planned}
	if exit.err != nil {
		event["error"] = exit.err.Error()
	}
	if exit.state != nil {
		event["exit_code"] = exit.state.ExitCode()
		event["user_cpu_ns"] = exit.state.UserTime().Nanoseconds()
		event["system_cpu_ns"] = exit.state.SystemTime().Nanoseconds()
		if usage, ok := exit.state.SysUsage().(*syscall.Rusage); ok {
			event["max_rss_kib_linux"] = usage.Maxrss
		}
	}
	s.evidence.event("worker_exited", event)
	return planned
}
func (s *supervisor) killRandom(random *rand.Rand) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var live []int
	for i, c := range s.slots {
		if c.process != nil && !c.planned {
			live = append(live, i)
		}
	}
	if len(live) == 0 {
		return fmt.Errorf("no live worker to kill")
	}
	slot := live[random.Intn(len(live))]
	c := &s.slots[slot]
	if err := c.process.Kill(); err != nil {
		return err
	}
	c.planned = true
	s.Kills.Add(1)
	s.evidence.event("worker_sigkill", map[string]int{"slot": slot, "pid": c.process.Pid, "generation": c.generation})
	return nil
}
func (s *supervisor) stop() {
	s.mu.Lock()
	count := 0
	for _, c := range s.slots {
		if c.process != nil {
			count++
			_ = c.process.Signal(syscall.SIGTERM)
		}
	}
	s.mu.Unlock()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for count > 0 {
		select {
		case exit := <-s.exits:
			s.record(exit)
			count--
		case <-timer.C:
			s.mu.Lock()
			for _, c := range s.slots {
				if c.process != nil {
					_ = c.process.Kill()
				}
			}
			s.mu.Unlock()
		}
	}
}
func (s *supervisor) live() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.slots {
		if c.process != nil {
			n++
		}
	}
	return n
}
func (s *supervisor) settings() map[string]string {
	return map[string]string{"workers": strconv.Itoa(len(s.slots)), "lease": "10s", "heartbeat": "2s", "poll": "100ms", "recovery": "1s", "db_timeout": "3s", "worker_pool": "4"}
}
