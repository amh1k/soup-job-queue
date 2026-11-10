package experiment

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/amh1k/soup-job-queue/internal/audit"
	"github.com/amh1k/soup-job-queue/internal/jobs"
)

type response struct {
	Status         int
	Body           string
	Job            jobs.Job
	Error          string
	Started, Ended time.Time
}
type client struct {
	base      string
	http      *http.Client
	evidence  *evidence
	mu        sync.Mutex
	ids       map[string]string
	keys      []string
	cancelled map[string]bool
	counts    map[string]int
}

func newClient(base string, e *evidence) *client {
	return &client{base: base, http: &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{MaxIdleConns: 512, MaxIdleConnsPerHost: 256, MaxConnsPerHost: 512}}, evidence: e, ids: map[string]string{}, cancelled: map[string]bool{}}
}
func (c *client) request(ctx context.Context, phase, method, path, key string, input *jobs.CreateInput) response {
	var data []byte
	if input != nil {
		data, _ = json.Marshal(input)
	}
	result := response{Started: time.Now()}
	request, err := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(data))
	if err == nil {
		request.Header.Set("Content-Type", "application/json")
		if key != "" {
			request.Header.Set("Idempotency-Key", key)
		}
		var reply *http.Response
		reply, err = c.http.Do(request)
		if err == nil {
			result.Status = reply.StatusCode
			body, readErr := io.ReadAll(io.LimitReader(reply.Body, 1<<20))
			_ = reply.Body.Close()
			result.Body = string(body)
			err = readErr
			if err == nil && (method == "POST" || method == "GET") && path != "/jobs" {
				_ = json.Unmarshal(body, &result.Job)
			}
			if path == "/jobs" && method == "POST" {
				_ = json.Unmarshal(body, &result.Job)
			}
		}
	}
	result.Ended = time.Now()
	if err != nil {
		result.Error = err.Error()
	}
	c.mu.Lock()
	if c.counts == nil {
		c.counts = map[string]int{}
	}
	c.counts[phase]++
	c.mu.Unlock()
	c.evidence.event("http", map[string]any{"phase": phase, "method": method, "path": path, "key": key, "input": input, "response": result})
	return result
}
func (c *client) create(ctx context.Context, phase string, entry audit.Entry) error {
	var last response
	for attempt := 0; attempt < 5; attempt++ {
		last = c.request(ctx, phase, "POST", "/jobs", entry.Key, &entry.Input)
		if err := c.accept(entry, last); err == nil {
			return nil
		}
		if last.Error == "" && last.Status < 500 && last.Status != 0 {
			break
		}
		if !sleep(ctx, time.Duration(attempt+1)*50*time.Millisecond) {
			break
		}
	}
	return fmt.Errorf("create %s failed: status=%d transport=%s", entry.Key, last.Status, last.Error)
}
func (c *client) accept(entry audit.Entry, result response) error {
	if result.Error != "" || (result.Status != 200 && result.Status != 201) || !jobs.ValidID(result.Job.ID) || result.Job.Request != entry.Input {
		return fmt.Errorf("invalid creation response")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if id, exists := c.ids[entry.Key]; exists {
		if id != result.Job.ID {
			return fmt.Errorf("same key returned multiple IDs")
		}
	} else {
		c.keys = append(c.keys, entry.Key)
		c.ids[entry.Key] = result.Job.ID
	}
	return nil
}
func (c *client) id(index int) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.keys) == 0 {
		return ""
	}
	return c.ids[c.keys[index%len(c.keys)]]
}
func (c *client) cancelJob(ctx context.Context, phase, id string) bool {
	result := c.request(ctx, phase, "POST", "/jobs/"+id+"/cancel", "", nil)
	if c.acceptCancel(result, id) {
		return true
	}
	return result.Error == "" && result.Status == 409
}
func (c *client) cancellations() map[string]bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	copy := map[string]bool{}
	for id := range c.cancelled {
		copy[id] = true
	}
	return copy
}

func (c *client) acceptCancel(result response, id string) bool {
	if result.Error != "" || result.Status != 200 || result.Job.Status != jobs.Cancelled || result.Job.ID != id {
		return false
	}
	c.mu.Lock()
	c.cancelled[id] = true
	c.mu.Unlock()
	return true
}
