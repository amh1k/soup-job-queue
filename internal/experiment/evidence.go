package experiment

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

type evidence struct {
	dir  string
	mu   sync.Mutex
	file *os.File
	err  error
}

func newEvidence(root, kind string, seed int64) (*evidence, error) {
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp(root, fmt.Sprintf("%s-%s-seed%d-", kind, time.Now().UTC().Format("20060102T150405Z"), seed))
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(filepath.Join(dir, "events.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	e := &evidence{dir: dir, file: file}
	git := func(args ...string) string {
		out, _ := exec.Command("git", args...).Output()
		return strings.TrimSpace(string(out))
	}
	diff := git("diff", "HEAD")
	if err := e.write("changes.patch", []byte(diff)); err != nil {
		file.Close()
		return nil, err
	}
	hashes := map[string]string{}
	paths := strings.Split(git("ls-files", "-co", "--exclude-standard"), "\n")
	for _, path := range paths {
		if strings.HasPrefix(path, "results/") || strings.HasPrefix(path, ".env") || strings.HasSuffix(path, ".pdf") {
			continue
		}
		data, err := os.ReadFile(path)
		if err == nil {
			sum := sha256.Sum256(data)
			hashes[path] = hex.EncodeToString(sum[:])
		}
	}
	metadata := map[string]any{"started_at": time.Now().UTC(), "seed": seed, "kind": kind, "commit": git("rev-parse", "HEAD"), "git_status": git("status", "--porcelain"), "source_hashes": hashes, "go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "cpus": runtime.NumCPU()}
	for _, path := range []string{"/sys/fs/cgroup/cpu.max", "/sys/fs/cgroup/memory.max", "/proc/meminfo"} {
		if b, err := os.ReadFile(path); err == nil {
			metadata[path] = string(b)
		}
	}
	if err := e.json("metadata.json", metadata); err != nil {
		file.Close()
		return nil, err
	}
	fmt.Println("Evidence:", dir)
	return e, nil
}
func (e *evidence) write(name string, data []byte) error {
	return os.WriteFile(filepath.Join(e.dir, name), data, 0644)
}
func (e *evidence) json(name string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return e.write(name, append(data, '\n'))
}
func (e *evidence) event(kind string, value any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.err == nil {
		e.err = json.NewEncoder(e.file).Encode(map[string]any{"time": time.Now().UTC(), "kind": kind, "data": value})
	}
}
func (e *evidence) close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	syncErr := e.file.Sync()
	closeErr := e.file.Close()
	if e.err != nil {
		return e.err
	}
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}
