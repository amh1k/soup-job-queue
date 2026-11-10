package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/amh1k/soup-job-queue/internal/jobs"
)

const testID = "00000000-0000-0000-0000-000000000001"

type stubStore struct {
	create func(context.Context, string, jobs.CreateInput) (jobs.Job, bool, error)
	get    func(context.Context, string) (jobs.Job, error)
	list   func(context.Context, int, *jobs.Cursor, jobs.Status) (jobs.Page, error)
	cancel func(context.Context, string) (jobs.Job, error)
}

func (s stubStore) Create(c context.Context, k string, i jobs.CreateInput) (jobs.Job, bool, error) {
	return s.create(c, k, i)
}
func (s stubStore) Get(c context.Context, id string) (jobs.Job, error) { return s.get(c, id) }
func (s stubStore) List(c context.Context, n int, p *jobs.Cursor, status jobs.Status) (jobs.Page, error) {
	return s.list(c, n, p, status)
}
func (s stubStore) Cancel(c context.Context, id string) (jobs.Job, error) { return s.cancel(c, id) }
func request(t *testing.T, s jobs.Store, method, path, body, key string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	NewAPIHandler(s, func(context.Context) error { return nil }, time.Second).ServeHTTP(w, r)
	return w
}
func TestCreateValidation(t *testing.T) {
	for _, tc := range []struct {
		body, key string
		status    int
	}{
		{`{"duration_ms":1}`, "", 400}, {`{"duration_ms":1}`, strings.Repeat("é", 65), 400},
		{`{}`, "k", 400}, {`null`, "k", 400}, {`{"duration_ms":null}`, "k", 400},
		{`{"duration_ms":0}`, "k", 400}, {`{"duration_ms":5001}`, "k", 400},
		{`{"duration_ms":1.5}`, "k", 400}, {`{"duration_ms":1,"unknown":1}`, "k", 400},
		{`{"duration_ms":1} {}`, "k", 400}, {`{"duration_ms":1}garbage`, "k", 400},
		{`{"duration_ms":1,"simulate_failure":null}`, "k", 400},
		{`{"duration_ms":1,"simulate_failure":"true"}`, "k", 400},
		{`{"duration_ms":1,"simulate_failure":` + strings.Repeat(" ", 17<<10) + `false}`, "k", 413},
	} {
		t.Run(tc.body[:min(len(tc.body), 60)]+tc.key[:min(len(tc.key), 8)], func(t *testing.T) {
			w := request(t, stubStore{}, "POST", "/jobs", tc.body, tc.key)
			if w.Code != tc.status {
				t.Fatalf("got %d %s", w.Code, w.Body.String())
			}
		})
	}
}
func TestCreateDefaultsAndReplay(t *testing.T) {
	for _, created := range []bool{true, false} {
		s := stubStore{create: func(ctx context.Context, key string, input jobs.CreateInput) (jobs.Job, bool, error) {
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("missing deadline")
			}
			if input != (jobs.CreateInput{DurationMS: 1}) || key != "key" {
				t.Fatalf("unexpected input: %+v", input)
			}
			return jobs.Job{ID: testID, Status: jobs.Queued}, created, nil
		}}
		for _, body := range []string{`{"duration_ms":1}`, `{"simulate_failure":false,"duration_ms":1}`} {
			w := request(t, s, "POST", "/jobs", body, "key")
			want := 200
			if created {
				want = 201
			}
			if w.Code != want || w.Header().Get("Location") != "/jobs/"+testID {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
		}
	}
}
func TestJobErrors(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
	}{
		{jobs.ErrNotFound, 404}, {jobs.ErrIdempotencyConflict, 409}, {jobs.ErrTerminal, 409}, {jobs.ErrInvalidInput, 400},
		{context.DeadlineExceeded, 503}, {errors.New("secret database failure"), 500},
	} {
		s := stubStore{get: func(context.Context, string) (jobs.Job, error) { return jobs.Job{}, tc.err }}
		w := request(t, s, "GET", "/jobs/"+testID, "", "")
		if w.Code != tc.status || strings.Contains(w.Body.String(), "secret") {
			t.Fatalf("%d %s", w.Code, w.Body.String())
		}
	}
}
func TestPaginationValidationAndRoundTrip(t *testing.T) {
	cursor := jobs.Cursor{CreatedAt: time.Now().UTC().Truncate(time.Microsecond), ID: testID, Status: jobs.Queued}
	data, _ := json.Marshal(cursor)
	encoded := base64.RawURLEncoding.EncodeToString(data)
	s := stubStore{list: func(ctx context.Context, limit int, c *jobs.Cursor, status jobs.Status) (jobs.Page, error) {
		if limit != 1 || status != jobs.Queued || c == nil || !c.CreatedAt.Equal(cursor.CreatedAt) || c.ID != testID {
			t.Fatalf("unexpected query: %d %+v %s", limit, c, status)
		}
		return jobs.Page{Jobs: []jobs.Job{{ID: testID}}, NextCursor: &cursor}, nil
	}}
	w := request(t, s, "GET", "/jobs?limit=1&status=queued&cursor="+encoded, "", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), encoded) {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	for _, query := range []string{"limit=0", "limit=101", "limit=bad", "limit=1&limit=2", "status=", "status=bad", "cursor=", "cursor=bad", "cursor=" + encoded, "cursor=" + encoded + "&status=failed", "unknown=1", "limit=%zz"} {
		w := request(t, stubStore{}, "GET", "/jobs?"+query, "", "")
		if w.Code != 400 {
			t.Fatalf("query %s: %d", query, w.Code)
		}
	}
}
func TestGetCancelAndMethodValidation(t *testing.T) {
	for _, path := range []string{"/jobs/not-a-uuid", "/jobs/not-a-uuid/cancel"} {
		method := "GET"
		if strings.HasSuffix(path, "cancel") {
			method = "POST"
		}
		if w := request(t, stubStore{}, method, path, "", ""); w.Code != 400 {
			t.Fatalf("status %d", w.Code)
		}
	}
	s := stubStore{cancel: func(ctx context.Context, id string) (jobs.Job, error) {
		return jobs.Job{ID: id, Status: jobs.Cancelled}, nil
	}}
	if w := request(t, s, "POST", "/jobs/"+testID+"/cancel", "", ""); w.Code != 200 {
		t.Fatalf("status %d", w.Code)
	}
	if w := request(t, s, "DELETE", "/jobs", "", ""); w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status %d", w.Code)
	}
}
