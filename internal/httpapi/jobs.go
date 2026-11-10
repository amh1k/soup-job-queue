package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/amh1k/soup-job-queue/internal/jobs"
)

// NewAPIHandler mounts job routes alongside the existing health endpoints.
func NewAPIHandler(store jobs.Store, ready func(context.Context) error, timeout time.Duration) http.Handler {
	api := jobAPI{store: store}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", api.create)
	mux.HandleFunc("GET /jobs", api.list)
	mux.HandleFunc("GET /jobs/{id}", api.get)
	mux.HandleFunc("POST /jobs/{id}/cancel", api.cancel)
	mux.HandleFunc("/jobs", methodNotAllowed("GET, HEAD, POST"))
	mux.HandleFunc("/jobs/{id}", methodNotAllowed("GET, HEAD"))
	mux.HandleFunc("/jobs/{id}/cancel", methodNotAllowed("POST"))
	mux.Handle("/", NewHandler(ready, timeout))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}

type jobAPI struct{ store jobs.Store }

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func apiError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}
func storeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, jobs.ErrNotFound):
		apiError(w, 404, "not_found", "Job not found")
	case errors.Is(err, jobs.ErrInvalidInput):
		apiError(w, 400, "invalid_input", "Invalid job input")
	case errors.Is(err, jobs.ErrIdempotencyConflict):
		apiError(w, 409, "idempotency_conflict", "Key already used with different input")
	case errors.Is(err, jobs.ErrTerminal):
		apiError(w, 409, "already_completed", "Job has already completed")
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		apiError(w, 503, "unavailable", "Request could not complete; retry with the same idempotency key when creating a job")
	default:
		slog.Error("job operation failed", "error", err)
		apiError(w, 500, "internal_error", "Unable to process request")
	}
}
func (api jobAPI) create(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if len(r.Header.Values("Idempotency-Key")) != 1 || strings.TrimSpace(key) == "" || len(key) > 128 {
		apiError(w, 400, "invalid_input", "Supply one Idempotency-Key of 1–128 bytes")
		return
	}
	if r.Header.Get("Content-Type") != "" {
		media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || media != "application/json" {
			apiError(w, 415, "unsupported_media_type", "Use application/json")
			return
		}
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	// Pointers distinguish omitted defaults from explicitly invalid/null input.
	var body struct {
		DurationMS      *int            `json:"duration_ms"`
		SimulateFailure json.RawMessage `json:"simulate_failure"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	err := decoder.Decode(&body)
	if err == nil {
		var trailing any
		if tailErr := decoder.Decode(&trailing); !errors.Is(tailErr, io.EOF) {
			if tailErr == nil {
				err = jobs.ErrInvalidInput
			} else {
				err = tailErr
			}
		}
	}
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			apiError(w, 413, "body_too_large", "Body exceeds 16 KiB")
		} else {
			apiError(w, 400, "invalid_input", "Supply one JSON object with known fields")
		}
		return
	}
	if body.DurationMS == nil || *body.DurationMS < 1 || *body.DurationMS > 5000 {
		apiError(w, 400, "invalid_input", "duration_ms must be an integer between 1 and 5000")
		return
	}
	input := jobs.CreateInput{DurationMS: *body.DurationMS}
	if body.SimulateFailure != nil {
		if string(body.SimulateFailure) == "null" || json.Unmarshal(body.SimulateFailure, &input.SimulateFailure) != nil {
			apiError(w, 400, "invalid_input", "simulate_failure must be a boolean")
			return
		}
	}
	j, created, err := api.store.Create(r.Context(), key, input)
	if err != nil {
		storeError(w, err)
		return
	}
	w.Header().Set("Location", "/jobs/"+j.ID)
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, j)
}
func validPath(w http.ResponseWriter, r *http.Request) bool {
	if !jobs.ValidID(r.PathValue("id")) {
		apiError(w, 400, "invalid_input", "Invalid job UUID")
		return false
	}
	return true
}
func (api jobAPI) get(w http.ResponseWriter, r *http.Request) {
	if !validPath(w, r) {
		return
	}
	j, err := api.store.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, 200, j)
}
func (api jobAPI) cancel(w http.ResponseWriter, r *http.Request) {
	if !validPath(w, r) {
		return
	}
	j, err := api.store.Cancel(r.Context(), r.PathValue("id"))
	if err != nil {
		storeError(w, err)
		return
	}
	writeJSON(w, 200, j)
}
func (api jobAPI) list(w http.ResponseWriter, r *http.Request) {
	query, err := parseListQuery(r)
	if err != nil {
		apiError(w, 400, "invalid_input", "Invalid pagination limit, cursor, or status")
		return
	}
	page, err := api.store.List(r.Context(), query.limit, query.cursor, query.status)
	if err != nil {
		storeError(w, err)
		return
	}
	var next *string
	if page.NextCursor != nil {
		data, _ := json.Marshal(page.NextCursor)
		encoded := base64.RawURLEncoding.EncodeToString(data)
		next = &encoded
	}
	if page.Jobs == nil {
		page.Jobs = []jobs.Job{}
	}
	writeJSON(w, 200, struct {
		Jobs       []jobs.Job `json:"jobs"`
		NextCursor *string    `json:"next_cursor"`
	}{page.Jobs, next})
}

type listQuery struct {
	limit  int
	cursor *jobs.Cursor
	status jobs.Status
}

func parseListQuery(r *http.Request) (listQuery, error) {
	query := listQuery{limit: 50}
	values, err := parseQuery(r)
	if err != nil {
		return query, err
	}
	if v, ok := values["limit"]; ok {
		query.limit, err = strconv.Atoi(v)
		if err != nil || query.limit < 1 || query.limit > 100 {
			return query, jobs.ErrInvalidInput
		}
	}
	if v, ok := values["status"]; ok {
		query.status = jobs.Status(v)
		if !jobs.ValidStatus(query.status) {
			return query, jobs.ErrInvalidInput
		}
	}
	if v, ok := values["cursor"]; ok {
		if len(v) > 1024 || v == "" {
			return query, jobs.ErrInvalidInput
		}
		data, err := base64.RawURLEncoding.Strict().DecodeString(v)
		if err != nil {
			return query, jobs.ErrInvalidInput
		}
		var cursor jobs.Cursor
		decoder := json.NewDecoder(strings.NewReader(string(data)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&cursor); err != nil {
			return query, jobs.ErrInvalidInput
		}
		var trailing any
		if !errors.Is(decoder.Decode(&trailing), io.EOF) || !jobs.ValidID(cursor.ID) || cursor.CreatedAt.IsZero() || cursor.Status != query.status {
			return query, jobs.ErrInvalidInput
		}
		query.cursor = &cursor
	}
	return query, nil
}

func parseQuery(r *http.Request) (map[string]string, error) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, jobs.ErrInvalidInput
	}
	result := make(map[string]string)
	for key, vs := range values {
		if (key != "limit" && key != "cursor" && key != "status") || len(vs) != 1 {
			return nil, jobs.ErrInvalidInput
		}
		result[key] = vs[0]
	}
	return result, nil
}
func methodNotAllowed(allow string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Allow", allow)
		apiError(w, 405, "method_not_allowed", "HTTP method is not supported")
	}
}
