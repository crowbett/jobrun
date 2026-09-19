package admin

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/crowbett/jobrun/internal/job"
)

// waitFor polls cond until true or the deadline.
func waitFor(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

// sseClient reads events from an open SSE response body.
type sseClient struct {
	resp *http.Response
	sc   *bufio.Scanner
}

func connect(t *testing.T, url string) *sseClient {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return &sseClient{resp: resp, sc: bufio.NewScanner(resp.Body)}
}

// next returns the next (event, data) pair, skipping comments.
func (c *sseClient) next(t *testing.T) (string, string) {
	t.Helper()
	var event, data string
	for c.sc.Scan() {
		line := c.sc.Text()
		switch {
		case line == "":
			if event != "" {
				return event, data
			}
		case strings.HasPrefix(line, ":"):
			// comment/keepalive
		case strings.HasPrefix(line, "event: "):
			event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			data = strings.TrimPrefix(line, "data: ")
		default:
			t.Fatalf("unexpected SSE line %q", line)
		}
	}
	t.Fatalf("stream ended: %v", c.sc.Err())
	return "", ""
}

func staticSnapshot(rows []job.DepthRow) func(context.Context) ([]job.DepthRow, error) {
	return func(context.Context) ([]job.DepthRow, error) { return rows, nil }
}

func TestEventsSnapshotThenStream(t *testing.T) {
	t.Parallel()
	b := NewBroker(nil)
	rows := []job.DepthRow{
		{Queue: "default", Status: job.StatusQueued, N: 3, OldestRunAt: fixedNow},
		{Queue: "default", Status: job.StatusDead, N: 0},
	}
	srv := httptest.NewServer(Events(b, staticSnapshot(rows)))
	t.Cleanup(srv.Close)

	c := connect(t, srv.URL+"/admin/events")
	if c.resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", c.resp.StatusCode)
	}
	for k, want := range map[string]string{
		"Content-Type":                "text/event-stream",
		"Cache-Control":               "no-cache",
		"X-Accel-Buffering":           "no",
		"Access-Control-Allow-Origin": "*",
	} {
		if got := c.resp.Header.Get(k); got != want {
			t.Errorf("header %s = %q, want %q", k, got, want)
		}
	}

	event, data := c.next(t)
	if event != "snapshot" {
		t.Fatalf("first event = %q, want snapshot", event)
	}
	var snap []map[string]any
	if err := json.Unmarshal([]byte(data), &snap); err != nil {
		t.Fatalf("snapshot json %q: %v", data, err)
	}
	if len(snap) != 2 || snap[0]["queue"] != "default" || snap[0]["status"] != "queued" || snap[0]["n"] != float64(3) {
		t.Errorf("snapshot = %v", snap)
	}
	if _, ok := snap[0]["oldest_run_at"]; !ok {
		t.Error("snapshot[0] lacks oldest_run_at")
	}
	if _, ok := snap[1]["oldest_run_at"]; ok {
		t.Error("empty cell carries oldest_run_at")
	}

	if !waitFor(t, time.Second, func() bool { return b.Subscribers() == 1 }) {
		t.Fatalf("Subscribers = %d, want 1 while connected", b.Subscribers())
	}
	b.Publish(job.Event{JobID: testID, Queue: "default", Type: "email", From: job.StatusLeased, To: job.StatusQueued, Attempt: 2, At: fixedNow, Err: "boom"})
	event, data = c.next(t)
	if event != "job" {
		t.Fatalf("second event = %q, want job", event)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(data), &got); err != nil {
		t.Fatalf("job json %q: %v", data, err)
	}
	if got["job_id"] != testID.String() || got["from"] != "leased" || got["to"] != "queued" || got["attempt"] != float64(2) || got["error"] != "boom" {
		t.Errorf("job event = %v", got)
	}
	if _, ok := got["at"]; !ok {
		t.Error("job event lacks at")
	}
}

func TestEventsDisconnectUnsubscribes(t *testing.T) {
	t.Parallel()
	b := NewBroker(nil)
	srv := httptest.NewServer(Events(b, staticSnapshot(nil)))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/admin/events", http.NoBody)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	if _, err := resp.Body.Read(buf); err != nil {
		t.Fatalf("read first bytes: %v", err)
	}
	if got := b.Subscribers(); got != 1 {
		t.Fatalf("Subscribers = %d, want 1", got)
	}
	cancel()
	resp.Body.Close()
	if !waitFor(t, 2*time.Second, func() bool { return b.Subscribers() == 0 }) {
		t.Fatalf("Subscribers = %d after disconnect, want 0", b.Subscribers())
	}
}

func TestEventsKeepaliveWhenIdle(t *testing.T) {
	t.Parallel()
	b := NewBroker(nil)
	h := &sseHandler{broker: b, snapshot: staticSnapshot(nil), keepalive: 20 * time.Millisecond}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	c := connect(t, srv.URL+"/admin/events")
	c.next(t) // snapshot
	deadline := time.Now().Add(2 * time.Second)
	for c.sc.Scan() {
		if c.sc.Text() == ": keepalive" {
			return
		}
		if time.Now().After(deadline) {
			break
		}
	}
	t.Fatalf("no keepalive seen: %v", c.sc.Err())
}

func TestEventsSnapshotErrorStillStreams(t *testing.T) {
	t.Parallel()
	b := NewBroker(nil)
	srv := httptest.NewServer(Events(b, func(context.Context) ([]job.DepthRow, error) { return nil, errors.New("db down") }))
	t.Cleanup(srv.Close)
	c := connect(t, srv.URL+"/admin/events")
	event, data := c.next(t)
	if event != "snapshot" || data != "[]" {
		t.Fatalf("got %q %q, want an empty snapshot", event, data)
	}
}

// noFlush hides Flusher, like http.TimeoutHandler's writer does.
type noFlush struct{ http.ResponseWriter }

func TestEventsNonFlushingWriterIs500(t *testing.T) {
	t.Parallel()
	rr := httptest.NewRecorder()
	Events(NewBroker(nil), staticSnapshot(nil)).ServeHTTP(noFlush{rr}, httptest.NewRequest(http.MethodGet, "/admin/events", http.NoBody))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rr.Code)
	}
}

func TestEventsInsideTimeoutHandlerFails(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.TimeoutHandler(Events(NewBroker(nil), staticSnapshot(nil)), time.Second, "timeout"))
	t.Cleanup(srv.Close)
	c := connect(t, srv.URL+"/admin/events")
	body, _ := io.ReadAll(c.resp.Body)
	if c.resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status inside TimeoutHandler = %d (%s), want 500: mount /admin/events outside the timeout middleware", c.resp.StatusCode, body)
	}
}
