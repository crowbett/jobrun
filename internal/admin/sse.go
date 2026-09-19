package admin

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/crowbett/jobrun/internal/job"
)

const (
	// KeepaliveInterval is how long the feed may be silent before a
	// `: keepalive` comment is sent so proxies and EventSource keep the
	// connection alive.
	KeepaliveInterval = 15 * time.Second
	// subscriberBuffer is the per-connection event backlog; the Broker drops
	// events for this connection (never blocks) once it is full.
	subscriberBuffer = 64
)

// streamHeaders are set on every successful SSE response.
var streamHeaders = map[string]string{
	"Content-Type":                "text/event-stream",
	"Cache-Control":               "no-cache",
	"Connection":                  "keep-alive",
	"X-Accel-Buffering":           "no",
	"Access-Control-Allow-Origin": "*",
}

// eventJSON is the wire shape of a transition on the feed (job.Event has no
// JSON tags; the page reads these names).
type eventJSON struct {
	JobID   job.ID     `json:"job_id"`
	Queue   string     `json:"queue"`
	Type    string     `json:"type"`
	From    job.Status `json:"from"`
	To      job.Status `json:"to"`
	Attempt int32      `json:"attempt"`
	At      time.Time  `json:"at"`
	Error   string     `json:"error,omitempty"`
}

// depthJSON is the wire shape of one depth row on the feed and /admin/stats.
type depthJSON struct {
	Queue       string     `json:"queue"`
	Status      job.Status `json:"status"`
	N           int64      `json:"n"`
	OldestRunAt *time.Time `json:"oldest_run_at,omitempty"`
}

func depthsJSON(rows []job.DepthRow) []depthJSON {
	out := make([]depthJSON, 0, len(rows))
	for _, r := range rows {
		d := depthJSON{Queue: r.Queue, Status: r.Status, N: r.N}
		if !r.OldestRunAt.IsZero() {
			t := r.OldestRunAt
			d.OldestRunAt = &t
		}
		out = append(out, d)
	}
	return out
}

// Events streams job transitions from b as Server-Sent Events: one
// `event: snapshot` (the depth table from snapshot) on connect, then
// `event: job` per transition, with a keepalive comment while idle. Responses
// carry Access-Control-Allow-Origin: * so a file:// page can subscribe.
//
// The handler holds the connection open for as long as the client stays, so
// it MUST be mounted outside any http.TimeoutHandler (whose ResponseWriter
// cannot flush and whose deadline would cut the stream); it clears the
// server's per-request write deadline for its own connection. Handler and
// NewServer mount it correctly.
func Events(b *Broker, snapshot func(context.Context) ([]job.DepthRow, error)) http.Handler {
	return &sseHandler{broker: b, snapshot: snapshot, keepalive: KeepaliveInterval}
}

type sseHandler struct {
	broker    *Broker
	snapshot  func(context.Context) ([]job.DepthRow, error)
	keepalive time.Duration
}

func (h *sseHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	hdr := w.Header()
	for k, v := range streamHeaders {
		hdr.Set(k, v)
	}

	// The first Flush commits the 200 and the headers above. A writer that
	// cannot flush (e.g. inside http.TimeoutHandler) fails here before anything
	// is sent.
	rc := http.NewResponseController(w)
	if err := rc.Flush(); err != nil {
		for k := range streamHeaders {
			hdr.Del(k)
		}
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	// The server's WriteTimeout is sized for request/response; lift it for this
	// long-lived connection. Unsupported writers simply keep their deadline.
	_ = rc.SetWriteDeadline(time.Time{})

	// Subscribe before the snapshot so no transition between the two is lost.
	ch, cancel := h.broker.Subscribe(subscriberBuffer)
	defer cancel()

	ctx := r.Context()
	enc := json.NewEncoder(w)
	send := func(event string, v any) error {
		if _, err := w.Write([]byte("event: " + event + "\ndata: ")); err != nil {
			return err
		}
		if err := enc.Encode(v); err != nil { // Encode appends "\n"
			return err
		}
		if _, err := w.Write([]byte{'\n'}); err != nil {
			return err
		}
		return rc.Flush()
	}

	rows, err := h.snapshot(ctx)
	if err != nil {
		slog.Default().WarnContext(ctx, "admin sse: snapshot failed", slog.Any("err", err))
		rows = nil // the page shows "no jobs" and the next connect retries
	}
	if err := send("snapshot", depthsJSON(rows)); err != nil {
		return
	}

	ticker := time.NewTicker(h.keepalive)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case e, ok := <-ch:
			if !ok {
				return
			}
			ev := eventJSON{JobID: e.JobID, Queue: e.Queue, Type: e.Type, From: e.From, To: e.To, Attempt: e.Attempt, At: e.At, Error: e.Err}
			if err := send("job", ev); err != nil {
				return
			}
			ticker.Reset(h.keepalive)
		case <-ticker.C:
			if _, err := w.Write([]byte(": keepalive\n\n")); err != nil {
				return
			}
			if err := rc.Flush(); err != nil {
				return
			}
		}
	}
}
