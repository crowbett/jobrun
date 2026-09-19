package server_test

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	jobrunv1 "github.com/crowbett/jobrun/gen/jobrun/v1"
	"github.com/crowbett/jobrun/internal/job"
	"github.com/crowbett/jobrun/internal/server"
)

func validEnqueue() *jobrunv1.EnqueueRequest {
	return &jobrunv1.EnqueueRequest{
		Queue:          "default",
		Type:           "email.send",
		Payload:        []byte(`{"to":"x@y.z"}`),
		Priority:       7,
		When:           &jobrunv1.EnqueueRequest_Delay{Delay: durationpb.New(90 * time.Second)},
		MaxAttempts:    5,
		Backoff:        &jobrunv1.BackoffPolicy{Base: durationpb.New(2 * time.Second), Max: durationpb.New(time.Minute), Factor: 3, Jitter: false},
		IdempotencyKey: "key-1",
	}
}

func TestEnqueue(t *testing.T) {
	t.Parallel()

	for _, tt := range jobErrCases() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var gotIn job.Job
			fake := &fakeStore{enqueue: func(_ context.Context, j job.Job) (job.Job, error) {
				gotIn = j
				if tt.svcErr != nil {
					return job.Job{}, tt.svcErr
				}
				stored := sampleJob()
				stored.ID = j.ID // fresh insert: the store returns our row
				return stored, nil
			}}
			c := newTestClients(t, fake)

			resp, err := c.jobs.Enqueue(ctxWithRequestID(t, "req-enqueue-"+tt.name), validEnqueue())
			checkStatus(t, err, tt.wantCode, c.logs, "req-enqueue-"+tt.name)
			if tt.wantCode != codes.OK {
				return
			}
			if !resp.GetCreated() {
				t.Error("created = false for a fresh insert")
			}
			if resp.GetJob().GetId() != gotIn.ID.String() {
				t.Errorf("returned id %s, store got %s", resp.GetJob().GetId(), gotIn.ID)
			}
			want := job.Job{
				Queue: "default", Type: "email.send", Payload: []byte(`{"to":"x@y.z"}`), Priority: 7,
				Delay: 90 * time.Second, MaxAttempts: 5, IdempotencyKey: "key-1", Status: job.StatusQueued,
				Backoff: job.Policy{Base: 2 * time.Second, Max: time.Minute, Factor: 3},
			}
			if gotIn.Queue != want.Queue || gotIn.Type != want.Type || !bytes.Equal(gotIn.Payload, want.Payload) ||
				gotIn.Priority != want.Priority || gotIn.Delay != want.Delay || !gotIn.RunAt.IsZero() ||
				gotIn.MaxAttempts != want.MaxAttempts || gotIn.IdempotencyKey != want.IdempotencyKey ||
				gotIn.Status != want.Status || gotIn.Backoff != want.Backoff || gotIn.ID == uuid.Nil {
				t.Errorf("store received %+v, want %+v", gotIn, want)
			}
		})
	}
}

func TestEnqueueReplayIsNotCreated(t *testing.T) {
	t.Parallel()
	fake := &fakeStore{enqueue: func(context.Context, job.Job) (job.Job, error) {
		return sampleJob(), nil // a different id than the request's: the stored row
	}}
	c := newTestClients(t, fake)
	resp, err := c.jobs.Enqueue(t.Context(), validEnqueue())
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetCreated() {
		t.Error("created = true for a replay")
	}
	checkJob(t, resp.GetJob(), sampleJob())
}

func TestEnqueueValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		mut  func(*jobrunv1.EnqueueRequest)
	}{
		{"empty queue", func(r *jobrunv1.EnqueueRequest) { r.Queue = "" }},
		{"bad queue chars", func(r *jobrunv1.EnqueueRequest) { r.Queue = "Default Queue" }},
		{"empty type", func(r *jobrunv1.EnqueueRequest) { r.Type = "" }},
		{"priority too high", func(r *jobrunv1.EnqueueRequest) { r.Priority = 101 }},
		{"priority overflow", func(r *jobrunv1.EnqueueRequest) { r.Priority = 1 << 20 }},
		{"payload not json", func(r *jobrunv1.EnqueueRequest) { r.Payload = []byte("{nope") }},
		{"max_attempts too high", func(r *jobrunv1.EnqueueRequest) { r.MaxAttempts = 101 }},
		{"negative delay", func(r *jobrunv1.EnqueueRequest) {
			r.When = &jobrunv1.EnqueueRequest_Delay{Delay: durationpb.New(-time.Second)}
		}},
		{"backoff max < base", func(r *jobrunv1.EnqueueRequest) {
			r.Backoff = &jobrunv1.BackoffPolicy{Base: durationpb.New(time.Minute), Max: durationpb.New(time.Second), Factor: 2}
		}},
		{"idempotency key too long", func(r *jobrunv1.EnqueueRequest) { r.IdempotencyKey = string(make([]byte, 256)) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fake := &fakeStore{}
			c := newTestClients(t, fake)
			req := validEnqueue()
			tt.mut(req)
			_, err := c.jobs.Enqueue(t.Context(), req)
			checkStatus(t, err, codes.InvalidArgument, c.logs, "")
			if fake.callCount() != 0 {
				t.Errorf("store called %d times for invalid input", fake.callCount())
			}
		})
	}
}

func TestEnqueueRunAtAndDefaults(t *testing.T) {
	t.Parallel()
	var gotIn job.Job
	fake := &fakeStore{enqueue: func(_ context.Context, j job.Job) (job.Job, error) {
		gotIn = j
		return j, nil
	}}
	c := newTestClients(t, fake)
	runAt := fixedNow.Add(time.Hour)
	_, err := c.jobs.Enqueue(t.Context(), &jobrunv1.EnqueueRequest{
		Queue: "q", Type: "t", When: &jobrunv1.EnqueueRequest_RunAt{RunAt: timestamppb.New(runAt)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !gotIn.RunAt.Equal(runAt) || gotIn.Delay != 0 {
		t.Errorf("run_at/delay = %v/%v, want %v/0", gotIn.RunAt, gotIn.Delay, runAt)
	}
	if string(gotIn.Payload) != "{}" || gotIn.MaxAttempts != job.DefaultMaxAttempts || gotIn.Backoff != job.DefaultPolicy {
		t.Errorf("defaults not applied: %+v", gotIn)
	}
}

func TestGet(t *testing.T) {
	t.Parallel()

	type tc struct {
		errCase
		id        string
		wantCalls int
	}
	domain := jobErrCases()
	tests := make([]tc, 0, 2+len(domain))
	tests = append(tests,
		tc{errCase: errCase{name: "empty id", wantCode: codes.InvalidArgument}, id: "", wantCalls: 0},
		tc{errCase: errCase{name: "bad uuid", wantCode: codes.InvalidArgument}, id: "not-a-uuid", wantCalls: 0},
	)
	for _, c := range domain {
		tests = append(tests, tc{errCase: c, id: jobID.String(), wantCalls: 1})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fake := &fakeStore{get: func(_ context.Context, id job.ID) (job.Job, error) {
				if id != jobID {
					t.Errorf("store got id %s, want %s", id, jobID)
				}
				if tt.svcErr != nil {
					return job.Job{}, tt.svcErr
				}
				return sampleJob(), nil
			}}
			c := newTestClients(t, fake)

			resp, err := c.jobs.Get(ctxWithRequestID(t, "req-get-"+tt.name), &jobrunv1.GetRequest{Id: tt.id})
			checkStatus(t, err, tt.wantCode, c.logs, "req-get-"+tt.name)
			if fake.callCount() != tt.wantCalls {
				t.Errorf("store called %d times, want %d", fake.callCount(), tt.wantCalls)
			}
			if tt.wantCode == codes.OK {
				checkJob(t, resp.GetJob(), sampleJob())
				if len(resp.GetEvents()) != 0 {
					t.Errorf("events returned without include_events: %v", resp.GetEvents())
				}
			}
		})
	}
}

func TestGetIncludeEvents(t *testing.T) {
	t.Parallel()
	events := []job.Event{
		{JobID: jobID, Queue: "default", Type: "email.send", From: job.StatusUnspecified, To: job.StatusQueued, At: fixedNow},
		{JobID: jobID, Queue: "default", Type: "email.send", From: job.StatusQueued, To: job.StatusLeased, Attempt: 1, At: fixedNow.Add(time.Second)},
		{JobID: jobID, Queue: "default", Type: "email.send", From: job.StatusLeased, To: job.StatusQueued, Attempt: 1, At: fixedNow.Add(2 * time.Second), Err: "boom"},
	}
	fake := &fakeStore{
		get:    func(context.Context, job.ID) (job.Job, error) { return sampleJob(), nil },
		events: func(context.Context, job.ID) ([]job.Event, error) { return events, nil },
	}
	c := newTestClients(t, fake)
	resp, err := c.jobs.Get(t.Context(), &jobrunv1.GetRequest{Id: jobID.String(), IncludeEvents: true})
	if err != nil {
		t.Fatal(err)
	}
	got := resp.GetEvents()
	if len(got) != 3 {
		t.Fatalf("events = %d, want 3", len(got))
	}
	if got[0].GetFromStatus() != jobrunv1.JobStatus_JOB_STATUS_UNSPECIFIED || got[0].GetToStatus() != jobrunv1.JobStatus_JOB_STATUS_QUEUED {
		t.Errorf("event[0] = %v", got[0])
	}
	if got[2].GetError() != "boom" || got[2].GetAttempt() != 1 || !got[2].GetAt().AsTime().Equal(events[2].At) {
		t.Errorf("event[2] = %v", got[2])
	}
	if fake.callCount() != 2 {
		t.Errorf("store called %d times, want 2", fake.callCount())
	}
}

func TestCancelAndRetry(t *testing.T) {
	t.Parallel()

	type tc struct {
		errCase
		id        string
		wantCalls int
	}
	domain := jobErrCases()
	tests := make([]tc, 0, 2+len(domain))
	tests = append(tests,
		tc{errCase: errCase{name: "empty id", wantCode: codes.InvalidArgument}, id: "", wantCalls: 0},
		tc{errCase: errCase{name: "bad uuid", wantCode: codes.InvalidArgument}, id: "019210a0-zzzz", wantCalls: 0},
	)
	for _, c := range domain {
		tests = append(tests, tc{errCase: c, id: jobID.String(), wantCalls: 1})
	}

	for _, tt := range tests {
		t.Run("cancel/"+tt.name, func(t *testing.T) {
			t.Parallel()
			fake := &fakeStore{cancel: func(_ context.Context, id job.ID) (job.Job, error) {
				if id != jobID {
					t.Errorf("store got id %s, want %s", id, jobID)
				}
				if tt.svcErr != nil {
					return job.Job{}, tt.svcErr
				}
				j := sampleJob()
				j.CancelRequested = true
				return j, nil
			}}
			c := newTestClients(t, fake)
			resp, err := c.jobs.Cancel(ctxWithRequestID(t, "req-cancel-"+tt.name), &jobrunv1.CancelRequest{Id: tt.id})
			checkStatus(t, err, tt.wantCode, c.logs, "req-cancel-"+tt.name)
			if fake.callCount() != tt.wantCalls {
				t.Errorf("store called %d times, want %d", fake.callCount(), tt.wantCalls)
			}
			if tt.wantCode == codes.OK && !resp.GetJob().GetCancelRequested() {
				t.Errorf("job = %v, want cancel_requested", resp.GetJob())
			}
		})
		t.Run("retry/"+tt.name, func(t *testing.T) {
			t.Parallel()
			fake := &fakeStore{retry: func(_ context.Context, id job.ID) (job.Job, error) {
				if id != jobID {
					t.Errorf("store got id %s, want %s", id, jobID)
				}
				if tt.svcErr != nil {
					return job.Job{}, tt.svcErr
				}
				j := sampleJob()
				j.Status, j.Attempts, j.LeaseOwner, j.LeaseUntil = job.StatusQueued, 0, "", nil
				return j, nil
			}}
			c := newTestClients(t, fake)
			resp, err := c.jobs.Retry(ctxWithRequestID(t, "req-retry-"+tt.name), &jobrunv1.RetryRequest{Id: tt.id})
			checkStatus(t, err, tt.wantCode, c.logs, "req-retry-"+tt.name)
			if fake.callCount() != tt.wantCalls {
				t.Errorf("store called %d times, want %d", fake.callCount(), tt.wantCalls)
			}
			if tt.wantCode != codes.OK {
				return
			}
			if resp.GetJob().GetStatus() != jobrunv1.JobStatus_JOB_STATUS_QUEUED || resp.GetJob().GetAttempts() != 0 || resp.GetJob().GetLeaseUntil() != nil {
				t.Errorf("job = %v, want QUEUED attempts=0", resp.GetJob())
			}
		})
	}
}

func TestList(t *testing.T) {
	t.Parallel()

	type tc struct {
		errCase
		req       *jobrunv1.ListRequest
		wantCalls int
	}
	domain := jobErrCases()
	tests := make([]tc, 0, 6+len(domain))
	tests = append(tests,
		tc{errCase: errCase{name: "negative page size", wantCode: codes.InvalidArgument}, req: &jobrunv1.ListRequest{PageSize: -1}},
		tc{errCase: errCase{name: "page size too big", wantCode: codes.InvalidArgument}, req: &jobrunv1.ListRequest{PageSize: 201}},
		tc{errCase: errCase{name: "bad page token", wantCode: codes.InvalidArgument}, req: &jobrunv1.ListRequest{PageToken: "!!garbage!!"}},
		tc{errCase: errCase{name: "unknown status enum", wantCode: codes.InvalidArgument}, req: &jobrunv1.ListRequest{Status: jobrunv1.JobStatus(42)}},
		tc{errCase: errCase{name: "bad queue name", wantCode: codes.InvalidArgument}, req: &jobrunv1.ListRequest{Queue: "No Way"}},
		tc{errCase: errCase{name: "bad type name", wantCode: codes.InvalidArgument}, req: &jobrunv1.ListRequest{Type: "a/b"}},
	)
	for _, c := range domain {
		tests = append(tests, tc{errCase: c, req: &jobrunv1.ListRequest{Queue: "default", Type: "email.send", PageSize: 10}, wantCalls: 1})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fake := &fakeStore{list: func(_ context.Context, q job.ListQuery) ([]job.Job, job.Cursor, error) {
				if q.Queue != "default" || q.Type != "email.send" || q.Limit != 10 || q.Status != nil || !q.Cursor.IsZero() {
					t.Errorf("store got query %+v", q)
				}
				if tt.svcErr != nil {
					return nil, job.Cursor{}, tt.svcErr
				}
				return []job.Job{sampleJob()}, job.Cursor{}, nil
			}}
			c := newTestClients(t, fake)

			resp, err := c.jobs.List(ctxWithRequestID(t, "req-list-"+tt.name), tt.req)
			checkStatus(t, err, tt.wantCode, c.logs, "req-list-"+tt.name)
			if fake.callCount() != tt.wantCalls {
				t.Errorf("store called %d times, want %d", fake.callCount(), tt.wantCalls)
			}
			if tt.wantCode != codes.OK {
				return
			}
			if len(resp.GetJobs()) != 1 || resp.GetNextPageToken() != "" {
				t.Fatalf("resp = %v", resp)
			}
			checkJob(t, resp.GetJobs()[0], sampleJob())
		})
	}
}

func TestListDefaultLimit(t *testing.T) {
	t.Parallel()
	fake := &fakeStore{list: func(_ context.Context, q job.ListQuery) ([]job.Job, job.Cursor, error) {
		if q.Limit != job.DefaultListLimit {
			t.Errorf("limit = %d, want default %d", q.Limit, job.DefaultListLimit)
		}
		return nil, job.Cursor{}, nil
	}}
	c := newTestClients(t, fake)
	if _, err := c.jobs.List(t.Context(), &jobrunv1.ListRequest{}); err != nil {
		t.Fatal(err)
	}
}

func TestListPageTokenRoundTrip(t *testing.T) {
	t.Parallel()

	last := sampleJob()
	last.ID = uuid.MustParse("019210a0-0000-7000-8000-000000000002")
	last.CreatedAt = fixedNow.Add(-time.Minute)
	wantNext := job.Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
	wantStatus := job.StatusDead

	var (
		mu      sync.Mutex
		cursors []job.Cursor
	)
	fake := &fakeStore{list: func(_ context.Context, q job.ListQuery) ([]job.Job, job.Cursor, error) {
		mu.Lock()
		cursors = append(cursors, q.Cursor)
		mu.Unlock()
		if q.Status == nil || *q.Status != wantStatus {
			t.Errorf("status filter = %v, want %s", q.Status, wantStatus)
		}
		if q.Cursor.IsZero() {
			return []job.Job{sampleJob(), last}, wantNext, nil
		}
		return []job.Job{last}, job.Cursor{}, nil
	}}
	c := newTestClients(t, fake)

	first, err := c.jobs.List(t.Context(), &jobrunv1.ListRequest{PageSize: 2, Status: jobrunv1.JobStatus_JOB_STATUS_DEAD})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	tok := first.GetNextPageToken()
	if tok == "" {
		t.Fatal("first page has no next_page_token")
	}
	if got, err := job.DecodeCursor(tok); err != nil || !got.CreatedAt.Equal(wantNext.CreatedAt) || got.ID != wantNext.ID {
		t.Fatalf("token decodes to (%+v, %v), want %+v", got, err, wantNext)
	}

	second, err := c.jobs.List(t.Context(), &jobrunv1.ListRequest{PageSize: 2, Status: jobrunv1.JobStatus_JOB_STATUS_DEAD, PageToken: tok})
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if second.GetNextPageToken() != "" {
		t.Errorf("last page has a token %q", second.GetNextPageToken())
	}

	mu.Lock()
	defer mu.Unlock()
	if len(cursors) != 2 || !cursors[0].IsZero() {
		t.Fatalf("cursors seen by the store = %+v", cursors)
	}
	if !cursors[1].CreatedAt.Equal(wantNext.CreatedAt) || cursors[1].ID != wantNext.ID {
		t.Errorf("second call cursor = %+v, want %+v", cursors[1], wantNext)
	}
}

func TestPurge(t *testing.T) {
	t.Parallel()

	type tc struct {
		errCase
		req       *jobrunv1.PurgeRequest
		wantCalls int
		wantQ     job.PurgeQuery
	}
	domain := jobErrCases()
	tests := make([]tc, 0, 5+len(domain))
	tests = append(tests,
		tc{errCase: errCase{name: "live status", wantCode: codes.InvalidArgument}, req: &jobrunv1.PurgeRequest{Statuses: []jobrunv1.JobStatus{jobrunv1.JobStatus_JOB_STATUS_QUEUED}}},
		tc{errCase: errCase{name: "unspecified status", wantCode: codes.InvalidArgument}, req: &jobrunv1.PurgeRequest{Statuses: []jobrunv1.JobStatus{jobrunv1.JobStatus_JOB_STATUS_UNSPECIFIED}}},
		tc{errCase: errCase{name: "unknown status", wantCode: codes.InvalidArgument}, req: &jobrunv1.PurgeRequest{Statuses: []jobrunv1.JobStatus{99}}},
		tc{errCase: errCase{name: "negative older_than", wantCode: codes.InvalidArgument}, req: &jobrunv1.PurgeRequest{OlderThan: durationpb.New(-time.Hour)}},
		tc{errCase: errCase{name: "bad queue", wantCode: codes.InvalidArgument}, req: &jobrunv1.PurgeRequest{Queue: "Q!"}},
	)
	for _, c := range domain {
		tests = append(tests, tc{
			errCase: c, wantCalls: 1,
			req:   &jobrunv1.PurgeRequest{Queue: "default", Statuses: []jobrunv1.JobStatus{jobrunv1.JobStatus_JOB_STATUS_DEAD}, OlderThan: durationpb.New(time.Hour), Limit: 10},
			wantQ: job.PurgeQuery{Queue: "default", Statuses: []job.Status{job.StatusDead}, OlderThan: time.Hour, Limit: 10},
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fake := &fakeStore{purge: func(_ context.Context, q job.PurgeQuery) (int64, error) {
				if q.Queue != tt.wantQ.Queue || q.OlderThan != tt.wantQ.OlderThan || q.Limit != tt.wantQ.Limit ||
					len(q.Statuses) != len(tt.wantQ.Statuses) || q.Statuses[0] != tt.wantQ.Statuses[0] {
					t.Errorf("store got %+v, want %+v", q, tt.wantQ)
				}
				if tt.svcErr != nil {
					return 0, tt.svcErr
				}
				return 42, nil
			}}
			c := newTestClients(t, fake)
			resp, err := c.jobs.Purge(ctxWithRequestID(t, "req-purge-"+tt.name), tt.req)
			checkStatus(t, err, tt.wantCode, c.logs, "req-purge-"+tt.name)
			if fake.callCount() != tt.wantCalls {
				t.Errorf("store called %d times, want %d", fake.callCount(), tt.wantCalls)
			}
			if tt.wantCode == codes.OK && resp.GetDeleted() != 42 {
				t.Errorf("deleted = %d, want 42", resp.GetDeleted())
			}
		})
	}
}

func TestPurgeDefaultStatuses(t *testing.T) {
	t.Parallel()
	fake := &fakeStore{purge: func(_ context.Context, q job.PurgeQuery) (int64, error) {
		if len(q.Statuses) != 3 {
			t.Errorf("statuses = %v, want the three terminal ones", q.Statuses)
		}
		return 0, nil
	}}
	c := newTestClients(t, fake)
	if _, err := c.jobs.Purge(t.Context(), &jobrunv1.PurgeRequest{}); err != nil {
		t.Fatal(err)
	}
}

func depthRows() []job.DepthRow {
	return []job.DepthRow{
		{Queue: "default", Status: job.StatusQueued, N: 12, OldestRunAt: fixedNow.Add(-time.Minute)},
		{Queue: "default", Status: job.StatusLeased, N: 3, OldestRunAt: fixedNow.Add(-time.Second)},
		{Queue: "mail", Status: job.StatusDead, N: 1, OldestRunAt: fixedNow.Add(-time.Hour)},
	}
}

func TestStats(t *testing.T) {
	t.Parallel()

	type tc struct {
		errCase
		queue     string
		wantRows  int
		wantCalls int
	}
	tests := []tc{
		{errCase: errCase{name: "bad queue", wantCode: codes.InvalidArgument}, queue: "Bad Q"},
		{errCase: errCase{name: "all queues", wantCode: codes.OK}, wantRows: 3, wantCalls: 1},
		{errCase: errCase{name: "one queue", wantCode: codes.OK}, queue: "default", wantRows: 2, wantCalls: 1},
		{errCase: errCase{name: "internal", svcErr: errBoom, wantCode: codes.Internal}, wantCalls: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fake := &fakeStore{depth: func(context.Context) ([]job.DepthRow, error) {
				if tt.svcErr != nil {
					return nil, tt.svcErr
				}
				return depthRows(), nil
			}}
			c := newTestClients(t, fake)
			resp, err := c.jobs.Stats(ctxWithRequestID(t, "req-stats-"+tt.name), &jobrunv1.StatsRequest{Queue: tt.queue})
			checkStatus(t, err, tt.wantCode, c.logs, "req-stats-"+tt.name)
			if fake.callCount() != tt.wantCalls {
				t.Errorf("store called %d times, want %d", fake.callCount(), tt.wantCalls)
			}
			if tt.wantCode != codes.OK {
				return
			}
			if len(resp.GetDepths()) != tt.wantRows {
				t.Fatalf("depths = %d, want %d: %v", len(resp.GetDepths()), tt.wantRows, resp.GetDepths())
			}
			d := resp.GetDepths()[0]
			if d.GetQueue() != "default" || d.GetStatus() != jobrunv1.JobStatus_JOB_STATUS_QUEUED || d.GetCount() != 12 ||
				!d.GetOldestRunAt().AsTime().Equal(fixedNow.Add(-time.Minute)) {
				t.Errorf("depths[0] = %v", d)
			}
			if resp.GetAsOf() == nil {
				t.Error("as_of missing")
			}
		})
	}
}

type fakeDepthCache struct {
	rows []job.DepthRow
	at   time.Time
}

func (f fakeDepthCache) Depth() ([]job.DepthRow, time.Time) { return f.rows, f.at }

func TestStatsUsesDepthCache(t *testing.T) {
	t.Parallel()
	fake := &fakeStore{} // Depth on the store must not be called
	c := newTestClients(t, fake, server.WithDepthCache(fakeDepthCache{rows: depthRows()[:1], at: fixedNow}))
	resp, err := c.jobs.Stats(t.Context(), &jobrunv1.StatsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetDepths()) != 1 || !resp.GetAsOf().AsTime().Equal(fixedNow) {
		t.Errorf("resp = %v", resp)
	}
	if fake.callCount() != 0 {
		t.Errorf("store called %d times, want 0 with a depth cache", fake.callCount())
	}
}

func TestStatusMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		domain job.Status
		proto  jobrunv1.JobStatus
	}{
		{job.StatusQueued, jobrunv1.JobStatus_JOB_STATUS_QUEUED},
		{job.StatusLeased, jobrunv1.JobStatus_JOB_STATUS_LEASED},
		{job.StatusSucceeded, jobrunv1.JobStatus_JOB_STATUS_SUCCEEDED},
		{job.StatusFailed, jobrunv1.JobStatus_JOB_STATUS_FAILED},
		{job.StatusDead, jobrunv1.JobStatus_JOB_STATUS_DEAD},
		{job.StatusCancelled, jobrunv1.JobStatus_JOB_STATUS_CANCELLED},
		{job.StatusUnspecified, jobrunv1.JobStatus_JOB_STATUS_UNSPECIFIED},
	}
	for _, tt := range tests {
		t.Run(tt.domain.String(), func(t *testing.T) {
			t.Parallel()
			fake := &fakeStore{
				get: func(context.Context, job.ID) (job.Job, error) {
					j := sampleJob()
					j.Status = tt.domain
					return j, nil
				},
				list: func(_ context.Context, q job.ListQuery) ([]job.Job, job.Cursor, error) {
					switch {
					case tt.domain == job.StatusUnspecified && q.Status != nil:
						t.Errorf("UNSPECIFIED should be no filter, got %v", *q.Status)
					case tt.domain != job.StatusUnspecified && (q.Status == nil || *q.Status != tt.domain):
						t.Errorf("filter = %v, want %s", q.Status, tt.domain)
					}
					return nil, job.Cursor{}, nil
				},
			}
			c := newTestClients(t, fake)

			resp, err := c.jobs.Get(t.Context(), &jobrunv1.GetRequest{Id: jobID.String()})
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if got := resp.GetJob().GetStatus(); got != tt.proto {
				t.Errorf("status = %v, want %v", got, tt.proto)
			}
			if _, err := c.jobs.List(t.Context(), &jobrunv1.ListRequest{Status: tt.proto}); err != nil {
				t.Fatalf("List: %v", err)
			}
		})
	}
}
