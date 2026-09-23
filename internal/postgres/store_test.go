//go:build integration

package postgres_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/crowbett/jobrun/internal/job"
	"github.com/crowbett/jobrun/internal/postgres"
)

const envDatabaseURL = "TEST_DATABASE_URL"

var discard = slog.New(slog.DiscardHandler)

// newTestPool creates a private schema, opens a pool whose search_path is that
// schema, migrates it and drops it again in Cleanup. Every test gets its own
// schema, so tests run in parallel without sharing rows. NOTIFY channels and
// advisory locks are database-global, so those tests use random names/keys.
func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv(envDatabaseURL)
	if url == "" {
		t.Skipf("%s not set", envDatabaseURL)
	}
	ctx := context.Background()

	var raw [6]byte
	if _, err := rand.Read(raw[:]); err != nil {
		t.Fatalf("random schema name: %v", err)
	}
	schema := "test_" + hex.EncodeToString(raw[:])

	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if err := admin.Close(ctx); err != nil {
		t.Fatalf("close admin: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 10
	pool, err := postgres.OpenConfig(ctx, cfg, discard)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		admin, err := pgx.Connect(ctx, url)
		if err != nil {
			t.Logf("cleanup connect: %v", err)
			return
		}
		defer admin.Close(ctx)
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Logf("drop schema %s: %v", schema, err)
		}
	})

	if err := postgres.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

func newStore(t *testing.T, opts ...postgres.Option) (*postgres.Store, *pgxpool.Pool) {
	t.Helper()
	pool := newTestPool(t)
	return postgres.NewStore(pool, discard, opts...), pool
}

func newJob(t *testing.T, queue string, mutate ...func(*job.NewJob)) job.Job {
	t.Helper()
	n := job.NewJob{Queue: queue, Type: "noop", Payload: []byte(`{"k":1}`)}
	for _, m := range mutate {
		m(&n)
	}
	j, err := job.New(n)
	if err != nil {
		t.Fatalf("job.New: %v", err)
	}
	return j
}

func mustEnqueue(t *testing.T, s *postgres.Store, j job.Job) job.Job {
	t.Helper()
	out, err := s.Enqueue(context.Background(), j)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return out
}

func mustGet(t *testing.T, s *postgres.Store, id job.ID) job.Job {
	t.Helper()
	j, err := s.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get(%s): %v", id, err)
	}
	return j
}

func mustClaim(t *testing.T, s *postgres.Store, queue, owner string, limit int32, ttl time.Duration) []job.Job {
	t.Helper()
	jobs, err := s.Claim(context.Background(), job.ClaimQuery{Queue: queue, Owner: owner, Limit: limit, LeaseTTL: ttl})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	return jobs
}

// curLease is the lease a job holds right now (its current fencing token).
func curLease(t *testing.T, s *postgres.Store, id job.ID) job.Lease {
	t.Helper()
	return job.LeaseOf(mustGet(t, s, id))
}

func dbNow(t *testing.T, s *postgres.Store) time.Time {
	t.Helper()
	now, err := s.Now(context.Background())
	if err != nil {
		t.Fatalf("Now: %v", err)
	}
	return now
}

// expireLease pushes a leased job's lease_until into the past.
func expireLease(t *testing.T, pool *pgxpool.Pool, id job.ID) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), "UPDATE jobs SET lease_until = now() - interval '1 second' WHERE id = $1", id); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
}

func eventStatuses(evs []job.Event) []string {
	out := make([]string, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.From.String()+"->"+e.To.String())
	}
	return out
}

func TestMigrateIdempotent(t *testing.T) {
	t.Parallel()
	pool := newTestPool(t) // first Migrate ran here
	if err := postgres.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM goose_db_version WHERE version_id > 0").Scan(&n); err != nil {
		t.Fatalf("goose_db_version: %v", err)
	}
	if n != 5 {
		t.Fatalf("applied migrations: want 5 got %d", n)
	}
}

func TestOpen(t *testing.T) {
	t.Parallel()
	url := os.Getenv(envDatabaseURL)
	if url == "" {
		t.Skipf("%s not set", envDatabaseURL)
	}
	ctx := context.Background()
	pool, err := postgres.Open(ctx, url, 4, discard)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := pool.Config().MaxConns; got != 4 {
		t.Fatalf("MaxConns: want 4 got %d", got)
	}
	pool.Close()
	if _, err := postgres.Open(ctx, "postgres://nobody:nope@localhost:1/none?connect_timeout=1", 0, discard); err == nil {
		t.Fatal("Open with a dead address: want error")
	}
}

func TestEnqueueGetEvents(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)
	ctx := context.Background()
	before := dbNow(t, s)

	j := mustEnqueue(t, s, newJob(t, "q1", func(n *job.NewJob) { n.Delay = time.Hour; n.Priority = 7; n.IdempotencyKey = "k1" }))
	got := mustGet(t, s, j.ID)
	if got.Status != job.StatusQueued || got.Queue != "q1" || got.Type != "noop" || got.Priority != 7 ||
		got.Attempts != 0 || got.MaxAttempts != job.DefaultMaxAttempts || got.IdempotencyKey != "k1" ||
		got.Backoff != job.DefaultPolicy || string(got.Payload) != `{"k": 1}` || got.LeaseOwner != "" || got.LeaseUntil != nil {
		t.Fatalf("stored job: %+v", got)
	}
	if got.RunAt.Before(before.Add(time.Hour)) || got.RunAt.After(before.Add(time.Hour+5*time.Second)) {
		t.Fatalf("run_at = now()+delay on the db clock: got %v, before %v", got.RunAt, before)
	}
	if got.CreatedAt.Before(before) || got.CreatedAt.Location() != time.UTC {
		t.Fatalf("created_at: %v (loc %v)", got.CreatedAt, got.CreatedAt.Location())
	}

	evs, err := s.Events(ctx, j.ID)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(evs) != 1 || evs[0].From != job.StatusUnspecified || evs[0].To != job.StatusQueued ||
		evs[0].JobID != j.ID || evs[0].Queue != "q1" || evs[0].Type != "noop" || evs[0].Attempt != 0 {
		t.Fatalf("events: %+v", evs)
	}

	// An absolute run_at crosses the wire unchanged; an empty payload is {}.
	at := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	abs := mustEnqueue(t, s, newJob(t, "q1", func(n *job.NewJob) { n.RunAt = at; n.Payload = nil }))
	if !abs.RunAt.Equal(at) || string(abs.Payload) != "{}" {
		t.Fatalf("absolute run_at / empty payload: %+v", abs)
	}

	if _, err := s.Get(ctx, job.NewID()); !errors.Is(err, job.ErrNotFound) {
		t.Fatalf("Get(unknown): want ErrNotFound got %v", err)
	}
	if evs, err := s.Events(ctx, job.NewID()); err != nil || len(evs) != 0 {
		t.Fatalf("Events(unknown): %v / %d", err, len(evs))
	}
}

func TestEnqueueIdempotentReplay(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)
	ctx := context.Background()

	first := mustEnqueue(t, s, newJob(t, "q1", func(n *job.NewJob) { n.IdempotencyKey = "k1"; n.Payload = []byte(`{"b":2,"a":1}`) }))

	// Same body, new id, different key spacing/order: a replay returns the stored row.
	replay := newJob(t, "q1", func(n *job.NewJob) { n.IdempotencyKey = "k1"; n.Payload = []byte(`{ "a": 1, "b": 2 }`) })
	stored, err := s.Enqueue(ctx, replay)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if stored.ID != first.ID || stored.ID == replay.ID {
		t.Fatalf("replay must return the stored job: first %s stored %s replay %s", first.ID, stored.ID, replay.ID)
	}
	if _, err := s.Get(ctx, replay.ID); !errors.Is(err, job.ErrNotFound) {
		t.Fatalf("replayed job must not be stored, got %v", err)
	}
	if evs, _ := s.Events(ctx, first.ID); len(evs) != 1 {
		t.Fatalf("replay must not write an event: %v", eventStatuses(evs))
	}

	// Different body under the same key is a conflict.
	other := newJob(t, "q1", func(n *job.NewJob) { n.IdempotencyKey = "k1"; n.Payload = []byte(`{"a":9}`) })
	if _, err := s.Enqueue(ctx, other); !errors.Is(err, job.ErrIdempotencyConflict) {
		t.Fatalf("conflict: want ErrIdempotencyConflict got %v", err)
	}
	// The same key on another queue is a different job; no key never conflicts.
	mustEnqueue(t, s, newJob(t, "q2", func(n *job.NewJob) { n.IdempotencyKey = "k1" }))
	mustEnqueue(t, s, newJob(t, "q1"))
	mustEnqueue(t, s, newJob(t, "q1"))
}

func TestEnqueueBatch(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)
	ctx := context.Background()
	before := dbNow(t, s)

	js := make([]job.Job, 0, 50)
	for i := range 50 {
		js = append(js, newJob(t, "batch", func(n *job.NewJob) {
			if i%2 == 0 {
				n.Delay = time.Minute
			}
		}))
	}
	n, err := s.EnqueueBatch(ctx, js)
	if err != nil || n != 50 {
		t.Fatalf("EnqueueBatch: %d / %v", n, err)
	}
	if n, err := s.EnqueueBatch(ctx, nil); err != nil || n != 0 {
		t.Fatalf("EnqueueBatch(nil): %d / %v", n, err)
	}
	for i, j := range js {
		got := mustGet(t, s, j.ID)
		wantMin := before
		if i%2 == 0 {
			wantMin = before.Add(time.Minute)
		}
		if got.RunAt.Before(wantMin) || got.RunAt.After(wantMin.Add(5*time.Second)) {
			t.Fatalf("job %d run_at %v, want >= %v", i, got.RunAt, wantMin)
		}
		if evs, _ := s.Events(ctx, j.ID); len(evs) != 1 || evs[0].To != job.StatusQueued {
			t.Fatalf("job %d events: %v", i, eventStatuses(evs))
		}
	}
	// Only the 25 undelayed jobs are claimable now.
	claimed := mustClaim(t, s, "batch", "w", 100, time.Minute)
	if len(claimed) != 25 {
		t.Fatalf("claimable: want 25 got %d", len(claimed))
	}
}

func TestClaimExclusiveUnderConcurrency(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)
	ctx := context.Background()

	const total, claimers = 200, 8
	js := make([]job.Job, 0, total)
	for i := range total {
		js = append(js, newJob(t, "hot", func(n *job.NewJob) { n.Priority = int16(i % 5) }))
	}
	if _, err := s.EnqueueBatch(ctx, js); err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}

	var (
		mu    sync.Mutex
		seen  = make(map[job.ID]string, total)
		dupes []string
		wg    sync.WaitGroup
		errs  = make(chan error, claimers)
	)
	for c := range claimers {
		wg.Add(1)
		go func(owner string) {
			defer wg.Done()
			for {
				batch, err := s.Claim(ctx, job.ClaimQuery{Queue: "hot", Owner: owner, Limit: 7, LeaseTTL: time.Minute})
				if err != nil {
					errs <- err
					return
				}
				if len(batch) == 0 {
					return
				}
				mu.Lock()
				for _, j := range batch {
					if prev, ok := seen[j.ID]; ok {
						dupes = append(dupes, fmt.Sprintf("%s claimed by %s and %s", j.ID, prev, owner))
					}
					seen[j.ID] = owner
					if j.Status != job.StatusLeased || j.LeaseOwner != owner || j.Attempts != 1 || j.LeaseUntil == nil || j.StartedAt == nil {
						dupes = append(dupes, fmt.Sprintf("%s bad lease state: %+v", j.ID, j))
					}
				}
				mu.Unlock()
			}
		}(fmt.Sprintf("worker-%d", c))
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("claimer: %v", err)
	}
	if len(dupes) > 0 {
		t.Fatalf("exclusivity violated: %v", dupes)
	}
	if len(seen) != total {
		t.Fatalf("claimed %d of %d", len(seen), total)
	}
	if again := mustClaim(t, s, "hot", "late", 10, time.Minute); len(again) != 0 {
		t.Fatalf("nothing should be left: %d", len(again))
	}
	// One claim event per job, attempt 1.
	evs, err := s.Events(ctx, js[0].ID)
	if err != nil || len(evs) != 2 || evs[1].From != job.StatusQueued || evs[1].To != job.StatusLeased || evs[1].Attempt != 1 {
		t.Fatalf("claim events: %v / %v", eventStatuses(evs), err)
	}
}

func TestClaimOrderAndReadiness(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)

	low := mustEnqueue(t, s, newJob(t, "p", func(n *job.NewJob) { n.Priority = -5 }))
	high := mustEnqueue(t, s, newJob(t, "p", func(n *job.NewJob) { n.Priority = 50 }))
	mustEnqueue(t, s, newJob(t, "p", func(n *job.NewJob) { n.Priority = 100; n.Delay = time.Hour })) // not ready
	mustEnqueue(t, s, newJob(t, "other"))                                                            // other queue

	got := mustClaim(t, s, "p", "w", 10, time.Minute)
	if len(got) != 2 {
		t.Fatalf("claimed %d, want 2 (ready jobs on queue p)", len(got))
	}
	ids := map[job.ID]bool{got[0].ID: true, got[1].ID: true}
	if !ids[low.ID] || !ids[high.ID] {
		t.Fatalf("claimed wrong jobs: %+v", got)
	}
	// A limit of 1 takes the highest priority first.
	s2, _ := newStore(t)
	mustEnqueue(t, s2, newJob(t, "p", func(n *job.NewJob) { n.Priority = -5 }))
	top := mustEnqueue(t, s2, newJob(t, "p", func(n *job.NewJob) { n.Priority = 50 }))
	if one := mustClaim(t, s2, "p", "w", 1, time.Minute); len(one) != 1 || one[0].ID != top.ID {
		t.Fatalf("priority order: %+v", one)
	}
	if _, err := s.Claim(context.Background(), job.ClaimQuery{Queue: "p"}); !errors.Is(err, job.ErrInvalidArgument) {
		t.Fatalf("Claim without owner/limit/ttl: want ErrInvalidArgument got %v", err)
	}
}

func TestHeartbeatFencing(t *testing.T) {
	t.Parallel()
	s, pool := newStore(t)
	ctx := context.Background()
	for range 3 {
		mustEnqueue(t, s, newJob(t, "hb"))
	}
	claimed := mustClaim(t, s, "hb", "w1", 3, 30*time.Second)
	if len(claimed) != 3 {
		t.Fatalf("claimed %d", len(claimed))
	}
	expired, cancelled, healthy := claimed[0], claimed[1], claimed[2]
	expireLease(t, pool, expired.ID)
	if _, err := s.Cancel(ctx, cancelled.ID); err != nil {
		t.Fatalf("Cancel leased: %v", err)
	}

	until := dbNow(t, s).Add(time.Minute)
	leases := []job.Lease{job.LeaseOf(expired), job.LeaseOf(cancelled), job.LeaseOf(healthy), {ID: job.NewID(), Token: 1}}
	r, err := s.Heartbeat(ctx, "w1", leases, until)
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if len(r.Kept) != 1 || r.Kept[0] != job.LeaseOf(healthy) {
		t.Fatalf("kept: want [%s] got %v", healthy.ID, r.Kept)
	}
	if len(r.CancelRequested) != 1 || r.CancelRequested[0] != job.LeaseOf(cancelled) {
		t.Fatalf("cancel requested: want [%s] got %v", cancelled.ID, r.CancelRequested)
	}
	if got := mustGet(t, s, healthy.ID); got.LeaseUntil == nil || !got.LeaseUntil.Equal(until.Truncate(time.Microsecond)) {
		t.Fatalf("healthy lease_until not extended: %+v", got.LeaseUntil)
	}
	// The cancelled one is still leased (extended, but the worker was told to stop).
	if got := mustGet(t, s, cancelled.ID); got.Status != job.StatusLeased || !got.CancelRequested {
		t.Fatalf("cancelled job: %+v", got)
	}
	// Another owner keeps nothing, a wrong token keeps nothing; an empty list is a no-op.
	if r, err := s.Heartbeat(ctx, "w2", []job.Lease{job.LeaseOf(healthy)}, until); err != nil || len(r.Kept)+len(r.CancelRequested) != 0 {
		t.Fatalf("other owner: %+v / %v", r, err)
	}
	stale := job.Lease{ID: healthy.ID, Token: healthy.LeaseToken - 1}
	if r, err := s.Heartbeat(ctx, "w1", []job.Lease{stale}, until); err != nil || len(r.Kept)+len(r.CancelRequested) != 0 {
		t.Fatalf("stale token: %+v / %v", r, err)
	}
	if r, err := s.Heartbeat(ctx, "w1", nil, until); err != nil || len(r.Kept) != 0 {
		t.Fatalf("empty leases: %+v / %v", r, err)
	}
}

// TestLeaseTokenFencesReclaim is the case owner-only fencing missed: the same
// owner reclaims a reaped job, and the first attempt's lease must stay dead.
func TestLeaseTokenFencesReclaim(t *testing.T) {
	t.Parallel()
	s, pool := newStore(t)
	ctx := context.Background()
	j := mustEnqueue(t, s, newJob(t, "fence"))
	first := mustClaim(t, s, "fence", "w1", 1, time.Minute)
	if len(first) != 1 || first[0].LeaseToken != 1 {
		t.Fatalf("first claim: %+v", first)
	}
	expireLease(t, pool, j.ID)
	if evs, err := s.ReapExpired(ctx, dbNow(t, s), 10, job.Policy{Base: time.Microsecond, Max: time.Microsecond, Factor: 1}); err != nil || len(evs) != 1 {
		t.Fatalf("reap: %+v / %v", evs, err)
	}
	var second []job.Job
	deadline := time.Now().Add(5 * time.Second)
	for len(second) == 0 && time.Now().Before(deadline) {
		second = mustClaim(t, s, "fence", "w1", 1, time.Minute)
	}
	if len(second) != 1 || second[0].LeaseToken != 2 || second[0].Attempts != 2 {
		t.Fatalf("second claim: %+v", second)
	}
	old, cur := job.LeaseOf(first[0]), job.LeaseOf(second[0])
	until := dbNow(t, s).Add(time.Minute)
	if r, err := s.Heartbeat(ctx, "w1", []job.Lease{old}, until); err != nil || len(r.Kept) != 0 {
		t.Fatalf("old lease heartbeat: %+v / %v", r, err)
	}
	if err := s.Complete(ctx, old, "w1", nil); !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("old lease Complete: want ErrLeaseLost got %v", err)
	}
	if _, err := s.Fail(ctx, old, "w1", "late", false, until); !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("old lease Fail: want ErrLeaseLost got %v", err)
	}
	if r, err := s.Heartbeat(ctx, "w1", []job.Lease{old, cur}, until); err != nil || len(r.Kept) != 1 || r.Kept[0] != cur {
		t.Fatalf("current lease heartbeat: %+v / %v", r, err)
	}
	if err := s.Complete(ctx, cur, "w1", nil); err != nil {
		t.Fatalf("current lease Complete: %v", err)
	}
}

// TestTraceParentRoundTrip: the enqueue trace context is stored and read back
// on Get and Claim, for single and batch enqueue.
func TestTraceParentRoundTrip(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)
	ctx := context.Background()
	const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	one := newJob(t, "tp")
	one.TraceParent = tp
	mustEnqueue(t, s, one)
	batch := newJob(t, "tp")
	batch.TraceParent = tp
	plain := newJob(t, "tp")
	if _, err := s.EnqueueBatch(ctx, []job.Job{batch, plain}); err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}
	for _, tc := range []struct {
		id   job.ID
		want string
	}{{one.ID, tp}, {batch.ID, tp}, {plain.ID, ""}} {
		if got := mustGet(t, s, tc.id).TraceParent; got != tc.want {
			t.Fatalf("Get(%s).TraceParent = %q want %q", tc.id, got, tc.want)
		}
	}
	claimed := mustClaim(t, s, "tp", "w1", 3, time.Minute)
	n := 0
	for _, c := range claimed {
		if c.TraceParent == tp {
			n++
		}
	}
	if len(claimed) != 3 || n != 2 {
		t.Fatalf("claimed %d, %d with traceparent", len(claimed), n)
	}
}

func TestCompleteRunsEffectOnceInTx(t *testing.T) {
	t.Parallel()
	s, pool := newStore(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, "CREATE TABLE effects_out (job_id uuid NOT NULL)"); err != nil {
		t.Fatalf("side table: %v", err)
	}
	j := mustEnqueue(t, s, newJob(t, "c", func(n *job.NewJob) { n.IdempotencyKey = "eff-1" }))
	claimed := mustClaim(t, s, "c", "w1", 1, time.Minute)
	if len(claimed) != 1 {
		t.Fatalf("claimed %d", len(claimed))
	}
	effect := func(ctx context.Context, ex job.Execer) error {
		n, err := ex.Exec(ctx, "INSERT INTO effects_out (job_id) VALUES ($1)", j.ID)
		if err != nil || n != 1 {
			return fmt.Errorf("effect: %d / %w", n, err)
		}
		return nil
	}

	// Wrong owner: nothing changes, the effect rolls back.
	if err := s.Complete(ctx, curLease(t, s, j.ID), "w2", effect); !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("wrong owner: want ErrLeaseLost got %v", err)
	}
	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM effects_out").Scan(&n); err != nil || n != 0 {
		t.Fatalf("effect must roll back with a lost lease: %d / %v", n, err)
	}
	if done, err := s.EffectDone(ctx, "job:noop", "eff-1"); err != nil || done {
		t.Fatalf("EffectDone before: %v / %v", done, err)
	}

	if err := s.Complete(ctx, curLease(t, s, j.ID), "w1", effect); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	got := mustGet(t, s, j.ID)
	if got.Status != job.StatusSucceeded || got.LeaseOwner != "" || got.LeaseUntil != nil || got.FinishedAt == nil || got.Attempts != 1 {
		t.Fatalf("completed: %+v", got)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM effects_out").Scan(&n); err != nil || n != 1 {
		t.Fatalf("effect rows: %d / %v", n, err)
	}
	if done, err := s.EffectDone(ctx, "job:noop", "eff-1"); err != nil || !done {
		t.Fatalf("EffectDone after: %v / %v", done, err)
	}
	evs, _ := s.Events(ctx, j.ID)
	if want := []string{"unspecified->queued", "queued->leased", "leased->succeeded"}; fmt.Sprint(eventStatuses(evs)) != fmt.Sprint(want) {
		t.Fatalf("events: %v", eventStatuses(evs))
	}
	// A second Complete finds no lease.
	if err := s.Complete(ctx, curLease(t, s, j.ID), "w1", nil); !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("second Complete: want ErrLeaseLost got %v", err)
	}
	if err := s.Complete(ctx, job.Lease{ID: job.NewID(), Token: 1}, "w1", nil); !errors.Is(err, job.ErrNotFound) {
		t.Fatalf("Complete(unknown): want ErrNotFound got %v", err)
	}

	// A failing effect rolls the whole completion back.
	j2 := mustEnqueue(t, s, newJob(t, "c"))
	mustClaim(t, s, "c", "w1", 1, time.Minute)
	boom := errors.New("boom")
	if err := s.Complete(ctx, curLease(t, s, j2.ID), "w1", func(context.Context, job.Execer) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("failing effect: want boom got %v", err)
	}
	if got := mustGet(t, s, j2.ID); got.Status != job.StatusLeased {
		t.Fatalf("job must stay leased after a failed effect: %+v", got)
	}
	if done, _ := s.EffectDone(ctx, "job:noop", j2.ID.String()); done {
		t.Fatal("effect row must roll back with the failed closure")
	}
}

func TestCompleteAfterReapIsLeaseLost(t *testing.T) {
	t.Parallel()
	s, pool := newStore(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, "CREATE TABLE effects_out (job_id uuid NOT NULL)"); err != nil {
		t.Fatalf("side table: %v", err)
	}
	j := mustEnqueue(t, s, newJob(t, "r"))
	mustClaim(t, s, "r", "w1", 1, time.Minute)
	expireLease(t, pool, j.ID)

	evs, err := s.ReapExpired(ctx, dbNow(t, s), 10, job.DefaultPolicy)
	if err != nil || len(evs) != 1 || evs[0].From != job.StatusLeased || evs[0].To != job.StatusQueued || evs[0].Err != "lease expired" {
		t.Fatalf("ReapExpired: %+v / %v", evs, err)
	}
	effectRan := false
	err = s.Complete(ctx, curLease(t, s, j.ID), "w1", func(ctx context.Context, ex job.Execer) error {
		effectRan = true
		_, err := ex.Exec(ctx, "INSERT INTO effects_out (job_id) VALUES ($1)", j.ID)
		return err
	})
	if !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("Complete after reap: want ErrLeaseLost got %v", err)
	}
	if effectRan {
		t.Fatal("effect must not run once the lease is gone")
	}
	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM effects_out").Scan(&n); err != nil || n != 0 {
		t.Fatalf("effects_out: %d / %v", n, err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM processed_effects").Scan(&n); err != nil || n != 0 {
		t.Fatalf("processed_effects: %d / %v", n, err)
	}
	got := mustGet(t, s, j.ID)
	if got.Status != job.StatusQueued || got.Attempts != 1 || got.LastError != "lease expired" || got.LeaseOwner != "" {
		t.Fatalf("reaped job: %+v", got)
	}
	// Fail from the stale owner is equally refused.
	if _, err := s.Fail(ctx, curLease(t, s, j.ID), "w1", "late", false, dbNow(t, s)); !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("Fail after reap: want ErrLeaseLost got %v", err)
	}
}

func TestFailRetriesThenDead(t *testing.T) {
	t.Parallel()
	s, pool := newStore(t)
	ctx := context.Background()
	j := mustEnqueue(t, s, newJob(t, "f", func(n *job.NewJob) { n.MaxAttempts = 2 }))

	mustClaim(t, s, "f", "w1", 1, time.Minute)
	retryAt := dbNow(t, s).Add(time.Hour).Truncate(time.Microsecond)
	st, err := s.Fail(ctx, curLease(t, s, j.ID), "w1", "first failure", false, retryAt)
	if err != nil || st != job.StatusQueued {
		t.Fatalf("Fail #1: %v / %v", st, err)
	}
	got := mustGet(t, s, j.ID)
	if got.Status != job.StatusQueued || !got.RunAt.Equal(retryAt) || got.Attempts != 1 || got.LastError != "first failure" ||
		got.LeaseOwner != "" || got.FinishedAt != nil {
		t.Fatalf("after fail #1: %+v", got)
	}
	if again := mustClaim(t, s, "f", "w1", 1, time.Minute); len(again) != 0 {
		t.Fatalf("a future run_at must not be claimable: %+v", again)
	}
	if _, err := pool.Exec(ctx, "UPDATE jobs SET run_at = now() WHERE id = $1", j.ID); err != nil {
		t.Fatal(err)
	}
	if c := mustClaim(t, s, "f", "w1", 1, time.Minute); len(c) != 1 || c[0].Attempts != 2 {
		t.Fatalf("claim #2: %+v", c)
	}
	st, err = s.Fail(ctx, curLease(t, s, j.ID), "w1", "second failure", false, dbNow(t, s))
	if err != nil || st != job.StatusDead {
		t.Fatalf("Fail #2: %v / %v", st, err)
	}
	got = mustGet(t, s, j.ID)
	if got.Status != job.StatusDead || got.FinishedAt == nil || got.LastError != "second failure" {
		t.Fatalf("dead: %+v", got)
	}
	evs, _ := s.Events(ctx, j.ID)
	want := []string{"unspecified->queued", "queued->leased", "leased->queued", "queued->leased", "leased->dead"}
	if fmt.Sprint(eventStatuses(evs)) != fmt.Sprint(want) || evs[4].Err != "second failure" || evs[4].Attempt != 2 {
		t.Fatalf("events: %v", eventStatuses(evs))
	}

	// Permanent failure goes straight to dead on the first attempt.
	p := mustEnqueue(t, s, newJob(t, "f"))
	mustClaim(t, s, "f", "w1", 1, time.Minute)
	if st, err := s.Fail(ctx, curLease(t, s, p.ID), "w1", "bad payload", true, dbNow(t, s)); err != nil || st != job.StatusDead {
		t.Fatalf("permanent: %v / %v", st, err)
	}
	if _, err := s.Fail(ctx, curLease(t, s, p.ID), "w1", "again", false, dbNow(t, s)); !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("Fail on a dead job: want ErrLeaseLost got %v", err)
	}
}

// TestFailStoresHostileCause pins that a handler error Postgres text cannot
// hold (a NUL byte, invalid UTF-8, a multibyte rune straddling MaxErrorLen)
// still records the failure instead of failing Fail and leaving the job leased.
func TestFailStoresHostileCause(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)
	ctx := context.Background()
	causes := []struct {
		name, cause, want string
	}{
		{"nul", "boom\x00 hidden", "boom hidden"},
		{"invalid utf-8", "bad \xff byte", "bad � byte"},
		{"rune straddles cap", strings.Repeat("a", job.MaxErrorLen-1) + "é", strings.Repeat("a", job.MaxErrorLen-1)},
	}
	for _, tc := range causes {
		j := mustEnqueue(t, s, newJob(t, "hostile", func(n *job.NewJob) { n.MaxAttempts = 1 }))
		mustClaim(t, s, "hostile", "w1", 1, time.Minute)
		st, err := s.Fail(ctx, curLease(t, s, j.ID), "w1", tc.cause, false, dbNow(t, s))
		if err != nil || st != job.StatusDead {
			t.Fatalf("%s: Fail: %v / %v", tc.name, st, err)
		}
		got := mustGet(t, s, j.ID)
		if got.LastError != tc.want {
			t.Fatalf("%s: last_error %q, want %q", tc.name, got.LastError, tc.want)
		}
		evs, err := s.Events(ctx, j.ID)
		if err != nil || len(evs) == 0 || evs[len(evs)-1].Err != tc.want {
			t.Fatalf("%s: events %v: %v", tc.name, eventStatuses(evs), err)
		}
	}
}

func TestReapExpired(t *testing.T) {
	t.Parallel()
	s, pool := newStore(t)
	ctx := context.Background()

	exhausted := mustEnqueue(t, s, newJob(t, "reap", func(n *job.NewJob) { n.MaxAttempts = 1 }))
	retry := mustEnqueue(t, s, newJob(t, "reap", func(n *job.NewJob) {
		n.Backoff = job.Policy{Base: 10 * time.Second, Max: 10 * time.Second, Factor: 1, Jitter: false}
	}))
	cancelled := mustEnqueue(t, s, newJob(t, "reap"))
	alive := mustEnqueue(t, s, newJob(t, "reap"))
	if got := mustClaim(t, s, "reap", "w1", 10, time.Minute); len(got) != 4 {
		t.Fatalf("claimed %d", len(got))
	}
	for _, id := range []job.ID{exhausted.ID, retry.ID, cancelled.ID} {
		expireLease(t, pool, id)
	}
	if _, err := s.Cancel(ctx, cancelled.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	now := dbNow(t, s)
	evs, err := s.ReapExpired(ctx, now, 10, job.DefaultPolicy)
	if err != nil {
		t.Fatalf("ReapExpired: %v", err)
	}
	byID := map[job.ID]job.Event{}
	for _, e := range evs {
		byID[e.JobID] = e
	}
	if len(byID) != 3 || byID[exhausted.ID].To != job.StatusDead || byID[retry.ID].To != job.StatusQueued || byID[cancelled.ID].To != job.StatusCancelled {
		t.Fatalf("reap events: %+v", evs)
	}
	if got := mustGet(t, s, exhausted.ID); got.Status != job.StatusDead || got.FinishedAt == nil || got.LastError != "lease expired" {
		t.Fatalf("exhausted: %+v", got)
	}
	if got := mustGet(t, s, retry.ID); got.Status != job.StatusQueued || !got.RunAt.Equal(now.Add(10*time.Second).Truncate(time.Microsecond)) {
		t.Fatalf("retry: want run_at now+10s (%v) got %+v", now.Add(10*time.Second), got)
	}
	if got := mustGet(t, s, cancelled.ID); got.Status != job.StatusCancelled || got.FinishedAt == nil {
		t.Fatalf("cancelled: %+v", got)
	}
	if got := mustGet(t, s, alive.ID); got.Status != job.StatusLeased {
		t.Fatalf("alive lease must be untouched: %+v", got)
	}
	if evs, err := s.ReapExpired(ctx, dbNow(t, s), 10, job.DefaultPolicy); err != nil || len(evs) != 0 {
		t.Fatalf("second reap: %v / %v", evs, err)
	}
	if hist, _ := s.Events(ctx, retry.ID); hist[len(hist)-1].To != job.StatusQueued || hist[len(hist)-1].Err != "lease expired" {
		t.Fatalf("retry history: %v", eventStatuses(hist))
	}
}

func TestListKeyset(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)
	ctx := context.Background()

	var wantQueued []job.ID // newest first
	for i := range 30 {
		j := mustEnqueue(t, s, newJob(t, "list", func(n *job.NewJob) {
			if i%6 == 5 {
				n.Type = "other"
			}
		}))
		if i%6 == 5 { // 5 cancelled rows interleaved with 25 queued
			if _, err := s.Cancel(ctx, j.ID); err != nil {
				t.Fatalf("Cancel: %v", err)
			}
			continue
		}
		wantQueued = append([]job.ID{j.ID}, wantQueued...)
	}
	mustEnqueue(t, s, newJob(t, "elsewhere")) // must not leak in

	queued := job.StatusQueued
	var got []job.ID
	var cursor job.Cursor
	pages := 0
	for {
		page, next, err := s.List(ctx, job.ListQuery{Queue: "list", Status: &queued, Cursor: cursor, Limit: 10})
		if err != nil {
			t.Fatalf("List page %d: %v", pages, err)
		}
		pages++
		for i, j := range page {
			if j.Status != job.StatusQueued || j.Queue != "list" {
				t.Fatalf("page %d row %d: %+v", pages, i, j)
			}
			if i > 0 && page[i-1].CreatedAt.Before(j.CreatedAt) {
				t.Fatalf("page %d row %d: not descending", pages, i)
			}
			got = append(got, j.ID)
		}
		if next.IsZero() {
			break
		}
		if len(page) != 10 {
			t.Fatalf("page %d: %d rows with a next cursor", pages, len(page))
		}
		cursor = next
	}
	if pages != 3 || len(got) != 25 {
		t.Fatalf("pages %d rows %d, want 3 / 25", pages, len(got))
	}
	for i := range wantQueued {
		if got[i] != wantQueued[i] {
			t.Fatalf("row %d: want %s got %s", i, wantQueued[i], got[i])
		}
	}

	all, next, err := s.List(ctx, job.ListQuery{Queue: "list", Limit: 100})
	if err != nil || len(all) != 30 || !next.IsZero() {
		t.Fatalf("unfiltered: %d / %v / %v", len(all), next, err)
	}
	byType, _, err := s.List(ctx, job.ListQuery{Type: "other"})
	if err != nil || len(byType) != 5 {
		t.Fatalf("type filter: %d / %v", len(byType), err)
	}
	none, _, err := s.List(ctx, job.ListQuery{Queue: "nope"})
	if err != nil || len(none) != 0 {
		t.Fatalf("unknown queue: %d / %v", len(none), err)
	}
	// Limit 0 is the default page; an exact multiple has no phantom page.
	def, _, err := s.List(ctx, job.ListQuery{})
	if err != nil || len(def) != 31 {
		t.Fatalf("default limit: %d / %v", len(def), err)
	}
	exact, next, err := s.List(ctx, job.ListQuery{Queue: "list", Limit: 30})
	if err != nil || len(exact) != 30 || !next.IsZero() {
		t.Fatalf("exact page: %d / %v / %v", len(exact), next, err)
	}
}

func TestCancel(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)
	ctx := context.Background()

	q := mustEnqueue(t, s, newJob(t, "cancel"))
	got, err := s.Cancel(ctx, q.ID)
	if err != nil || got.Status != job.StatusCancelled || got.FinishedAt == nil {
		t.Fatalf("cancel queued: %+v / %v", got, err)
	}
	if evs, _ := s.Events(ctx, q.ID); fmt.Sprint(eventStatuses(evs)) != "[unspecified->queued queued->cancelled]" {
		t.Fatalf("events: %v", eventStatuses(evs))
	}
	if _, err := s.Cancel(ctx, q.ID); !errors.Is(err, job.ErrNotCancellable) {
		t.Fatalf("cancel cancelled: want ErrNotCancellable got %v", err)
	}
	if _, err := s.Cancel(ctx, job.NewID()); !errors.Is(err, job.ErrNotFound) {
		t.Fatalf("cancel unknown: want ErrNotFound got %v", err)
	}

	// Leased: cooperative. The job stays leased with cancel_requested until the
	// worker's Complete/Fail lands, which ends it cancelled either way.
	l := mustEnqueue(t, s, newJob(t, "cancel"))
	mustClaim(t, s, "cancel", "w1", 1, time.Minute)
	got, err = s.Cancel(ctx, l.ID)
	if err != nil || got.Status != job.StatusLeased || !got.CancelRequested || got.LeaseOwner != "w1" {
		t.Fatalf("cancel leased: %+v / %v", got, err)
	}
	if got, err := s.Cancel(ctx, l.ID); err != nil || !got.CancelRequested { // idempotent
		t.Fatalf("second cancel leased: %+v / %v", got, err)
	}
	if err := s.Complete(ctx, curLease(t, s, l.ID), "w1", nil); err != nil {
		t.Fatalf("Complete after cancel: %v", err)
	}
	if got := mustGet(t, s, l.ID); got.Status != job.StatusCancelled || got.FinishedAt == nil || got.LeaseOwner != "" {
		t.Fatalf("after complete: %+v", got)
	}
	if evs, _ := s.Events(ctx, l.ID); evs[len(evs)-1].To != job.StatusCancelled || evs[len(evs)-1].From != job.StatusLeased {
		t.Fatalf("events: %v", eventStatuses(evs))
	}

	// Fail after a cancel also ends cancelled, never queued.
	f := mustEnqueue(t, s, newJob(t, "cancel"))
	mustClaim(t, s, "cancel", "w1", 1, time.Minute)
	if _, err := s.Cancel(ctx, f.ID); err != nil {
		t.Fatal(err)
	}
	st, err := s.Fail(ctx, curLease(t, s, f.ID), "w1", "context canceled", false, dbNow(t, s).Add(time.Second))
	if err != nil || st != job.StatusCancelled {
		t.Fatalf("Fail after cancel: %v / %v", st, err)
	}
	if got := mustGet(t, s, f.ID); got.Status != job.StatusCancelled || got.LastError != "context canceled" {
		t.Fatalf("after fail: %+v", got)
	}
}

func TestRetry(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)
	ctx := context.Background()

	j := mustEnqueue(t, s, newJob(t, "retry", func(n *job.NewJob) { n.MaxAttempts = 1 }))
	if _, err := s.Retry(ctx, j.ID); !errors.Is(err, job.ErrNotRetryable) {
		t.Fatalf("retry queued: want ErrNotRetryable got %v", err)
	}
	mustClaim(t, s, "retry", "w1", 1, time.Minute)
	if _, err := s.Retry(ctx, j.ID); !errors.Is(err, job.ErrNotRetryable) {
		t.Fatalf("retry leased: want ErrNotRetryable got %v", err)
	}
	if st, err := s.Fail(ctx, curLease(t, s, j.ID), "w1", "nope", false, dbNow(t, s)); err != nil || st != job.StatusDead {
		t.Fatalf("Fail: %v / %v", st, err)
	}
	before := dbNow(t, s)
	got, err := s.Retry(ctx, j.ID)
	if err != nil {
		t.Fatalf("Retry: %v", err)
	}
	if got.Status != job.StatusQueued || got.Attempts != 0 || got.LastError != "" || got.FinishedAt != nil ||
		got.RunAt.Before(before) || got.RunAt.After(before.Add(5*time.Second)) {
		t.Fatalf("retried: %+v", got)
	}
	if evs, _ := s.Events(ctx, j.ID); evs[len(evs)-1].From != job.StatusDead || evs[len(evs)-1].To != job.StatusQueued {
		t.Fatalf("events: %v", eventStatuses(evs))
	}
	if c := mustClaim(t, s, "retry", "w2", 1, time.Minute); len(c) != 1 || c[0].Attempts != 1 {
		t.Fatalf("claim after retry: %+v", c)
	}
	// Cancelled is retryable too and clears cancel_requested.
	c := mustEnqueue(t, s, newJob(t, "retry"))
	mustClaim(t, s, "retry", "w1", 1, time.Minute)
	if _, err := s.Cancel(ctx, c.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Complete(ctx, curLease(t, s, c.ID), "w1", nil); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Retry(ctx, c.ID); err != nil || got.Status != job.StatusQueued || got.CancelRequested {
		t.Fatalf("retry cancelled: %+v / %v", got, err)
	}
	if _, err := s.Retry(ctx, job.NewID()); !errors.Is(err, job.ErrNotFound) {
		t.Fatalf("retry unknown: want ErrNotFound got %v", err)
	}
}

func TestPurge(t *testing.T) {
	t.Parallel()
	s, pool := newStore(t)
	ctx := context.Background()

	done1 := mustEnqueue(t, s, newJob(t, "purge"))
	done2 := mustEnqueue(t, s, newJob(t, "purge"))
	dead := mustEnqueue(t, s, newJob(t, "purge", func(n *job.NewJob) { n.MaxAttempts = 1 }))
	live := mustEnqueue(t, s, newJob(t, "purge"))
	elsewhere := mustEnqueue(t, s, newJob(t, "keep"))
	mustClaim(t, s, "purge", "w1", 3, time.Minute)
	for _, id := range []job.ID{done1.ID, done2.ID} {
		if err := s.Complete(ctx, curLease(t, s, id), "w1", nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Fail(ctx, curLease(t, s, dead.ID), "w1", "x", true, dbNow(t, s)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Cancel(ctx, elsewhere.ID); err != nil {
		t.Fatal(err)
	}

	if n, err := s.Purge(ctx, job.PurgeQuery{Queue: "purge", OlderThan: time.Hour}); err != nil || n != 0 {
		t.Fatalf("purge too old: %d / %v", n, err)
	}
	if n, err := s.Purge(ctx, job.PurgeQuery{Queue: "purge", Statuses: []job.Status{job.StatusSucceeded}, Limit: 1}); err != nil || n != 1 {
		t.Fatalf("purge limit 1: %d / %v", n, err)
	}
	if n, err := s.Purge(ctx, job.PurgeQuery{Queue: "purge"}); err != nil || n != 2 {
		t.Fatalf("purge rest: %d / %v", n, err)
	}
	for _, id := range []job.ID{done1.ID, done2.ID, dead.ID} {
		if _, err := s.Get(ctx, id); !errors.Is(err, job.ErrNotFound) {
			t.Fatalf("purged %s still there: %v", id, err)
		}
		if evs, _ := s.Events(ctx, id); len(evs) != 0 {
			t.Fatalf("events of purged %s remain: %v", id, eventStatuses(evs))
		}
	}
	mustGet(t, s, live.ID)
	mustGet(t, s, elsewhere.ID) // other queue untouched
	if _, err := s.Purge(ctx, job.PurgeQuery{Statuses: []job.Status{job.StatusQueued}}); !errors.Is(err, job.ErrInvalidArgument) {
		t.Fatalf("purge queued: want ErrInvalidArgument got %v", err)
	}
	if n, err := s.Purge(ctx, job.PurgeQuery{}); err != nil || n != 1 {
		t.Fatalf("purge all queues: %d / %v", n, err)
	}
	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM job_events").Scan(&n); err != nil || n != 1 {
		t.Fatalf("events left: %d / %v (want 1: the live job's enqueue)", n, err)
	}
	if pruned, err := s.PruneEvents(ctx, 0); err != nil || pruned != 1 {
		t.Fatalf("PruneEvents: %d / %v", pruned, err)
	}
}

func TestDepth(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)
	ctx := context.Background()
	if rows, err := s.Depth(ctx); err != nil || len(rows) != 0 {
		t.Fatalf("empty depth: %v / %v", rows, err)
	}
	oldest := dbNow(t, s)
	for range 3 {
		mustEnqueue(t, s, newJob(t, "a"))
	}
	mustEnqueue(t, s, newJob(t, "b", func(n *job.NewJob) { n.Delay = time.Hour }))
	mustClaim(t, s, "a", "w1", 1, time.Minute)

	rows, err := s.Depth(ctx)
	if err != nil {
		t.Fatalf("Depth: %v", err)
	}
	type cell struct {
		q string
		s job.Status
	}
	got := map[cell]job.DepthRow{}
	for _, r := range rows {
		got[cell{r.Queue, r.Status}] = r
	}
	if len(got) != 3 || got[cell{"a", job.StatusQueued}].N != 2 || got[cell{"a", job.StatusLeased}].N != 1 || got[cell{"b", job.StatusQueued}].N != 1 {
		t.Fatalf("depth: %+v", rows)
	}
	if r := got[cell{"a", job.StatusQueued}]; r.OldestRunAt.Before(oldest) || r.OldestRunAt.After(oldest.Add(5*time.Second)) {
		t.Fatalf("oldest run_at: %v", r.OldestRunAt)
	}
	if r := got[cell{"b", job.StatusQueued}]; r.OldestRunAt.Before(oldest.Add(time.Hour)) {
		t.Fatalf("delayed oldest run_at: %v", r.OldestRunAt)
	}
}

func TestEventModeTerminal(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t, postgres.WithEventMode(postgres.EventsTerminal))
	ctx := context.Background()
	j := mustEnqueue(t, s, newJob(t, "t"))
	mustClaim(t, s, "t", "w1", 1, time.Minute)
	if evs, _ := s.Events(ctx, j.ID); len(evs) != 0 {
		t.Fatalf("terminal mode must skip enqueue/claim events: %v", eventStatuses(evs))
	}
	if err := s.Complete(ctx, curLease(t, s, j.ID), "w1", nil); err != nil {
		t.Fatal(err)
	}
	if evs, _ := s.Events(ctx, j.ID); fmt.Sprint(eventStatuses(evs)) != "[leased->succeeded]" {
		t.Fatalf("terminal events: %v", eventStatuses(evs))
	}
	if m, err := postgres.ParseEventMode("TERMINAL"); err != nil || m != postgres.EventsTerminal {
		t.Fatalf("ParseEventMode: %v / %v", m, err)
	}
	if _, err := postgres.ParseEventMode("some"); !errors.Is(err, job.ErrInvalidArgument) {
		t.Fatalf("ParseEventMode(bad): %v", err)
	}
}

func TestNotifierWakesWait(t *testing.T) {
	t.Parallel()
	pool := newTestPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var raw [4]byte
	_, _ = rand.Read(raw[:])
	channel := "test_" + hex.EncodeToString(raw[:]) // NOTIFY is database-global
	queue := "q_" + hex.EncodeToString(raw[:])
	s := postgres.NewStore(pool, discard, postgres.WithNotifyChannel(channel))
	n := postgres.NewNotifier(pool, discard, postgres.WithChannel(channel), postgres.WithPollFloor(10*time.Second))

	ch := n.Wait(ctx, queue)
	other := n.Wait(ctx, "other-"+queue)
	runDone := make(chan error, 1)
	go func() { runDone <- n.Run(ctx) }()

	// Connecting wakes every waiter once (covers work that arrived while down).
	for name, c := range map[string]<-chan struct{}{"queue": ch, "other": other} {
		select {
		case <-c:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: no wake-up on connect", name)
		}
	}

	start := time.Now()
	mustEnqueue(t, s, newJob(t, queue))
	select {
	case _, ok := <-ch:
		if !ok {
			t.Fatal("channel closed early")
		}
		if d := time.Since(start); d > 500*time.Millisecond {
			t.Fatalf("wake-up took %v", d)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("enqueue did not wake Wait within 500ms")
	}
	select {
	case <-other:
		t.Fatal("notification leaked to another queue")
	case <-time.After(100 * time.Millisecond):
	}

	// Ready on its own wakes too, and many signals coalesce into one pending slot.
	for range 5 {
		if err := n.Ready(ctx, queue); err != nil {
			t.Fatalf("Ready: %v", err)
		}
	}
	select {
	case <-ch:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Ready did not wake Wait")
	}
	time.Sleep(50 * time.Millisecond)
	pending := 0
	for {
		select {
		case <-ch:
			pending++
			continue
		default:
		}
		break
	}
	if pending > 1 {
		t.Fatalf("signals must coalesce, got %d pending", pending)
	}

	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("Wait channel not closed after cancel")
		}
	}
}

func TestLockerExclusive(t *testing.T) {
	t.Parallel()
	pool := newTestPool(t)
	ctx := context.Background()
	var raw [8]byte
	_, _ = rand.Read(raw[:])
	key := int64(uint64(raw[0])<<56|uint64(raw[1])<<48|uint64(raw[2])<<40|uint64(raw[3])<<32|uint64(raw[4])<<24|uint64(raw[5])<<16|uint64(raw[6])<<8|uint64(raw[7])) & (1<<62 - 1)

	a, b := postgres.NewLocker(pool), postgres.NewLocker(pool)
	release, ok, err := a.TryLock(ctx, key)
	if err != nil || !ok || release == nil {
		t.Fatalf("first TryLock: %v / %v", ok, err)
	}
	if rel, ok, err := b.TryLock(ctx, key); err != nil || ok || rel != nil {
		t.Fatalf("second TryLock while held: %v / %v", ok, err)
	}
	if rel, ok, err := b.TryLock(ctx, key+1); err != nil || !ok {
		t.Fatalf("other key: %v / %v", ok, err)
	} else if err := rel(ctx); err != nil {
		t.Fatalf("release other: %v", err)
	}
	if err := release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := release(ctx); err == nil {
		t.Fatal("second release must error")
	}
	rel2, ok, err := b.TryLock(ctx, key)
	if err != nil || !ok {
		t.Fatalf("TryLock after release: %v / %v", ok, err)
	}
	if err := rel2(ctx); err != nil {
		t.Fatalf("release 2: %v", err)
	}
}

func TestSchedules(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)
	ctx := context.Background()
	next := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

	mk := func(name string, next time.Time) job.Schedule {
		return job.Schedule{
			Name: name, Cron: "*/5 * * * *", Timezone: "America/New_York", Enabled: true, NextRun: next,
			Template: job.NewJob{
				Queue: "sched", Type: "tick", Payload: []byte(`{"n":1}`), Priority: 3, MaxAttempts: 2,
				Backoff: job.Policy{Base: 2 * time.Second, Max: time.Minute, Factor: 3, Jitter: false},
			},
		}
	}
	created, err := s.CreateSchedule(ctx, mk("alpha", next))
	if err != nil {
		t.Fatalf("CreateSchedule: %v", err)
	}
	if created.ID == uuid.Nil || created.Name != "alpha" || created.Timezone != "America/New_York" || !created.NextRun.Equal(next) ||
		created.LastRun != nil || created.Template.Queue != "sched" || created.Template.Type != "tick" || created.Template.Priority != 3 ||
		created.Template.MaxAttempts != 2 || created.Template.Backoff.Base != 2*time.Second || string(created.Template.Payload) != `{"n": 1}` {
		t.Fatalf("created: %+v", created)
	}
	if _, err := s.CreateSchedule(ctx, mk("alpha", next)); !errors.Is(err, job.ErrScheduleExists) {
		t.Fatalf("duplicate name: want ErrScheduleExists got %v", err)
	}
	got, err := s.GetSchedule(ctx, created.ID)
	if err != nil || got.ID != created.ID || got.Cron != "*/5 * * * *" {
		t.Fatalf("GetSchedule: %+v / %v", got, err)
	}
	if _, err := s.GetSchedule(ctx, job.NewID()); !errors.Is(err, job.ErrScheduleNotFound) {
		t.Fatalf("GetSchedule(unknown): %v", err)
	}

	upd := got
	upd.Cron, upd.Timezone, upd.Enabled, upd.NextRun = "0 * * * *", "", false, next.Add(time.Hour)
	upd.Template.Payload = []byte(`{"n":2}`)
	updated, err := s.UpdateSchedule(ctx, upd)
	if err != nil || updated.Cron != "0 * * * *" || updated.Timezone != "UTC" || updated.Enabled || !updated.NextRun.Equal(next.Add(time.Hour)) ||
		string(updated.Template.Payload) != `{"n": 2}` || !updated.UpdatedAt.After(created.UpdatedAt) {
		t.Fatalf("UpdateSchedule: %+v / %v", updated, err)
	}
	missing := upd
	missing.ID = job.NewID()
	if _, err := s.UpdateSchedule(ctx, missing); !errors.Is(err, job.ErrScheduleNotFound) {
		t.Fatalf("UpdateSchedule(unknown): %v", err)
	}

	// Listing pages by name.
	for _, name := range []string{"charlie", "bravo", "delta"} {
		if _, err := s.CreateSchedule(ctx, mk(name, next)); err != nil {
			t.Fatal(err)
		}
	}
	page1, err := s.ListSchedules(ctx, "", 2)
	if err != nil || len(page1) != 2 || page1[0].Name != "alpha" || page1[1].Name != "bravo" {
		t.Fatalf("page1: %+v / %v", page1, err)
	}
	page2, err := s.ListSchedules(ctx, page1[1].Name, 10)
	if err != nil || len(page2) != 2 || page2[0].Name != "charlie" || page2[1].Name != "delta" {
		t.Fatalf("page2: %+v / %v", page2, err)
	}

	// Due: alpha is disabled; bravo/charlie/delta are due at next, none before.
	if due, err := s.DueSchedules(ctx, next.Add(-time.Second), 10); err != nil || len(due) != 0 {
		t.Fatalf("due before: %d / %v", len(due), err)
	}
	due, err := s.DueSchedules(ctx, next, 2)
	if err != nil || len(due) != 2 {
		t.Fatalf("due limit 2: %d / %v", len(due), err)
	}
	last := next
	if err := s.AdvanceSchedule(ctx, due[0].ID, last, next.Add(5*time.Minute)); err != nil {
		t.Fatalf("AdvanceSchedule: %v", err)
	}
	adv, _ := s.GetSchedule(ctx, due[0].ID)
	if adv.LastRun == nil || !adv.LastRun.Equal(last) || !adv.NextRun.Equal(next.Add(5*time.Minute)) {
		t.Fatalf("advanced: %+v", adv)
	}
	if err := s.AdvanceSchedule(ctx, job.NewID(), last, next); !errors.Is(err, job.ErrScheduleNotFound) {
		t.Fatalf("AdvanceSchedule(unknown): %v", err)
	}
	if due, err := s.DueSchedules(ctx, next, 10); err != nil || len(due) != 2 {
		t.Fatalf("due after advance: %d / %v", len(due), err)
	}

	if err := s.DeleteSchedule(ctx, created.ID); err != nil {
		t.Fatalf("DeleteSchedule: %v", err)
	}
	if err := s.DeleteSchedule(ctx, created.ID); !errors.Is(err, job.ErrScheduleNotFound) {
		t.Fatalf("DeleteSchedule twice: %v", err)
	}
	if rest, _ := s.ListSchedules(ctx, "", 0); len(rest) != 3 {
		t.Fatalf("after delete: %d", len(rest))
	}
}
