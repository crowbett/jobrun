//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/crowbett/jobrun/internal/job"
	"github.com/crowbett/jobrun/internal/postgres"
	"github.com/crowbett/jobrun/internal/reaper"
	"github.com/crowbett/jobrun/internal/server"
	"github.com/crowbett/jobrun/pkg/jobworker"
)

// childEnv makes the test binary act as the jobworker command, so the test can
// run a real worker process and kill it by PID without a separate go build.
const childEnv = "JOBWORKER_TEST_CHILD_ARGS"

func TestMain(m *testing.M) {
	if args := os.Getenv(childEnv); args != "" {
		if err := run(strings.Fields(args)); err != nil {
			fmt.Fprintln(os.Stderr, "jobworker child:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// schemaPool opens a pool on a private, migrated schema (dropped at cleanup).
func schemaPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	var raw [6]byte
	_, _ = rand.Read(raw[:])
	schema := "test_kill_" + hex.EncodeToString(raw[:])
	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	_ = admin.Close(ctx)
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := postgres.OpenConfig(ctx, cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		if a, err := pgx.Connect(ctx, url); err == nil {
			_, _ = a.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
			_ = a.Close(ctx)
		}
	})
	if err := postgres.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return pool
}

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestKillRemoteWorkerMidJob is the crash path end to end against Postgres: a
// real jobworker process leases a long job and is killed by PID; nobody
// heartbeats the lease, it expires, the reaper requeues the job with its
// backoff, and a second remote worker leases it with a newer fencing token and
// completes it. The dead worker's lease can never finish the job.
func TestKillRemoteWorkerMidJob(t *testing.T) {
	pool := schemaPool(t)
	logs := &syncBuf{}
	log := slog.New(slog.NewTextHandler(logs, nil))
	store := postgres.NewStore(pool, log, postgres.WithNotifyChannel(""))
	const ttl = 2 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ws := server.NewWorkerServer(store, log, server.WorkerConfig{LeaseTTL: ttl, PollFloor: 50 * time.Millisecond})
	gs := grpc.NewServer()
	ws.Register(gs)
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	rp := reaper.NewReaper(store, nil, log, reaper.Config{LeaseTTL: ttl})
	go func() { _ = rp.Run(ctx) }()

	j, err := job.New(job.NewJob{
		Queue: "killq", Type: "sleep", Payload: []byte(`{"ms":600000}`),
		Backoff: job.Policy{Base: 10 * time.Millisecond, Max: 10 * time.Millisecond, Factor: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Enqueue(ctx, j); err != nil {
		t.Fatal(err)
	}

	// Worker A: a real process running the jobworker command.
	childOut := &syncBuf{}
	child := exec.Command(os.Args[0], "-test.run=^$") // re-exec of this test binary as the worker
	child.Env = append(os.Environ(), childEnv+"=-addr "+lis.Addr().String()+" -queues killq -id worker-a -log-format text -concurrency 2")
	child.Stdout, child.Stderr = childOut, childOut
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Process.Kill(); _ = child.Wait() })

	var first job.Job
	waitFor(t, "worker-a to lease the job", 15*time.Second, func() bool {
		first, err = store.Get(ctx, j.ID)
		return err == nil && first.Status == job.StatusLeased && first.LeaseOwner == server.RemoteOwnerPrefix+"worker-a"
	})
	// Let it heartbeat at least once so the kill happens mid-job, not mid-claim.
	time.Sleep(ttl / 2)

	proc, err := os.FindProcess(child.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	killedAt := time.Now()
	if err := proc.Kill(); err != nil {
		t.Fatalf("kill pid %d: %v", child.Process.Pid, err)
	}
	_ = child.Wait()
	t.Logf("killed worker-a (pid %d) holding %s token %d", child.Process.Pid, j.ID, first.LeaseToken)

	// Worker B: in-process client, a fast "sleep".
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	var ranOn sync.Map
	wb := jobworker.New(conn, jobworker.Config{WorkerID: "worker-b", Queues: []string{"killq"}, Logger: log})
	_ = wb.Handle("sleep", jobworker.HandlerFunc(func(_ context.Context, rj jobworker.Job) error {
		ranOn.Store(rj.LeaseToken, rj.Attempt)
		return nil
	}))
	bctx, bstop := context.WithCancel(ctx)
	bdone := make(chan error, 1)
	go func() { bdone <- wb.Run(bctx) }()

	var final job.Job
	waitFor(t, "the job to succeed on worker-b", 30*time.Second, func() bool {
		final, err = store.Get(ctx, j.ID)
		return err == nil && final.Status == job.StatusSucceeded
	})
	recovered := time.Since(killedAt)
	bstop()
	if err := <-bdone; err != nil {
		t.Fatal(err)
	}

	if final.Attempts != 2 || final.LeaseToken != first.LeaseToken+1 {
		t.Fatalf("final job: attempts %d token %d, want 2 and %d", final.Attempts, final.LeaseToken, first.LeaseToken+1)
	}
	if a, ok := ranOn.Load(final.LeaseToken); !ok || a.(int32) != 2 {
		t.Fatalf("worker-b did not run attempt 2 under token %d", final.LeaseToken)
	}
	evs, err := store.Events(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(evs))
	for _, e := range evs {
		got = append(got, e.From.String()+"->"+e.To.String())
	}
	want := "[unspecified->queued queued->leased leased->queued queued->leased leased->succeeded]"
	if fmt.Sprint(got) != want {
		t.Fatalf("events %v, want %s", got, want)
	}
	if evs[2].Err != "lease expired" {
		t.Fatalf("requeue event error %q, want lease expired", evs[2].Err)
	}
	// The dead worker's lease is fenced for good.
	if err := store.Complete(ctx, job.Lease{ID: j.ID, Token: first.LeaseToken}, first.LeaseOwner, nil); err == nil {
		t.Fatal("the killed worker's lease completed the job")
	}
	if rp.Stats().Reaped < 1 || wb.Stats().Succeeded != 1 {
		t.Fatalf("reaper %+v worker-b %+v", rp.Stats(), wb.Stats())
	}
	// Lease TTL (2 s) + one reaper tick (TTL/3) + claim, with slack for a loaded box.
	if recovered > ttl+ttl/3+5*time.Second {
		t.Fatalf("recovery took %v", recovered)
	}
	t.Logf("kill -> succeeded on worker-b in %v (lease ttl %v, reaper tick %v)", recovered.Round(time.Millisecond), ttl, rp.Interval())
	if !strings.Contains(childOut.String(), "claim stream open") {
		t.Logf("child output:\n%s", childOut.String())
	}
}

// TestStolenLeaseIsFencedAgainstPostgres: a live remote worker whose lease
// expires under it (a stall: lease_until pushed into the past) loses the job
// to the reaper and reclaims it itself with a newer token. The first
// attempt's handler is fenced by the next heartbeat - its ctx cancelled, its
// result never reported - and the second attempt completes.
func TestStolenLeaseIsFencedAgainstPostgres(t *testing.T) {
	pool := schemaPool(t)
	log := slog.New(slog.NewTextHandler(&syncBuf{}, nil))
	store := postgres.NewStore(pool, log, postgres.WithNotifyChannel(""))
	const ttl = 1500 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ws := server.NewWorkerServer(store, log, server.WorkerConfig{LeaseTTL: ttl, PollFloor: 20 * time.Millisecond})
	gs := grpc.NewServer()
	ws.Register(gs)
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	rp := reaper.NewReaper(store, nil, log, reaper.Config{LeaseTTL: ttl})
	go func() { _ = rp.Run(ctx) }()

	j, err := job.New(job.NewJob{Queue: "stolen", Type: "block", Backoff: job.Policy{Base: time.Millisecond, Max: time.Millisecond, Factor: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Enqueue(ctx, j); err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	firstCancelled := make(chan time.Time, 1)
	w := jobworker.New(conn, jobworker.Config{WorkerID: "worker-s", Queues: []string{"stolen"}, Concurrency: 2, Logger: log})
	_ = w.Handle("block", jobworker.HandlerFunc(func(hctx context.Context, rj jobworker.Job) error {
		if rj.LeaseToken == 1 {
			<-hctx.Done()
			firstCancelled <- time.Now()
			return nil // claims success: must still never be completed
		}
		return nil
	}))
	wctx, wstop := context.WithCancel(ctx)
	wdone := make(chan error, 1)
	go func() { wdone <- w.Run(wctx) }()

	waitFor(t, "the first lease", 10*time.Second, func() bool {
		cur, err := store.Get(ctx, j.ID)
		return err == nil && cur.Status == job.StatusLeased && cur.LeaseToken == 1
	})
	expiredAt := time.Now()
	if _, err := pool.Exec(ctx, "UPDATE jobs SET lease_until = now() - interval '1 second' WHERE id = $1", j.ID); err != nil {
		t.Fatal(err)
	}
	var final job.Job
	waitFor(t, "the second attempt to succeed", 15*time.Second, func() bool {
		final, err = store.Get(ctx, j.ID)
		return err == nil && final.Status == job.StatusSucceeded
	})
	select {
	case at := <-firstCancelled:
		t.Logf("first attempt fenced %v after its lease expired", at.Sub(expiredAt).Round(time.Millisecond))
	case <-time.After(5 * time.Second):
		t.Fatal("the first attempt's handler was never cancelled")
	}
	wstop()
	if err := <-wdone; err != nil {
		t.Fatal(err)
	}
	if final.Attempts != 2 || final.LeaseToken != 2 {
		t.Fatalf("final: %+v", final)
	}
	st := w.Stats()
	if st.Fenced != 1 || st.Succeeded != 1 || st.LeaseLost != 0 {
		t.Fatalf("worker stats %+v: want 1 fenced, 1 succeeded, no refused Complete", st)
	}
	if ss := ws.Stats(); ss.LeaseLost != 0 || ss.Completed != 1 || ss.Fenced != 1 {
		t.Fatalf("server stats %+v", ss)
	}
}
