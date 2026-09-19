package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	jobrunv1 "github.com/crowbett/jobrun/gen/jobrun/v1"
	"github.com/crowbett/jobrun/internal/job"
	"github.com/crowbett/jobrun/internal/postgres"
)

// The plan's pass criterion (docs/PLAN.md, "Load target and benchmark"):
// 10k jobs/min = 167/s sustained for the whole run, p99 enqueue->done under a
// second, and a queue depth that stays flat (the workers keep up).
const (
	benchMinRate = 167.0
	benchMaxP99  = time.Second
	// benchFlatSeconds bounds the backlog: the depth must never exceed this
	// many seconds' worth of the target rate while enqueuing.
	benchFlatSeconds = 2
	benchDirectTick  = 100 * time.Millisecond
)

type benchOpts struct {
	rate     int
	duration time.Duration
	queue    string
	typ      string
	payload  string
	direct   bool
	workers  int
	wait     time.Duration
	sample   time.Duration
}

type benchSample struct {
	at             time.Duration
	queued, leased int64
	enqueued       int64
}

// bench is an open-loop enqueuer at a fixed rate followed by a drain watch
// and a latency sweep. Enqueue goes over gRPC (default) or, with --direct,
// through Store.EnqueueBatch on DATABASE_URL so the RPC is out of the picture;
// the watch and the sweep always use gRPC (Stats and List).
func (c *client) bench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	o := benchOpts{}
	fs.IntVar(&o.rate, "rate", 200, "target enqueue rate, jobs/s")
	fs.DurationVar(&o.duration, "duration", 60*time.Second, "how long to enqueue")
	fs.StringVar(&o.queue, "queue", "bench", "queue (jobrund must list it in WORKER_QUEUES)")
	fs.StringVar(&o.typ, "type", "noop", "job type")
	fs.StringVar(&o.payload, "payload", "{}", "JSON payload of every job")
	fs.BoolVar(&o.direct, "direct", false, "enqueue with EnqueueBatch on DATABASE_URL instead of gRPC")
	fs.IntVar(&o.workers, "workers", 8, "enqueuer goroutines (gRPC mode)")
	fs.DurationVar(&o.wait, "wait", 60*time.Second, "how long to wait for the last job after enqueuing stops")
	fs.DurationVar(&o.sample, "sample", 5*time.Second, "depth sampling interval")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if o.rate < 1 || o.duration <= 0 || o.workers < 1 {
		return errors.New("--rate and --workers must be >= 1 and --duration > 0")
	}

	mode := "grpc"
	if o.direct {
		mode = "direct"
	}
	fmt.Printf("bench: queue=%s type=%s rate=%d/s duration=%s mode=%s workers=%d\n",
		o.queue, o.typ, o.rate, o.duration, mode, o.workers)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Sampler: queue depth over time from the cached Stats RPC --------------
	var (
		samples   []benchSample
		samplesMu sync.Mutex
		enqueued  atomic.Int64
	)
	start := time.Now()
	sampler := make(chan struct{})
	go func() {
		defer close(sampler)
		t := time.NewTicker(o.sample)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			q, l, err := c.depth(o.queue)
			if err != nil {
				fmt.Fprintln(os.Stderr, "stats:", err)
				continue
			}
			s := benchSample{at: time.Since(start).Round(time.Second), queued: q, leased: l, enqueued: enqueued.Load()}
			samplesMu.Lock()
			samples = append(samples, s)
			samplesMu.Unlock()
			fmt.Printf("t=+%-6s queued=%-6d leased=%-4d enqueued=%d\n", s.at, s.queued, s.leased, s.enqueued)
		}
	}()

	// --- Enqueue phase --------------------------------------------------------------
	ids := &idSet{m: make(map[string]struct{}, o.rate*int(o.duration/time.Second)+1)}
	var stats enqueueStats
	var err error
	if o.direct {
		stats, err = c.enqueueDirect(ctx, o, ids, &enqueued)
	} else {
		stats, err = c.enqueueGRPC(ctx, o, ids, &enqueued)
	}
	if err != nil {
		return err
	}
	enqueueWall := stats.wall
	achieved := float64(ids.len()) / enqueueWall.Seconds()
	fmt.Printf("enqueue: %d jobs in %s (%.1f/s achieved, %d errors, %d lagged ticks)\n",
		ids.len(), enqueueWall.Round(10*time.Millisecond), achieved, stats.errors, stats.lagged)
	if ids.len() == 0 {
		return errors.New("nothing was enqueued")
	}

	// --- Drain watch: cached depth reaches zero, then the sweep confirms --------
	drainStart := time.Now()
	deadline := drainStart.Add(o.wait)
	var res benchResult
	for {
		q, l, derr := c.depth(o.queue)
		if derr == nil && q == 0 && l == 0 {
			if res, err = c.sweep(o, ids, start); err != nil {
				return err
			}
			if res.missing == 0 && res.nonTerminal == 0 {
				break
			}
		}
		if time.Now().After(deadline) {
			if res, err = c.sweep(o, ids, start); err != nil {
				return err
			}
			fmt.Printf("drain: timed out after %s with %d jobs not terminal and %d not found (is %q in WORKER_QUEUES?)\n",
				o.wait, res.nonTerminal, res.missing, o.queue)
			break
		}
		time.Sleep(time.Second)
	}
	cancel()
	<-sampler
	total := time.Since(start)

	// --- Report -------------------------------------------------------------------------
	fmt.Printf("drain:   %d/%d terminal %s after enqueue stopped (succeeded=%d dead=%d cancelled=%d other=%d)\n",
		res.terminal(), ids.len(), time.Since(drainStart).Round(100*time.Millisecond), res.succeeded, res.dead, res.cancelled, res.nonTerminal+res.missing)
	if len(res.latencies) == 0 {
		return errors.New("no completed jobs to measure")
	}
	slices.Sort(res.latencies)
	p50, p95, p99 := pct(res.latencies, 0.50), pct(res.latencies, 0.95), pct(res.latencies, 0.99)
	maxLat := res.latencies[len(res.latencies)-1]
	span := res.lastFinished.Sub(res.firstCreated)
	if span <= 0 {
		span = total
	}
	throughput := float64(len(res.latencies)) / span.Seconds()
	fmt.Printf("latency (enqueue -> done, db clock): p50=%s p95=%s p99=%s max=%s\n",
		p50.Round(time.Millisecond), p95.Round(time.Millisecond), p99.Round(time.Millisecond), maxLat.Round(time.Millisecond))
	fmt.Printf("throughput: %d completed over %s (first created -> last finished) = %.1f/s\n",
		len(res.latencies), span.Round(10*time.Millisecond), throughput)

	samplesMu.Lock()
	var maxDepth int64
	for _, s := range samples {
		if s.at <= enqueueWall+o.sample && s.queued+s.leased > maxDepth {
			maxDepth = s.queued + s.leased
		}
	}
	samplesMu.Unlock()
	flatBound := int64(benchFlatSeconds * o.rate)
	fmt.Printf("depth: max queued+leased while enqueuing = %d (flat bound %d = %ds of the target rate, sampled every %s)\n",
		maxDepth, flatBound, benchFlatSeconds, o.sample)

	okRate := throughput >= benchMinRate
	okP99 := p99 < benchMaxP99
	okFlat := maxDepth <= flatBound
	okAll := res.succeeded == int64(ids.len())
	verdict := "PASS"
	if !okRate || !okP99 || !okFlat || !okAll {
		verdict = "FAIL"
	}
	fmt.Printf("%s: throughput %.1f/s %s %.0f/s, p99 %s %s %s, depth %s, %d/%d succeeded\n",
		verdict, throughput, cmp(okRate), benchMinRate, p99.Round(time.Millisecond), cmpLt(okP99), benchMaxP99,
		flatWord(okFlat), res.succeeded, ids.len())
	if verdict == "FAIL" {
		return errors.New("bench criterion not met")
	}
	return nil
}

type enqueueStats struct {
	wall   time.Duration
	errors int64
	lagged int64
}

// enqueueGRPC ticks at 1/rate and hands each tick to a worker; a tick with no
// free worker slot is counted as lagged rather than queued, keeping the loop
// open-loop (the schedule never stretches to the server's pace).
func (c *client) enqueueGRPC(ctx context.Context, o benchOpts, ids *idSet, enqueued *atomic.Int64) (enqueueStats, error) {
	var st enqueueStats
	work := make(chan struct{}, o.workers*4)
	var wg sync.WaitGroup
	var errs atomic.Int64
	for range o.workers {
		wg.Go(func() {
			for range work {
				cctx, ccancel := context.WithTimeout(ctx, c.timeout)
				resp, err := c.jobs.Enqueue(cctx, &jobrunv1.EnqueueRequest{Queue: o.queue, Type: o.typ, Payload: []byte(o.payload)})
				ccancel()
				if err != nil {
					if errs.Add(1) <= 3 {
						fmt.Fprintln(os.Stderr, "enqueue:", err)
					}
					continue
				}
				ids.add(resp.GetJob().GetId())
				enqueued.Add(1)
			}
		})
	}
	tick := time.NewTicker(time.Second / time.Duration(o.rate))
	defer tick.Stop()
	begin := time.Now()
	end := begin.Add(o.duration)
	for now := range tick.C {
		if now.After(end) {
			break
		}
		select {
		case work <- struct{}{}:
		default:
			st.lagged++
		}
	}
	close(work)
	wg.Wait()
	st.wall = time.Since(begin)
	st.errors = errs.Load()
	return st, nil
}

// enqueueDirect opens the store on DATABASE_URL and writes rate/10 jobs every
// 100 ms with EnqueueBatch (COPY), in one goroutine so a slow batch shows up
// as lag rather than stretching the schedule.
func (c *client) enqueueDirect(ctx context.Context, o benchOpts, ids *idSet, enqueued *atomic.Int64) (enqueueStats, error) {
	var st enqueueStats
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return st, errors.New("--direct needs DATABASE_URL")
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	pool, err := postgres.Open(ctx, url, 4, log)
	if err != nil {
		return st, fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close()
	store := postgres.NewStore(pool, log)

	perTick := max(1, o.rate/int(time.Second/benchDirectTick))
	batches := make(chan []job.Job, 8)
	var errs atomic.Int64
	var wg sync.WaitGroup
	wg.Go(func() {
		for b := range batches {
			bctx, bcancel := context.WithTimeout(ctx, c.timeout)
			_, err := store.EnqueueBatch(bctx, b)
			bcancel()
			if err != nil {
				if errs.Add(1) <= 3 {
					fmt.Fprintln(os.Stderr, "enqueue batch:", err)
				}
				continue
			}
			for i := range b {
				ids.add(b[i].ID.String())
			}
			enqueued.Add(int64(len(b)))
		}
	})
	tick := time.NewTicker(benchDirectTick)
	defer tick.Stop()
	begin := time.Now()
	end := begin.Add(o.duration)
	for now := range tick.C {
		if now.After(end) {
			break
		}
		b := make([]job.Job, 0, perTick)
		for range perTick {
			j, err := job.New(job.NewJob{Queue: o.queue, Type: o.typ, Payload: []byte(o.payload)})
			if err != nil {
				close(batches)
				return st, err
			}
			b = append(b, j)
		}
		select {
		case batches <- b:
		default:
			st.lagged++
		}
	}
	close(batches)
	wg.Wait()
	st.wall = time.Since(begin)
	st.errors = errs.Load()
	return st, nil
}

// depth returns the cached queued and leased counts of queue.
func (c *client) depth(queue string) (queued, leased int64, err error) {
	ctx, cancel := c.call()
	defer cancel()
	resp, err := c.jobs.Stats(ctx, &jobrunv1.StatsRequest{Queue: queue})
	if err != nil {
		return 0, 0, err
	}
	for _, d := range resp.GetDepths() {
		switch d.GetStatus() { //nolint:exhaustive // only the live statuses matter here
		case jobrunv1.JobStatus_JOB_STATUS_QUEUED:
			queued += d.GetCount()
		case jobrunv1.JobStatus_JOB_STATUS_LEASED:
			leased += d.GetCount()
		}
	}
	return queued, leased, nil
}

type benchResult struct {
	latencies                  []time.Duration
	succeeded, dead, cancelled int64
	nonTerminal, missing       int64
	firstCreated, lastFinished time.Time
}

func (r benchResult) terminal() int64 { return r.succeeded + r.dead + r.cancelled }

// sweep pages the queue newest-first over List and records created_at ->
// finished_at for every job this run enqueued. The job row is the cheapest
// correct source: both stamps are database-clock and one page carries 200
// jobs, where job_events would need four rows per job.
func (c *client) sweep(o benchOpts, ids *idSet, start time.Time) (benchResult, error) {
	var res benchResult
	seen := make(map[string]struct{}, ids.len())
	token := ""
	floor := start.Add(-5 * time.Minute) // older than this cannot be ours, whatever the clock skew
	for {
		ctx, cancel := c.call()
		resp, err := c.jobs.List(ctx, &jobrunv1.ListRequest{Queue: o.queue, PageSize: 200, PageToken: token})
		cancel()
		if err != nil {
			return res, fmt.Errorf("list: %w", err)
		}
		for _, j := range resp.GetJobs() {
			if !ids.has(j.GetId()) {
				continue
			}
			seen[j.GetId()] = struct{}{}
			created := j.GetCreatedAt().AsTime()
			if res.firstCreated.IsZero() || created.Before(res.firstCreated) {
				res.firstCreated = created
			}
			switch j.GetStatus() {
			case jobrunv1.JobStatus_JOB_STATUS_SUCCEEDED:
				res.succeeded++
				fin := j.GetFinishedAt().AsTime()
				res.latencies = append(res.latencies, fin.Sub(created))
				if fin.After(res.lastFinished) {
					res.lastFinished = fin
				}
			case jobrunv1.JobStatus_JOB_STATUS_DEAD:
				res.dead++
			case jobrunv1.JobStatus_JOB_STATUS_CANCELLED:
				res.cancelled++
			default:
				res.nonTerminal++
			}
		}
		jobs := resp.GetJobs()
		token = resp.GetNextPageToken()
		if token == "" || len(seen) == ids.len() || (len(jobs) > 0 && jobs[len(jobs)-1].GetCreatedAt().AsTime().Before(floor)) {
			break
		}
	}
	res.missing = int64(ids.len() - len(seen))
	return res, nil
}

// idSet is the set of job ids this run enqueued.
type idSet struct {
	mu sync.Mutex
	m  map[string]struct{}
}

func (s *idSet) add(id string) {
	s.mu.Lock()
	s.m[id] = struct{}{}
	s.mu.Unlock()
}

func (s *idSet) has(id string) bool {
	s.mu.Lock()
	_, ok := s.m[id]
	s.mu.Unlock()
	return ok
}

func (s *idSet) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.m)
}

// pct returns the p-quantile (nearest-rank) of sorted.
func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(math.Ceil(p*float64(len(sorted)))) - 1
	return sorted[max(0, min(i, len(sorted)-1))]
}

func cmp(ok bool) string {
	if ok {
		return ">="
	}
	return "<"
}

func cmpLt(ok bool) string {
	if ok {
		return "<"
	}
	return ">="
}

func flatWord(ok bool) string {
	if ok {
		return "flat"
	}
	return "NOT flat"
}
