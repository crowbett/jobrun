package obs

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/crowbett/jobrun/internal/job"
)

// MeterName is the instrumentation scope of every jobrun instrument.
const MeterName = "jobrun"

// Snapshot is the runtime's cumulative counters since process start, gathered
// by cmd/jobrund from the worker pool, scheduler and reaper Stats() methods.
// The same values feed the OTel observable instruments here and the
// hand-written /metrics text in internal/admin, so the two cannot disagree.
type Snapshot struct {
	Claimed        uint64 // jobs leased by this process
	Succeeded      uint64 // Complete committed
	Failed         uint64 // handler returned an error (retried or dead)
	Retried        uint64 // failures that went back to queued
	Dead           uint64 // failures that ended dead
	LeaseLost      uint64 // Complete/Fail after the lease was reaped or cancelled
	SchedulerTicks uint64 // ticks run while holding the leader lock
}

// RegisterSnapshot exposes the runtime counters as observable counters sampled
// at every export: jobrun_jobs_processed_total{result} (claimed, succeeded,
// failed, retried, dead, lease_lost), jobrun_dead_total,
// jobrun_lease_lost_total and jobrun_scheduler_ticks_total. With OTel disabled
// the global no-op meter makes this free.
func RegisterSnapshot(read func() Snapshot) error {
	m := otel.Meter(MeterName)
	processed, err := m.Int64ObservableCounter("jobrun_jobs_processed_total",
		metric.WithDescription("Worker outcomes by result: claimed, succeeded, failed, retried, dead, lease_lost"))
	if err != nil {
		return fmt.Errorf("jobrun_jobs_processed_total: %w", err)
	}
	dead, err := m.Int64ObservableCounter("jobrun_dead_total",
		metric.WithDescription("Jobs that reached the dead state"))
	if err != nil {
		return fmt.Errorf("jobrun_dead_total: %w", err)
	}
	leaseLost, err := m.Int64ObservableCounter("jobrun_lease_lost_total",
		metric.WithDescription("Completions rejected because the lease had expired or the job was cancelled"))
	if err != nil {
		return fmt.Errorf("jobrun_lease_lost_total: %w", err)
	}
	ticks, err := m.Int64ObservableCounter("jobrun_scheduler_ticks_total",
		metric.WithDescription("Scheduler ticks run while holding the leader lock"))
	if err != nil {
		return fmt.Errorf("jobrun_scheduler_ticks_total: %w", err)
	}
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		s := read()
		o.ObserveInt64(processed, clamp(s.Claimed), metric.WithAttributes(resultClaimed))
		o.ObserveInt64(processed, clamp(s.Succeeded), metric.WithAttributes(resultSucceeded))
		o.ObserveInt64(processed, clamp(s.Failed), metric.WithAttributes(resultFailed))
		o.ObserveInt64(processed, clamp(s.Retried), metric.WithAttributes(resultRetried))
		o.ObserveInt64(processed, clamp(s.Dead), metric.WithAttributes(resultDead))
		o.ObserveInt64(processed, clamp(s.LeaseLost), metric.WithAttributes(resultLeaseLost))
		o.ObserveInt64(dead, clamp(s.Dead))
		o.ObserveInt64(leaseLost, clamp(s.LeaseLost))
		o.ObserveInt64(ticks, clamp(s.SchedulerTicks))
		return nil
	}, processed, dead, leaseLost, ticks)
	if err != nil {
		return fmt.Errorf("register snapshot callback: %w", err)
	}
	return nil
}

// RegisterQueueDepth exposes the cached depth table as an observable gauge
// jobrun_queue_depth{queue,status}, sampled at every export. depth is
// normally admin.DepthCache.Depth wrapped to drop the timestamp.
func RegisterQueueDepth(depth func() []job.DepthRow) error {
	m := otel.Meter(MeterName)
	g, err := m.Int64ObservableGauge("jobrun_queue_depth",
		metric.WithDescription("Jobs by queue and status (from the depth cache)"))
	if err != nil {
		return fmt.Errorf("jobrun_queue_depth: %w", err)
	}
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		for _, r := range depth() {
			o.ObserveInt64(g, r.N, metric.WithAttributes(
				attribute.String("queue", r.Queue),
				attribute.String("status", r.Status.String()),
			))
		}
		return nil
	}, g)
	if err != nil {
		return fmt.Errorf("register queue depth callback: %w", err)
	}
	return nil
}

const maxInt64 = uint64(1<<63 - 1)

func clamp(v uint64) int64 { return int64(min(v, maxInt64)) }

var (
	resultClaimed   = attribute.String("result", "claimed")
	resultSucceeded = attribute.String("result", "succeeded")
	resultFailed    = attribute.String("result", "failed")
	resultRetried   = attribute.String("result", "retried")
	resultDead      = attribute.String("result", "dead")
	resultLeaseLost = attribute.String("result", "lease_lost")
)
