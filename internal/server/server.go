// Package server is the gRPC adapter for jobrun.v1.JobService and
// jobrun.v1.ScheduleService: it validates request shape, converts proto <->
// domain and maps domain errors to status codes. Business rules live in
// internal/job and the store.
//
// The ports below are consumer-defined and satisfied by job.Store; tests pass
// fakes.
package server

import (
	"context"
	"time"

	"github.com/crowbett/jobrun/internal/job"
)

// Jobs is the slice of job.Store that JobServer drives.
type Jobs interface {
	Enqueue(ctx context.Context, j job.Job) (job.Job, error)
	Get(ctx context.Context, id job.ID) (job.Job, error)
	Events(ctx context.Context, id job.ID) ([]job.Event, error)
	List(ctx context.Context, q job.ListQuery) ([]job.Job, job.Cursor, error)
	Cancel(ctx context.Context, id job.ID) (job.Job, error)
	Retry(ctx context.Context, id job.ID) (job.Job, error)
	Purge(ctx context.Context, q job.PurgeQuery) (int64, error)
	Depth(ctx context.Context) ([]job.DepthRow, error)
}

// Schedules is the slice of job.Schedules that ScheduleServer drives.
type Schedules interface {
	CreateSchedule(ctx context.Context, s job.Schedule) (job.Schedule, error)
	UpdateSchedule(ctx context.Context, s job.Schedule) (job.Schedule, error)
	DeleteSchedule(ctx context.Context, id job.ID) error
	GetSchedule(ctx context.Context, id job.ID) (job.Schedule, error)
	ListSchedules(ctx context.Context, afterName string, limit int32) ([]job.Schedule, error)
}

// DepthSource is a cached depth table (admin.DepthCache satisfies it). When one
// is installed with WithDepthCache, Stats serves it instead of calling
// Jobs.Depth per request, as docs/PLAN.md requires.
type DepthSource interface {
	Depth() ([]job.DepthRow, time.Time)
}
