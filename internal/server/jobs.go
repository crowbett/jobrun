package server

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	jobrunv1 "github.com/crowbett/jobrun/gen/jobrun/v1"
	"github.com/crowbett/jobrun/internal/job"
)

// JobServer implements jobrunv1.JobServiceServer on top of Jobs.
type JobServer struct {
	jobrunv1.UnimplementedJobServiceServer

	jobs  Jobs
	depth DepthSource // nil: Stats calls jobs.Depth directly
	log   *slog.Logger
	now   func() time.Time
}

// JobOption configures NewJobServer.
type JobOption func(*JobServer)

// WithDepthCache makes Stats read the cached depth table instead of hitting
// the store on every call. Pass the admin.DepthCache.
func WithDepthCache(d DepthSource) JobOption {
	return func(s *JobServer) { s.depth = d }
}

// NewJobServer builds the adapter. A nil logger discards.
func NewJobServer(jobs Jobs, log *slog.Logger, opts ...JobOption) *JobServer {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	s := &JobServer{jobs: jobs, log: log, now: time.Now}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Register attaches the service to a gRPC server (or any ServiceRegistrar).
func (s *JobServer) Register(reg grpc.ServiceRegistrar) {
	jobrunv1.RegisterJobServiceServer(reg, s)
}

// Enqueue inserts one job. A replay of (queue, idempotency_key) returns the
// stored job with created=false.
func (s *JobServer) Enqueue(ctx context.Context, req *jobrunv1.EnqueueRequest) (*jobrunv1.EnqueueResponse, error) {
	n, err := newJobFromProto(req)
	if err != nil {
		return nil, toStatus(ctx, s.log, "Enqueue", err)
	}
	j, err := job.New(n) // validates and applies defaults
	if err != nil {
		return nil, toStatus(ctx, s.log, "Enqueue", err)
	}
	stored, err := s.jobs.Enqueue(ctx, j)
	if err != nil {
		return nil, toStatus(ctx, s.log, "Enqueue", err)
	}
	return &jobrunv1.EnqueueResponse{Job: jobToProto(stored), Created: stored.ID == j.ID}, nil
}

func newJobFromProto(req *jobrunv1.EnqueueRequest) (job.NewJob, error) {
	prio, err := priorityFromProto(req.GetPriority())
	if err != nil {
		return job.NewJob{}, err
	}
	n := job.NewJob{
		Queue:          req.GetQueue(),
		Type:           req.GetType(),
		Payload:        req.GetPayload(),
		Priority:       prio,
		MaxAttempts:    req.GetMaxAttempts(),
		Backoff:        policyFromProto(req.GetBackoff()),
		IdempotencyKey: req.GetIdempotencyKey(),
	}
	// Exactly one of the oneof is set; the store treats zero RunAt as "db now".
	if ts := req.GetRunAt(); ts != nil {
		n.RunAt = ts.AsTime()
	}
	if d := req.GetDelay(); d != nil {
		n.Delay = d.AsDuration()
	}
	return n, nil
}

// Get returns one job, with its history when include_events is set.
func (s *JobServer) Get(ctx context.Context, req *jobrunv1.GetRequest) (*jobrunv1.GetResponse, error) {
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	j, err := s.jobs.Get(ctx, id)
	if err != nil {
		return nil, toStatus(ctx, s.log, "Get", err)
	}
	resp := &jobrunv1.GetResponse{Job: jobToProto(j)}
	if req.GetIncludeEvents() {
		events, err := s.jobs.Events(ctx, id)
		if err != nil {
			return nil, toStatus(ctx, s.log, "Get", err)
		}
		resp.Events = eventsToProto(events)
	}
	return resp, nil
}

// List pages jobs newest first with an opaque keyset page_token.
func (s *JobServer) List(ctx context.Context, req *jobrunv1.ListRequest) (*jobrunv1.ListResponse, error) {
	if req.GetPageSize() < 0 {
		return nil, invalid("page_size must not be negative")
	}
	cursor, err := job.DecodeCursor(req.GetPageToken())
	if err != nil {
		return nil, toStatus(ctx, s.log, "List", err)
	}
	st, err := statusFromProto(req.GetStatus())
	if err != nil {
		return nil, toStatus(ctx, s.log, "List", err)
	}
	q := job.ListQuery{Queue: req.GetQueue(), Type: req.GetType(), Cursor: cursor, Limit: req.GetPageSize()}
	if st != job.StatusUnspecified {
		q.Status = &st
	}
	if err := q.Validate(); err != nil {
		return nil, toStatus(ctx, s.log, "List", err)
	}
	jobs, next, err := s.jobs.List(ctx, q)
	if err != nil {
		return nil, toStatus(ctx, s.log, "List", err)
	}
	return &jobrunv1.ListResponse{Jobs: jobsToProto(jobs), NextPageToken: next.Encode()}, nil
}

// Cancel stops a queued job at once or requests a cooperative cancel of a
// leased one.
func (s *JobServer) Cancel(ctx context.Context, req *jobrunv1.CancelRequest) (*jobrunv1.CancelResponse, error) {
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	j, err := s.jobs.Cancel(ctx, id)
	if err != nil {
		return nil, toStatus(ctx, s.log, "Cancel", err)
	}
	return &jobrunv1.CancelResponse{Job: jobToProto(j)}, nil
}

// Retry moves a dead or cancelled job back to queued with attempts reset.
func (s *JobServer) Retry(ctx context.Context, req *jobrunv1.RetryRequest) (*jobrunv1.RetryResponse, error) {
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	j, err := s.jobs.Retry(ctx, id)
	if err != nil {
		return nil, toStatus(ctx, s.log, "Retry", err)
	}
	return &jobrunv1.RetryResponse{Job: jobToProto(j)}, nil
}

// Purge deletes finished jobs by queue, status and age.
func (s *JobServer) Purge(ctx context.Context, req *jobrunv1.PurgeRequest) (*jobrunv1.PurgeResponse, error) {
	q := job.PurgeQuery{Queue: req.GetQueue(), Limit: req.GetLimit()}
	if d := req.GetOlderThan(); d != nil {
		q.OlderThan = d.AsDuration()
	}
	for _, ps := range req.GetStatuses() {
		st, err := statusFromProto(ps)
		if err != nil {
			return nil, toStatus(ctx, s.log, "Purge", err)
		}
		if st == job.StatusUnspecified {
			return nil, invalid("statuses must not contain UNSPECIFIED")
		}
		q.Statuses = append(q.Statuses, st)
	}
	if err := q.Validate(); err != nil {
		return nil, toStatus(ctx, s.log, "Purge", err)
	}
	n, err := s.jobs.Purge(ctx, q)
	if err != nil {
		return nil, toStatus(ctx, s.log, "Purge", err)
	}
	return &jobrunv1.PurgeResponse{Deleted: n}, nil
}

// Stats returns the queue depth by queue and status, from the depth cache when
// one is installed.
func (s *JobServer) Stats(ctx context.Context, req *jobrunv1.StatsRequest) (*jobrunv1.StatsResponse, error) {
	if q := req.GetQueue(); q != "" {
		if err := job.ValidateName("queue", q); err != nil {
			return nil, toStatus(ctx, s.log, "Stats", err)
		}
	}
	rows, asOf, err := s.depthRows(ctx)
	if err != nil {
		return nil, toStatus(ctx, s.log, "Stats", err)
	}
	resp := &jobrunv1.StatsResponse{Depths: make([]*jobrunv1.QueueDepth, 0, len(rows)), AsOf: timestamppb.New(asOf)}
	for _, r := range rows {
		if req.GetQueue() != "" && r.Queue != req.GetQueue() {
			continue
		}
		resp.Depths = append(resp.Depths, depthToProto(r))
	}
	return resp, nil
}

func (s *JobServer) depthRows(ctx context.Context) ([]job.DepthRow, time.Time, error) {
	if s.depth != nil {
		rows, asOf := s.depth.Depth()
		return rows, asOf, nil
	}
	rows, err := s.jobs.Depth(ctx)
	if err != nil {
		return nil, time.Time{}, err
	}
	return rows, s.now(), nil
}
