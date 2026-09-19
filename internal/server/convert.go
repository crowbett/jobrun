package server

import (
	"encoding/base64"
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	jobrunv1 "github.com/crowbett/jobrun/gen/jobrun/v1"
	"github.com/crowbett/jobrun/internal/job"
)

// statusToProto maps the domain status to the wire enum. The integer values
// are aligned by construction (job.Status doc) but the switch keeps the two
// enums independent.
func statusToProto(s job.Status) jobrunv1.JobStatus {
	switch s {
	case job.StatusQueued:
		return jobrunv1.JobStatus_JOB_STATUS_QUEUED
	case job.StatusLeased:
		return jobrunv1.JobStatus_JOB_STATUS_LEASED
	case job.StatusSucceeded:
		return jobrunv1.JobStatus_JOB_STATUS_SUCCEEDED
	case job.StatusFailed:
		return jobrunv1.JobStatus_JOB_STATUS_FAILED
	case job.StatusDead:
		return jobrunv1.JobStatus_JOB_STATUS_DEAD
	case job.StatusCancelled:
		return jobrunv1.JobStatus_JOB_STATUS_CANCELLED
	case job.StatusUnspecified:
		return jobrunv1.JobStatus_JOB_STATUS_UNSPECIFIED
	default:
		return jobrunv1.JobStatus_JOB_STATUS_UNSPECIFIED
	}
}

// statusFromProto maps the wire enum to the domain. UNSPECIFIED maps to
// StatusUnspecified (callers treat it as "no filter"); values the server does
// not know are an error so a newer client cannot smuggle in a bogus filter.
func statusFromProto(s jobrunv1.JobStatus) (job.Status, error) {
	switch s {
	case jobrunv1.JobStatus_JOB_STATUS_UNSPECIFIED:
		return job.StatusUnspecified, nil
	case jobrunv1.JobStatus_JOB_STATUS_QUEUED:
		return job.StatusQueued, nil
	case jobrunv1.JobStatus_JOB_STATUS_LEASED:
		return job.StatusLeased, nil
	case jobrunv1.JobStatus_JOB_STATUS_SUCCEEDED:
		return job.StatusSucceeded, nil
	case jobrunv1.JobStatus_JOB_STATUS_FAILED:
		return job.StatusFailed, nil
	case jobrunv1.JobStatus_JOB_STATUS_DEAD:
		return job.StatusDead, nil
	case jobrunv1.JobStatus_JOB_STATUS_CANCELLED:
		return job.StatusCancelled, nil
	default:
		return job.StatusUnspecified, fmt.Errorf("%w: unknown status %d", job.ErrInvalidArgument, int32(s))
	}
}

// policyToProto renders a backoff policy; a zero policy is nil on the wire.
func policyToProto(p job.Policy) *jobrunv1.BackoffPolicy {
	if p.IsZero() {
		return nil
	}
	return &jobrunv1.BackoffPolicy{
		Base:   durationpb.New(p.Base),
		Max:    durationpb.New(p.Max),
		Factor: p.Factor,
		Jitter: p.Jitter,
	}
}

// policyFromProto reads a backoff policy; nil is the zero policy (server
// default). Unset base/max/factor stay zero and are filled by Policy.OrDefault.
func policyFromProto(p *jobrunv1.BackoffPolicy) job.Policy {
	if p == nil {
		return job.Policy{}
	}
	out := job.Policy{Factor: p.GetFactor(), Jitter: p.GetJitter()}
	if p.GetBase() != nil {
		out.Base = p.GetBase().AsDuration()
	}
	if p.GetMax() != nil {
		out.Max = p.GetMax().AsDuration()
	}
	return out
}

// priorityFromProto narrows the wire int32 to the domain int16 after a range
// check, so the conversion can never wrap.
func priorityFromProto(p int32) (int16, error) {
	if p < job.MinPriority || p > job.MaxPriority {
		return 0, fmt.Errorf("%w: priority must be in %d..%d", job.ErrInvalidArgument, job.MinPriority, job.MaxPriority)
	}
	return int16(p), nil
}

// tsOrNil renders a time; the zero time is nil on the wire.
func tsOrNil(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// tsPtr renders an optional time.
func tsPtr(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return tsOrNil(*t)
}

func jobToProto(j job.Job) *jobrunv1.Job {
	return &jobrunv1.Job{
		Id:              j.ID.String(),
		Queue:           j.Queue,
		Type:            j.Type,
		Payload:         j.Payload,
		Status:          statusToProto(j.Status),
		Priority:        int32(j.Priority),
		RunAt:           tsOrNil(j.RunAt),
		Attempts:        j.Attempts,
		MaxAttempts:     j.MaxAttempts,
		Backoff:         policyToProto(j.Backoff),
		LeaseOwner:      j.LeaseOwner,
		LeaseUntil:      tsPtr(j.LeaseUntil),
		LastError:       j.LastError,
		IdempotencyKey:  j.IdempotencyKey,
		CreatedAt:       tsOrNil(j.CreatedAt),
		UpdatedAt:       tsOrNil(j.UpdatedAt),
		StartedAt:       tsPtr(j.StartedAt),
		FinishedAt:      tsPtr(j.FinishedAt),
		CancelRequested: j.CancelRequested,
	}
}

func jobsToProto(js []job.Job) []*jobrunv1.Job {
	out := make([]*jobrunv1.Job, 0, len(js))
	for i := range js {
		out = append(out, jobToProto(js[i]))
	}
	return out
}

func eventToProto(e job.Event) *jobrunv1.JobEvent {
	return &jobrunv1.JobEvent{
		JobId:      e.JobID.String(),
		Queue:      e.Queue,
		Type:       e.Type,
		FromStatus: statusToProto(e.From),
		ToStatus:   statusToProto(e.To),
		Attempt:    e.Attempt,
		At:         tsOrNil(e.At),
		Error:      e.Err,
	}
}

func eventsToProto(es []job.Event) []*jobrunv1.JobEvent {
	out := make([]*jobrunv1.JobEvent, 0, len(es))
	for i := range es {
		out = append(out, eventToProto(es[i]))
	}
	return out
}

func depthToProto(r job.DepthRow) *jobrunv1.QueueDepth {
	return &jobrunv1.QueueDepth{
		Queue:       r.Queue,
		Status:      statusToProto(r.Status),
		Count:       r.N,
		OldestRunAt: tsOrNil(r.OldestRunAt),
	}
}

func templateToProto(n job.NewJob) *jobrunv1.JobTemplate {
	return &jobrunv1.JobTemplate{
		Queue:       n.Queue,
		Type:        n.Type,
		Payload:     n.Payload,
		Priority:    int32(n.Priority),
		MaxAttempts: n.MaxAttempts,
		Backoff:     policyToProto(n.Backoff),
	}
}

// templateFromProto reads a schedule template; a nil template is an empty
// NewJob, which job.Schedule.Validate then rejects (queue must not be empty).
func templateFromProto(t *jobrunv1.JobTemplate) (job.NewJob, error) {
	if t == nil {
		return job.NewJob{}, nil
	}
	prio, err := priorityFromProto(t.GetPriority())
	if err != nil {
		return job.NewJob{}, fmt.Errorf("template: %w", err)
	}
	return job.NewJob{
		Queue:       t.GetQueue(),
		Type:        t.GetType(),
		Payload:     t.GetPayload(),
		Priority:    prio,
		MaxAttempts: t.GetMaxAttempts(),
		Backoff:     policyFromProto(t.GetBackoff()),
	}, nil
}

func scheduleToProto(s job.Schedule) *jobrunv1.Schedule {
	return &jobrunv1.Schedule{
		Id:        s.ID.String(),
		Name:      s.Name,
		Cron:      s.Cron,
		Timezone:  s.Timezone,
		Template:  templateToProto(s.Template),
		Enabled:   s.Enabled,
		NextRun:   tsOrNil(s.NextRun),
		LastRun:   tsPtr(s.LastRun),
		CreatedAt: tsOrNil(s.CreatedAt),
		UpdatedAt: tsOrNil(s.UpdatedAt),
	}
}

func schedulesToProto(ss []job.Schedule) []*jobrunv1.Schedule {
	out := make([]*jobrunv1.Schedule, 0, len(ss))
	for i := range ss {
		out = append(out, scheduleToProto(ss[i]))
	}
	return out
}

// parseID validates the id field of Get/Cancel/Retry/Delete requests;
// failures are already gRPC statuses.
func parseID(raw string) (job.ID, error) {
	if raw == "" {
		return uuid.Nil, invalid("id is required")
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, invalid("id must be a uuid")
	}
	return id, nil
}

// Schedule pages are keyed by name; the token is the last name of the previous
// page, base64url so it is opaque like the job cursor.
func encodeNameToken(name string) string {
	if name == "" {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString([]byte(name))
}

func decodeNameToken(token string) (string, error) {
	if token == "" {
		return "", nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", fmt.Errorf("%w: %w", job.ErrInvalidCursor, err)
	}
	if err := job.ValidateName("page_token", string(raw)); err != nil {
		return "", fmt.Errorf("%w: %w", job.ErrInvalidCursor, err)
	}
	return string(raw), nil
}
