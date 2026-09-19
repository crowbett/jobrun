package postgres

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/crowbett/jobrun/internal/job"
	"github.com/crowbett/jobrun/internal/postgres/sqlc"
)

// pgUniqueViolation is the SQLSTATE for unique_violation.
const pgUniqueViolation = "23505"

// isUniqueViolation reports whether err is a unique_violation (23505) on the
// named constraint. An empty constraint matches any unique violation.
func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == pgUniqueViolation && (constraint == "" || pgErr.ConstraintName == constraint)
}

// mapNotFound turns pgx.ErrNoRows into sentinel and wraps anything else with op.
func mapNotFound(err, sentinel error, op string) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return sentinel
	}
	return fmt.Errorf("%s: %w", op, err)
}

// interval converts a duration to the interval parameter type (microsecond
// resolution, which is also what Postgres stores).
func interval(d time.Duration) pgtype.Interval {
	return pgtype.Interval{Microseconds: d.Microseconds(), Valid: true}
}

// nullStr maps "" to NULL.
func nullStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// utcPtr normalises an optional timestamp to UTC.
func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

// statusFromDB maps a job_status enum value back onto the domain.
func statusFromDB(s sqlc.JobStatus) (job.Status, error) {
	st, err := job.ParseStatus(string(s))
	if err != nil {
		return job.StatusUnspecified, fmt.Errorf("stored job status: %w", err)
	}
	return st, nil
}

// policyFromDB decodes the backoff jsonb; "{}" yields the default policy.
func policyFromDB(raw []byte) (job.Policy, error) {
	var p job.Policy
	if len(raw) == 0 {
		return job.DefaultPolicy, nil
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return job.Policy{}, fmt.Errorf("stored backoff policy: %w", err)
	}
	return p.OrDefault(), nil
}

// policyToDB encodes a policy for the backoff jsonb column.
func policyToDB(p job.Policy) ([]byte, error) {
	b, err := json.Marshal(p.OrDefault())
	if err != nil {
		return nil, fmt.Errorf("encode backoff policy: %w", err)
	}
	return b, nil
}

// payloadToDB maps an empty payload to the {} document.
func payloadToDB(p []byte) []byte {
	if len(p) == 0 {
		return []byte("{}")
	}
	return p
}

// toJob converts a row; timestamps are normalised to UTC.
func toJob(row *sqlc.Job) (job.Job, error) {
	st, err := statusFromDB(row.Status)
	if err != nil {
		return job.Job{}, err
	}
	pol, err := policyFromDB(row.Backoff)
	if err != nil {
		return job.Job{}, err
	}
	return job.Job{
		ID:              row.ID,
		Queue:           row.Queue,
		Type:            row.Type,
		Payload:         row.Payload,
		Status:          st,
		Priority:        row.Priority,
		RunAt:           row.RunAt.UTC(),
		Attempts:        row.Attempts,
		MaxAttempts:     row.MaxAttempts,
		Backoff:         pol,
		LeaseOwner:      deref(row.LeaseOwner),
		LeaseUntil:      utcPtr(row.LeaseUntil),
		LastError:       deref(row.LastError),
		IdempotencyKey:  deref(row.IdempotencyKey),
		CancelRequested: row.CancelRequested,
		CreatedAt:       row.CreatedAt.UTC(),
		UpdatedAt:       row.UpdatedAt.UTC(),
		StartedAt:       utcPtr(row.StartedAt),
		FinishedAt:      utcPtr(row.FinishedAt),
	}, nil
}

func toJobs(rows []sqlc.Job) ([]job.Job, error) {
	out := make([]job.Job, 0, len(rows))
	for i := range rows {
		j, err := toJob(&rows[i])
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, nil
}

// toEvent converts a job_events row.
func toEvent(row *sqlc.JobEvent) (job.Event, error) {
	to, err := statusFromDB(row.ToStatus)
	if err != nil {
		return job.Event{}, err
	}
	from := job.StatusUnspecified
	if row.FromStatus != nil {
		if from, err = statusFromDB(*row.FromStatus); err != nil {
			return job.Event{}, err
		}
	}
	return job.Event{
		JobID:   row.JobID,
		Queue:   row.Queue,
		Type:    row.Type,
		From:    from,
		To:      to,
		Attempt: row.Attempt,
		At:      row.At.UTC(),
		Err:     deref(row.Error),
	}, nil
}

// toSchedule converts a schedules row into the domain shape; the template
// carries queue/type/payload/priority/max_attempts/backoff.
func toSchedule(row *sqlc.Schedule) (job.Schedule, error) {
	pol, err := policyFromDB(row.Backoff)
	if err != nil {
		return job.Schedule{}, err
	}
	return job.Schedule{
		ID:       row.ID,
		Name:     row.Name,
		Cron:     row.Cron,
		Timezone: row.Timezone,
		Template: job.NewJob{
			Queue:       row.Queue,
			Type:        row.JobType,
			Payload:     row.Payload,
			Priority:    row.Priority,
			MaxAttempts: row.MaxAttempts,
			Backoff:     pol,
		},
		Enabled:   row.Enabled,
		NextRun:   row.NextRun.UTC(),
		LastRun:   utcPtr(row.LastRun),
		CreatedAt: row.CreatedAt.UTC(),
		UpdatedAt: row.UpdatedAt.UTC(),
	}, nil
}

func toSchedules(rows []sqlc.Schedule) ([]job.Schedule, error) {
	out := make([]job.Schedule, 0, len(rows))
	for i := range rows {
		s, err := toSchedule(&rows[i])
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}
