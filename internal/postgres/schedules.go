package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/crowbett/jobrun/internal/job"
	"github.com/crowbett/jobrun/internal/postgres/sqlc"
)

// CreateSchedule inserts s (NextRun must be set by the caller from cronx). A
// zero ID gets a fresh one, "" timezone means UTC; a taken name is
// job.ErrScheduleExists.
func (s *Store) CreateSchedule(ctx context.Context, sc job.Schedule) (job.Schedule, error) {
	backoff, err := policyToDB(sc.Template.Backoff)
	if err != nil {
		return job.Schedule{}, err
	}
	id := sc.ID
	if id == uuid.Nil {
		id = job.NewID()
	}
	tz := sc.Timezone
	if tz == "" {
		tz = "UTC"
	}
	maxAttempts := sc.Template.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = job.DefaultMaxAttempts
	}
	row, err := s.q.InsertSchedule(ctx, sqlc.InsertScheduleParams{
		ID: id, Name: sc.Name, Cron: sc.Cron, Timezone: tz,
		Queue: sc.Template.Queue, JobType: sc.Template.Type, Payload: payloadToDB(sc.Template.Payload),
		Priority: sc.Template.Priority, MaxAttempts: maxAttempts, Backoff: backoff,
		Enabled: sc.Enabled, NextRun: sc.NextRun,
	})
	if err != nil {
		if isUniqueViolation(err, "") {
			return job.Schedule{}, fmt.Errorf("%w: %q", job.ErrScheduleExists, sc.Name)
		}
		return job.Schedule{}, fmt.Errorf("insert schedule: %w", err)
	}
	return toSchedule(&row)
}

// UpdateSchedule replaces cron, timezone, template, enabled and next_run of the
// schedule with sc.ID (the name is immutable); job.ErrScheduleNotFound when missing.
func (s *Store) UpdateSchedule(ctx context.Context, sc job.Schedule) (job.Schedule, error) {
	backoff, err := policyToDB(sc.Template.Backoff)
	if err != nil {
		return job.Schedule{}, err
	}
	tz := sc.Timezone
	if tz == "" {
		tz = "UTC"
	}
	maxAttempts := sc.Template.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = job.DefaultMaxAttempts
	}
	row, err := s.q.UpdateSchedule(ctx, sqlc.UpdateScheduleParams{
		ID: sc.ID, Cron: sc.Cron, Timezone: tz,
		Queue: sc.Template.Queue, JobType: sc.Template.Type, Payload: payloadToDB(sc.Template.Payload),
		Priority: sc.Template.Priority, MaxAttempts: maxAttempts, Backoff: backoff,
		Enabled: sc.Enabled, NextRun: sc.NextRun,
	})
	if err != nil {
		return job.Schedule{}, mapNotFound(err, job.ErrScheduleNotFound, "update schedule")
	}
	return toSchedule(&row)
}

// DeleteSchedule removes a schedule; job.ErrScheduleNotFound when missing.
func (s *Store) DeleteSchedule(ctx context.Context, id job.ID) error {
	n, err := s.q.DeleteSchedule(ctx, id)
	if err != nil {
		return fmt.Errorf("delete schedule: %w", err)
	}
	if n == 0 {
		return job.ErrScheduleNotFound
	}
	return nil
}

// GetSchedule returns one schedule or job.ErrScheduleNotFound.
func (s *Store) GetSchedule(ctx context.Context, id job.ID) (job.Schedule, error) {
	row, err := s.q.GetSchedule(ctx, id)
	if err != nil {
		return job.Schedule{}, mapNotFound(err, job.ErrScheduleNotFound, "get schedule")
	}
	return toSchedule(&row)
}

// ListSchedules pages by name ascending: rows with name > afterName, up to
// limit (0 means job.DefaultListLimit, clamped at job.MaxListLimit).
func (s *Store) ListSchedules(ctx context.Context, afterName string, limit int32) ([]job.Schedule, error) {
	switch {
	case limit <= 0:
		limit = job.DefaultListLimit
	case limit > job.MaxListLimit:
		limit = job.MaxListLimit
	}
	rows, err := s.q.ListSchedules(ctx, sqlc.ListSchedulesParams{AfterName: afterName, PageLimit: limit})
	if err != nil {
		return nil, fmt.Errorf("list schedules: %w", err)
	}
	return toSchedules(rows)
}

// DueSchedules returns up to limit enabled schedules with next_run <= now,
// oldest next_run first. It runs SELECT ... FOR UPDATE SKIP LOCKED in its own
// short transaction, so two leaders ticking at once split the due set instead
// of both materialising it; the idempotency key on the materialised job is the
// durable guard.
func (s *Store) DueSchedules(ctx context.Context, now time.Time, limit int32) ([]job.Schedule, error) {
	if limit <= 0 {
		limit = job.DefaultListLimit
	}
	var out []job.Schedule
	err := s.withTx(ctx, func(q *sqlc.Queries, _ pgx.Tx) error {
		rows, err := q.DueSchedules(ctx, sqlc.DueSchedulesParams{Now: now, BatchLimit: limit})
		if err != nil {
			return fmt.Errorf("due schedules: %w", err)
		}
		out, err = toSchedules(rows)
		return err
	})
	return out, err
}

// AdvanceSchedule sets last_run and next_run after a materialisation;
// job.ErrScheduleNotFound when missing.
func (s *Store) AdvanceSchedule(ctx context.Context, id job.ID, lastRun, nextRun time.Time) error {
	n, err := s.q.AdvanceSchedule(ctx, sqlc.AdvanceScheduleParams{ID: id, LastRun: lastRun, NextRun: nextRun})
	if err != nil {
		return fmt.Errorf("advance schedule: %w", err)
	}
	if n == 0 {
		return job.ErrScheduleNotFound
	}
	return nil
}
