package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/crowbett/jobrun/internal/job"
	"github.com/crowbett/jobrun/internal/postgres/sqlc"
)

// EventMode selects which transitions are written to job_events (JOB_EVENTS).
// The rows are always written in the same transaction as the jobs change, so
// history and state can never disagree; the mode only trades write
// amplification (about four rows per job in EventsAll) for a thinner history.
type EventMode int

const (
	// EventsAll records every transition, the enqueue event included.
	EventsAll EventMode = iota
	// EventsTerminal records only transitions into a terminal status
	// (succeeded, dead, cancelled): enqueue, claim and retry rows are skipped.
	EventsTerminal
)

// ParseEventMode reads the JOB_EVENTS spelling ("all" or "terminal", any case).
func ParseEventMode(s string) (EventMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "all":
		return EventsAll, nil
	case "terminal":
		return EventsTerminal, nil
	default:
		return EventsAll, fmt.Errorf("%w: JOB_EVENTS must be all or terminal, got %q", job.ErrInvalidArgument, s)
	}
}

// records reports whether the mode writes a transition into to.
func (m EventMode) records(to job.Status) bool {
	return m == EventsAll || to.Terminal()
}

// eventParams builds the insert row for ev. A StatusUnspecified From is stored
// as NULL (the "" spelling is mapped by NULLIF in the statement); a zero At
// means the database clock.
func eventParams(ev job.Event) sqlc.InsertJobEventParams {
	p := sqlc.InsertJobEventParams{
		JobID:    ev.JobID,
		Queue:    ev.Queue,
		Type:     ev.Type,
		ToStatus: ev.To.String(),
		Attempt:  ev.Attempt,
		Error:    nullStr(truncateErr(ev.Err)),
	}
	if ev.From != job.StatusUnspecified {
		p.FromStatus = ev.From.String()
	}
	if !ev.At.IsZero() {
		at := ev.At
		p.At = &at
	}
	return p
}

// recordEvent appends one transition inside the caller's transaction, subject
// to the event mode.
func (s *Store) recordEvent(ctx context.Context, q *sqlc.Queries, ev job.Event) error {
	if !s.events.records(ev.To) {
		return nil
	}
	if err := q.InsertJobEvent(ctx, eventParams(ev)); err != nil {
		return fmt.Errorf("insert job event %s -> %s: %w", ev.From, ev.To, err)
	}
	return nil
}

// recordEvents appends many transitions as one pgx batch inside the caller's
// transaction, subject to the event mode.
func (s *Store) recordEvents(ctx context.Context, q *sqlc.Queries, evs []job.Event) error {
	rows := make([]sqlc.InsertJobEventsBatchParams, 0, len(evs))
	for _, ev := range evs {
		if !s.events.records(ev.To) {
			continue
		}
		rows = append(rows, sqlc.InsertJobEventsBatchParams(eventParams(ev)))
	}
	if len(rows) == 0 {
		return nil
	}
	var first error
	q.InsertJobEventsBatch(ctx, rows).Exec(func(i int, err error) {
		if err != nil && first == nil {
			first = fmt.Errorf("insert job event %d/%d: %w", i+1, len(rows), err)
		}
	})
	return first
}

// Events returns a job's history oldest first (empty, not nil, for an unknown id).
func (s *Store) Events(ctx context.Context, id job.ID) ([]job.Event, error) {
	rows, err := s.q.ListJobEvents(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("list job events: %w", err)
	}
	out := make([]job.Event, 0, len(rows))
	for i := range rows {
		ev, err := toEvent(&rows[i])
		if err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, nil
}

// PruneEvents deletes job_events rows older than olderThan on the database
// clock (the reaper's retention step, paired with Purge) and returns the count.
func (s *Store) PruneEvents(ctx context.Context, olderThan time.Duration) (int64, error) {
	n, err := s.q.PruneJobEvents(ctx, interval(olderThan))
	if err != nil {
		return 0, fmt.Errorf("prune job events: %w", err)
	}
	return n, nil
}

// truncateErr bounds an error string to job.MaxErrorLen bytes.
func truncateErr(s string) string {
	if len(s) <= job.MaxErrorLen {
		return s
	}
	return s[:job.MaxErrorLen]
}
