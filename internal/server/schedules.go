package server

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"

	jobrunv1 "github.com/crowbett/jobrun/gen/jobrun/v1"
	"github.com/crowbett/jobrun/internal/cronx"
	"github.com/crowbett/jobrun/internal/job"
)

// ScheduleServer implements jobrunv1.ScheduleServiceServer on top of Schedules.
type ScheduleServer struct {
	jobrunv1.UnimplementedScheduleServiceServer

	svc Schedules
	log *slog.Logger
	// now is the clock next_run is computed from at create/update time; the
	// scheduler re-derives it from the database clock on every firing.
	now func() time.Time
}

// NewScheduleServer builds the adapter. A nil logger discards.
func NewScheduleServer(svc Schedules, log *slog.Logger) *ScheduleServer {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &ScheduleServer{svc: svc, log: log, now: time.Now}
}

// Register attaches the service to a gRPC server (or any ServiceRegistrar).
func (s *ScheduleServer) Register(reg grpc.ServiceRegistrar) {
	jobrunv1.RegisterScheduleServiceServer(reg, s)
}

// CreateSchedule registers a new schedule; enabled defaults to true.
func (s *ScheduleServer) CreateSchedule(ctx context.Context, req *jobrunv1.CreateScheduleRequest) (*jobrunv1.CreateScheduleResponse, error) {
	tmpl, err := templateFromProto(req.GetTemplate())
	if err != nil {
		return nil, toStatus(ctx, s.log, "CreateSchedule", err)
	}
	sc := job.Schedule{
		ID:       job.NewID(),
		Name:     req.GetName(),
		Cron:     req.GetCron(),
		Timezone: req.GetTimezone(),
		Template: tmpl,
		Enabled:  true,
	}
	if req.Enabled != nil {
		sc.Enabled = req.GetEnabled()
	}
	if err := s.prepare(&sc); err != nil {
		return nil, toStatus(ctx, s.log, "CreateSchedule", err)
	}
	out, err := s.svc.CreateSchedule(ctx, sc)
	if err != nil {
		return nil, toStatus(ctx, s.log, "CreateSchedule", err)
	}
	return &jobrunv1.CreateScheduleResponse{Schedule: scheduleToProto(out)}, nil
}

// UpdateSchedule changes cron, timezone, template or enabled; unset fields keep
// their value. next_run is recomputed from now.
func (s *ScheduleServer) UpdateSchedule(ctx context.Context, req *jobrunv1.UpdateScheduleRequest) (*jobrunv1.UpdateScheduleResponse, error) {
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	sc, err := s.svc.GetSchedule(ctx, id)
	if err != nil {
		return nil, toStatus(ctx, s.log, "UpdateSchedule", err)
	}
	if req.Cron != nil {
		sc.Cron = req.GetCron()
	}
	if req.Timezone != nil {
		sc.Timezone = req.GetTimezone()
	}
	if req.GetTemplate() != nil {
		tmpl, err := templateFromProto(req.GetTemplate())
		if err != nil {
			return nil, toStatus(ctx, s.log, "UpdateSchedule", err)
		}
		sc.Template = tmpl
	}
	if req.Enabled != nil {
		sc.Enabled = req.GetEnabled()
	}
	if err := s.prepare(&sc); err != nil {
		return nil, toStatus(ctx, s.log, "UpdateSchedule", err)
	}
	out, err := s.svc.UpdateSchedule(ctx, sc)
	if err != nil {
		return nil, toStatus(ctx, s.log, "UpdateSchedule", err)
	}
	return &jobrunv1.UpdateScheduleResponse{Schedule: scheduleToProto(out)}, nil
}

// prepare validates sc (names, timezone, template, cron via cronx) and sets
// NextRun to the first firing after now. Errors wrap job.ErrInvalidArgument.
func (s *ScheduleServer) prepare(sc *job.Schedule) error {
	if err := sc.Validate(); err != nil {
		return err
	}
	spec, err := cronx.Parse(sc.Cron)
	if err != nil {
		return fmt.Errorf("%w: cron: %w", job.ErrInvalidArgument, err)
	}
	loc := time.UTC
	if sc.Timezone != "" {
		loc, err = time.LoadLocation(sc.Timezone) // already validated
		if err != nil {
			return fmt.Errorf("%w: timezone: %w", job.ErrInvalidArgument, err)
		}
	}
	next := spec.Next(s.now(), loc)
	if next.IsZero() {
		return fmt.Errorf("%w: cron %q never fires in the next %d years", job.ErrInvalidArgument, sc.Cron, cronx.MaxLookahead)
	}
	sc.NextRun = next
	return nil
}

// DeleteSchedule removes a schedule; materialised jobs are untouched.
func (s *ScheduleServer) DeleteSchedule(ctx context.Context, req *jobrunv1.DeleteScheduleRequest) (*jobrunv1.DeleteScheduleResponse, error) {
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	if err := s.svc.DeleteSchedule(ctx, id); err != nil {
		return nil, toStatus(ctx, s.log, "DeleteSchedule", err)
	}
	return &jobrunv1.DeleteScheduleResponse{}, nil
}

// GetSchedule returns one schedule by id.
func (s *ScheduleServer) GetSchedule(ctx context.Context, req *jobrunv1.GetScheduleRequest) (*jobrunv1.GetScheduleResponse, error) {
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	sc, err := s.svc.GetSchedule(ctx, id)
	if err != nil {
		return nil, toStatus(ctx, s.log, "GetSchedule", err)
	}
	return &jobrunv1.GetScheduleResponse{Schedule: scheduleToProto(sc)}, nil
}

// ListSchedules pages by name ascending; the token is the last name seen.
func (s *ScheduleServer) ListSchedules(ctx context.Context, req *jobrunv1.ListSchedulesRequest) (*jobrunv1.ListSchedulesResponse, error) {
	limit := req.GetPageSize()
	switch {
	case limit < 0 || limit > job.MaxListLimit:
		return nil, invalid(fmt.Sprintf("page_size must be in 1..%d", job.MaxListLimit))
	case limit == 0:
		limit = job.DefaultListLimit
	}
	after, err := decodeNameToken(req.GetPageToken())
	if err != nil {
		return nil, toStatus(ctx, s.log, "ListSchedules", err)
	}
	// Ask for one extra row so next_page_token is exact, not a guess.
	rows, err := s.svc.ListSchedules(ctx, after, limit+1)
	if err != nil {
		return nil, toStatus(ctx, s.log, "ListSchedules", err)
	}
	resp := &jobrunv1.ListSchedulesResponse{}
	if int32(len(rows)) > limit { //nolint:gosec // len is bounded by limit+1 <= 201
		rows = rows[:limit]
		resp.NextPageToken = encodeNameToken(rows[len(rows)-1].Name)
	}
	resp.Schedules = schedulesToProto(rows)
	return resp, nil
}
