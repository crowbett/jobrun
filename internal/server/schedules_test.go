package server_test

import (
	"context"
	"encoding/base64"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"

	jobrunv1 "github.com/crowbett/jobrun/gen/jobrun/v1"
	"github.com/crowbett/jobrun/internal/job"
)

func validCreate() *jobrunv1.CreateScheduleRequest {
	return &jobrunv1.CreateScheduleRequest{
		Name:     "nightly",
		Cron:     "0 3 * * *",
		Timezone: "America/New_York",
		Template: &jobrunv1.JobTemplate{Queue: "default", Type: "prune", MaxAttempts: 3, Priority: -5},
	}
}

func TestCreateSchedule(t *testing.T) {
	t.Parallel()

	for _, tt := range scheduleErrCases() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var gotIn job.Schedule
			fake := &fakeStore{createSchedule: func(_ context.Context, s job.Schedule) (job.Schedule, error) {
				gotIn = s
				if tt.svcErr != nil {
					return job.Schedule{}, tt.svcErr
				}
				s.CreatedAt, s.UpdatedAt = fixedNow, fixedNow
				return s, nil
			}}
			c := newTestClients(t, fake)

			resp, err := c.schedules.CreateSchedule(ctxWithRequestID(t, "req-create-"+tt.name), validCreate())
			checkStatus(t, err, tt.wantCode, c.logs, "req-create-"+tt.name)
			if tt.wantCode != codes.OK {
				return
			}
			if gotIn.ID == uuid.Nil || gotIn.Name != "nightly" || gotIn.Cron != "0 3 * * *" || gotIn.Timezone != "America/New_York" ||
				!gotIn.Enabled || gotIn.Template.Queue != "default" || gotIn.Template.Type != "prune" ||
				gotIn.Template.MaxAttempts != 3 || gotIn.Template.Priority != -5 {
				t.Errorf("store received %+v", gotIn)
			}
			// next_run is the next 03:00 New York time after now, always in the future.
			ny, _ := time.LoadLocation("America/New_York")
			if gotIn.NextRun.IsZero() || !gotIn.NextRun.After(time.Now().Add(-time.Minute)) {
				t.Errorf("next_run = %v, want a future time", gotIn.NextRun)
			}
			if h, m := gotIn.NextRun.In(ny).Hour(), gotIn.NextRun.In(ny).Minute(); h != 3 || m != 0 {
				t.Errorf("next_run = %v, want 03:00 New York", gotIn.NextRun.In(ny))
			}
			checkSchedule(t, resp.GetSchedule(), gotIn)
		})
	}
}

func TestCreateScheduleValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		mut  func(*jobrunv1.CreateScheduleRequest)
	}{
		{"empty name", func(r *jobrunv1.CreateScheduleRequest) { r.Name = "" }},
		{"bad name", func(r *jobrunv1.CreateScheduleRequest) { r.Name = "Night Ly" }},
		{"empty cron", func(r *jobrunv1.CreateScheduleRequest) { r.Cron = "" }},
		{"bad cron", func(r *jobrunv1.CreateScheduleRequest) { r.Cron = "61 * * * *" }},
		{"four fields", func(r *jobrunv1.CreateScheduleRequest) { r.Cron = "* * * *" }},
		{"bad timezone", func(r *jobrunv1.CreateScheduleRequest) { r.Timezone = "Mars/Olympus" }},
		{"no template", func(r *jobrunv1.CreateScheduleRequest) { r.Template = nil }},
		{"template bad queue", func(r *jobrunv1.CreateScheduleRequest) { r.Template.Queue = "" }},
		{"template priority overflow", func(r *jobrunv1.CreateScheduleRequest) { r.Template.Priority = 40000 }},
		{"template bad payload", func(r *jobrunv1.CreateScheduleRequest) { r.Template.Payload = []byte("x") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fake := &fakeStore{}
			c := newTestClients(t, fake)
			req := validCreate()
			tt.mut(req)
			_, err := c.schedules.CreateSchedule(t.Context(), req)
			checkStatus(t, err, codes.InvalidArgument, c.logs, "")
			if fake.callCount() != 0 {
				t.Errorf("store called %d times for invalid input", fake.callCount())
			}
		})
	}
}

func TestCreateScheduleDisabled(t *testing.T) {
	t.Parallel()
	fake := &fakeStore{createSchedule: func(_ context.Context, s job.Schedule) (job.Schedule, error) {
		if s.Enabled {
			t.Error("enabled = true, want false")
		}
		if s.Timezone != "" {
			t.Errorf("timezone = %q, want empty (UTC)", s.Timezone)
		}
		return s, nil
	}}
	c := newTestClients(t, fake)
	req := validCreate()
	req.Timezone = ""
	req.Enabled = ptr(false)
	if _, err := c.schedules.CreateSchedule(t.Context(), req); err != nil {
		t.Fatal(err)
	}
}

func TestUpdateSchedule(t *testing.T) {
	t.Parallel()

	type tc struct {
		errCase
		req       *jobrunv1.UpdateScheduleRequest
		wantCalls int
	}
	tests := []tc{
		{errCase: errCase{name: "empty id", wantCode: codes.InvalidArgument}, req: &jobrunv1.UpdateScheduleRequest{}},
		{errCase: errCase{name: "bad uuid", wantCode: codes.InvalidArgument}, req: &jobrunv1.UpdateScheduleRequest{Id: "nope"}},
		{errCase: errCase{name: "bad cron", wantCode: codes.InvalidArgument}, req: &jobrunv1.UpdateScheduleRequest{Id: scheduleID.String(), Cron: ptr("x")}, wantCalls: 1},
		{errCase: errCase{name: "bad timezone", wantCode: codes.InvalidArgument}, req: &jobrunv1.UpdateScheduleRequest{Id: scheduleID.String(), Timezone: ptr("Nope/Nope")}, wantCalls: 1},
		{errCase: errCase{name: "get not found", svcErr: job.ErrScheduleNotFound, wantCode: codes.NotFound}, req: &jobrunv1.UpdateScheduleRequest{Id: scheduleID.String()}, wantCalls: 1},
		{errCase: errCase{name: "get internal", svcErr: errBoom, wantCode: codes.Internal}, req: &jobrunv1.UpdateScheduleRequest{Id: scheduleID.String()}, wantCalls: 1},
		{
			errCase: errCase{name: "ok", wantCode: codes.OK}, wantCalls: 2,
			req: &jobrunv1.UpdateScheduleRequest{
				Id: scheduleID.String(), Cron: ptr("*/5 * * * *"), Enabled: ptr(false),
				Template: &jobrunv1.JobTemplate{Queue: "other", Type: "noop"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fake := &fakeStore{
				getSchedule: func(context.Context, job.ID) (job.Schedule, error) {
					if tt.svcErr != nil {
						return job.Schedule{}, tt.svcErr
					}
					return sampleSchedule(), nil
				},
				updateSchedule: func(_ context.Context, s job.Schedule) (job.Schedule, error) {
					if s.ID != scheduleID || s.Cron != "*/5 * * * *" || s.Enabled || s.Timezone != "UTC" ||
						s.Template.Queue != "other" || s.Template.Type != "noop" || s.Name != "nightly" {
						t.Errorf("store got %+v", s)
					}
					if s.NextRun.Minute()%5 != 0 || !s.NextRun.After(time.Now().Add(-time.Minute)) {
						t.Errorf("next_run = %v, want the next 5-minute boundary", s.NextRun)
					}
					return s, nil
				},
			}
			c := newTestClients(t, fake)
			resp, err := c.schedules.UpdateSchedule(ctxWithRequestID(t, "req-update-"+tt.name), tt.req)
			checkStatus(t, err, tt.wantCode, c.logs, "req-update-"+tt.name)
			if fake.callCount() != tt.wantCalls {
				t.Errorf("store called %d times, want %d", fake.callCount(), tt.wantCalls)
			}
			if tt.wantCode == codes.OK && (resp.GetSchedule().GetCron() != "*/5 * * * *" || resp.GetSchedule().GetEnabled()) {
				t.Errorf("schedule = %v", resp.GetSchedule())
			}
		})
	}
}

func TestUpdateScheduleStoreErrors(t *testing.T) {
	t.Parallel()
	for _, tt := range scheduleErrCases() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fake := &fakeStore{
				getSchedule: func(context.Context, job.ID) (job.Schedule, error) { return sampleSchedule(), nil },
				updateSchedule: func(_ context.Context, s job.Schedule) (job.Schedule, error) {
					if tt.svcErr != nil {
						return job.Schedule{}, tt.svcErr
					}
					return s, nil
				},
			}
			c := newTestClients(t, fake)
			_, err := c.schedules.UpdateSchedule(ctxWithRequestID(t, "req-upd-"+tt.name), &jobrunv1.UpdateScheduleRequest{Id: scheduleID.String()})
			checkStatus(t, err, tt.wantCode, c.logs, "req-upd-"+tt.name)
		})
	}
}

func TestGetAndDeleteSchedule(t *testing.T) {
	t.Parallel()

	type tc struct {
		errCase
		id        string
		wantCalls int
	}
	domain := scheduleErrCases()
	tests := make([]tc, 0, 2+len(domain))
	tests = append(tests,
		tc{errCase: errCase{name: "empty id", wantCode: codes.InvalidArgument}, id: ""},
		tc{errCase: errCase{name: "bad uuid", wantCode: codes.InvalidArgument}, id: "zzz"},
	)
	for _, c := range domain {
		tests = append(tests, tc{errCase: c, id: scheduleID.String(), wantCalls: 1})
	}
	for _, tt := range tests {
		t.Run("get/"+tt.name, func(t *testing.T) {
			t.Parallel()
			fake := &fakeStore{getSchedule: func(_ context.Context, id job.ID) (job.Schedule, error) {
				if id != scheduleID {
					t.Errorf("store got id %s", id)
				}
				if tt.svcErr != nil {
					return job.Schedule{}, tt.svcErr
				}
				return sampleSchedule(), nil
			}}
			c := newTestClients(t, fake)
			resp, err := c.schedules.GetSchedule(ctxWithRequestID(t, "req-gets-"+tt.name), &jobrunv1.GetScheduleRequest{Id: tt.id})
			checkStatus(t, err, tt.wantCode, c.logs, "req-gets-"+tt.name)
			if fake.callCount() != tt.wantCalls {
				t.Errorf("store called %d times, want %d", fake.callCount(), tt.wantCalls)
			}
			if tt.wantCode == codes.OK {
				checkSchedule(t, resp.GetSchedule(), sampleSchedule())
			}
		})
		t.Run("delete/"+tt.name, func(t *testing.T) {
			t.Parallel()
			fake := &fakeStore{deleteSchedule: func(_ context.Context, id job.ID) error {
				if id != scheduleID {
					t.Errorf("store got id %s", id)
				}
				return tt.svcErr
			}}
			c := newTestClients(t, fake)
			_, err := c.schedules.DeleteSchedule(ctxWithRequestID(t, "req-del-"+tt.name), &jobrunv1.DeleteScheduleRequest{Id: tt.id})
			checkStatus(t, err, tt.wantCode, c.logs, "req-del-"+tt.name)
			if fake.callCount() != tt.wantCalls {
				t.Errorf("store called %d times, want %d", fake.callCount(), tt.wantCalls)
			}
		})
	}
}

func TestListSchedules(t *testing.T) {
	t.Parallel()

	type tc struct {
		errCase
		req       *jobrunv1.ListSchedulesRequest
		wantCalls int
	}
	domain := scheduleErrCases()
	tests := make([]tc, 0, 4+len(domain))
	tests = append(tests,
		tc{errCase: errCase{name: "negative page size", wantCode: codes.InvalidArgument}, req: &jobrunv1.ListSchedulesRequest{PageSize: -1}},
		tc{errCase: errCase{name: "page size too big", wantCode: codes.InvalidArgument}, req: &jobrunv1.ListSchedulesRequest{PageSize: 201}},
		tc{errCase: errCase{name: "bad token encoding", wantCode: codes.InvalidArgument}, req: &jobrunv1.ListSchedulesRequest{PageToken: "!!!"}},
		tc{errCase: errCase{name: "token not a name", wantCode: codes.InvalidArgument}, req: &jobrunv1.ListSchedulesRequest{PageToken: base64.RawURLEncoding.EncodeToString([]byte("Not A Name"))}},
	)
	for _, c := range domain {
		tests = append(tests, tc{errCase: c, req: &jobrunv1.ListSchedulesRequest{PageSize: 10}, wantCalls: 1})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fake := &fakeStore{listSchedules: func(_ context.Context, after string, limit int32) ([]job.Schedule, error) {
				if after != "" || limit != 11 {
					t.Errorf("store got after=%q limit=%d, want \"\"/11", after, limit)
				}
				if tt.svcErr != nil {
					return nil, tt.svcErr
				}
				return []job.Schedule{sampleSchedule()}, nil
			}}
			c := newTestClients(t, fake)
			resp, err := c.schedules.ListSchedules(ctxWithRequestID(t, "req-lists-"+tt.name), tt.req)
			checkStatus(t, err, tt.wantCode, c.logs, "req-lists-"+tt.name)
			if fake.callCount() != tt.wantCalls {
				t.Errorf("store called %d times, want %d", fake.callCount(), tt.wantCalls)
			}
			if tt.wantCode != codes.OK {
				return
			}
			if len(resp.GetSchedules()) != 1 || resp.GetNextPageToken() != "" {
				t.Fatalf("resp = %v", resp)
			}
			checkSchedule(t, resp.GetSchedules()[0], sampleSchedule())
		})
	}
}

func TestListSchedulesPageTokenRoundTrip(t *testing.T) {
	t.Parallel()
	a, b, c3 := sampleSchedule(), sampleSchedule(), sampleSchedule()
	a.Name, b.Name, c3.Name = "alpha", "bravo", "charlie"

	var (
		mu     sync.Mutex
		afters []string
	)
	fake := &fakeStore{listSchedules: func(_ context.Context, after string, limit int32) ([]job.Schedule, error) {
		mu.Lock()
		afters = append(afters, after)
		mu.Unlock()
		if limit != 3 {
			t.Errorf("limit = %d, want 3 (page_size + 1)", limit)
		}
		if after == "" {
			return []job.Schedule{a, b, c3}, nil // one more than the page: there is a next page
		}
		return []job.Schedule{c3}, nil
	}}
	c := newTestClients(t, fake)

	first, err := c.schedules.ListSchedules(t.Context(), &jobrunv1.ListSchedulesRequest{PageSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.GetSchedules()) != 2 || first.GetSchedules()[1].GetName() != "bravo" {
		t.Fatalf("first page = %v", first.GetSchedules())
	}
	tok := first.GetNextPageToken()
	if tok == "" {
		t.Fatal("no next_page_token")
	}
	second, err := c.schedules.ListSchedules(t.Context(), &jobrunv1.ListSchedulesRequest{PageSize: 2, PageToken: tok})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.GetSchedules()) != 1 || second.GetSchedules()[0].GetName() != "charlie" || second.GetNextPageToken() != "" {
		t.Fatalf("second page = %v token=%q", second.GetSchedules(), second.GetNextPageToken())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(afters) != 2 || afters[0] != "" || afters[1] != "bravo" {
		t.Errorf("after names seen by the store = %q, want [\"\" \"bravo\"]", afters)
	}
}
