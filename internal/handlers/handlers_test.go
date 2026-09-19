package handlers

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/crowbett/jobrun/internal/job"
)

func TestRegisterBindsEveryType(t *testing.T) {
	t.Parallel()
	reg := job.NewRegistry()
	if err := Register(reg); err != nil {
		t.Fatal(err)
	}
	want := []string{TypeEcho, TypeFail, TypeNoop, TypeSleep}
	if got := reg.Types(); !slices.Equal(got, want) {
		t.Fatalf("types = %v, want %v", got, want)
	}
	if err := Register(reg); !errors.Is(err, job.ErrDuplicateHandler) {
		t.Fatalf("second Register = %v, want ErrDuplicateHandler", err)
	}
}

func run(t *testing.T, h job.Handler, payload string, attempts int32) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return h.Handle(ctx, job.Job{ID: job.NewID(), Queue: "q", Type: "t", Payload: []byte(payload), Attempts: attempts})
}

func TestNoop(t *testing.T) {
	t.Parallel()
	if err := run(t, Noop(), `{"anything":true}`, 1); err != nil {
		t.Fatal(err)
	}
}

func TestSleep(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		payload string
		wantErr error
		minTook time.Duration
	}{
		{"empty payload", "", nil, 0},
		{"zero", `{"ms":0}`, nil, 0},
		{"sleeps", `{"ms":30}`, nil, 30 * time.Millisecond},
		{"negative is permanent", `{"ms":-1}`, job.ErrPermanent, 0},
		{"garbage is permanent", `{"ms":`, job.ErrPermanent, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			start := time.Now()
			err := run(t, Sleep(), tc.payload, 1)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if took := time.Since(start); took < tc.minTook {
				t.Fatalf("took %v, want >= %v", took, tc.minTook)
			}
		})
	}
	t.Run("cancel", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := Sleep().Handle(ctx, job.Job{Payload: []byte(`{"ms":60000}`)})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	})
}

func TestFail(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		payload  string
		attempts int32
		wantErr  bool
		wantPerm bool
	}{
		{"default never fails", "", 1, false, false},
		{"attempt 1 of 2 fails", `{"times":2}`, 1, true, false},
		{"attempt 2 of 2 fails", `{"times":2}`, 2, true, false},
		{"attempt 3 succeeds", `{"times":2}`, 3, false, false},
		{"permanent", `{"permanent":true}`, 1, true, true},
		{"permanent beats times", `{"times":0,"permanent":true}`, 5, true, true},
		{"garbage is permanent", `nope`, 1, true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := run(t, Fail(), tc.payload, tc.attempts)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if errors.Is(err, job.ErrPermanent) != tc.wantPerm {
				t.Fatalf("err = %v, wantPerm %v", err, tc.wantPerm)
			}
		})
	}
}

func TestEchoLogsPayload(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	if err := run(t, Echo(log), `{"hello":"world"}`, 2); err != nil {
		t.Fatal(err)
	}
	if out := buf.String(); !strings.Contains(out, `hello`) || !strings.Contains(out, "attempt=2") {
		t.Fatalf("log = %q", out)
	}
	if err := run(t, Echo(nil), "", 1); err != nil {
		t.Fatal(err)
	}
}
