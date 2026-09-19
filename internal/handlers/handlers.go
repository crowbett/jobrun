// Package handlers holds the built-in job types used by demos and the
// benchmark: noop, sleep, fail and echo. Register binds all of them.
package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/crowbett/jobrun/internal/job"
)

// Built-in job type names.
const (
	TypeNoop  = "noop"
	TypeSleep = "sleep"
	TypeFail  = "fail"
	TypeEcho  = "echo"
)

// Register binds every built-in type on reg; echo logs through slog.Default().
func Register(reg *job.Registry) error {
	return RegisterWith(reg, slog.Default())
}

// RegisterWith is Register with an explicit logger for echo.
func RegisterWith(reg *job.Registry, log *slog.Logger) error {
	if log == nil {
		log = slog.Default()
	}
	all := []struct {
		typ string
		h   job.Handler
	}{
		{TypeNoop, Noop()},
		{TypeSleep, Sleep()},
		{TypeFail, Fail()},
		{TypeEcho, Echo(log)},
	}
	for _, e := range all {
		if err := reg.Register(e.typ, e.h); err != nil {
			return fmt.Errorf("handlers: register %s: %w", e.typ, err)
		}
	}
	return nil
}

// Noop succeeds immediately; it is the benchmark's handler.
func Noop() job.Handler {
	return job.HandlerFunc(func(context.Context, job.Job) error { return nil })
}

// Sleep blocks for payload {"ms": N} (default 0) or until ctx is done, in which
// case it returns ctx.Err(). A negative N is a permanent failure.
func Sleep() job.Handler {
	return job.HandlerFunc(func(ctx context.Context, j job.Job) error {
		var p struct {
			MS int64 `json:"ms"`
		}
		if err := decode(j.Payload, &p); err != nil {
			return err
		}
		if p.MS < 0 {
			return fmt.Errorf("%w: sleep: ms must not be negative, got %d", job.ErrPermanent, p.MS)
		}
		if p.MS == 0 {
			return nil
		}
		t := time.NewTimer(time.Duration(p.MS) * time.Millisecond)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			return nil
		}
	})
}

// Fail fails the first N attempts of payload {"times": N} (default 0, i.e.
// never) and succeeds from attempt N+1 on; {"permanent": true} fails with
// job.ErrPermanent on every attempt. Attempts are counted by the store at claim
// time, so j.Attempts is 1 on the first run.
func Fail() job.Handler {
	return job.HandlerFunc(func(_ context.Context, j job.Job) error {
		var p struct {
			Times     int32 `json:"times"`
			Permanent bool  `json:"permanent"`
		}
		if err := decode(j.Payload, &p); err != nil {
			return err
		}
		if p.Permanent {
			return fmt.Errorf("%w: fail: permanent requested", job.ErrPermanent)
		}
		if j.Attempts <= p.Times {
			return fmt.Errorf("fail: attempt %d of %d requested failures", j.Attempts, p.Times)
		}
		return nil
	})
}

// Echo logs the job's payload at info level and succeeds.
func Echo(log *slog.Logger) job.Handler {
	if log == nil {
		log = slog.Default()
	}
	return job.HandlerFunc(func(_ context.Context, j job.Job) error {
		log.Info("echo", "job", j.ID, "queue", j.Queue, "attempt", j.Attempts, "payload", string(j.Payload))
		return nil
	})
}

// decode parses a JSON payload; empty means {} and malformed JSON is permanent
// (retrying cannot fix the payload).
func decode(payload []byte, into any) error {
	if len(payload) == 0 {
		return nil
	}
	if err := json.Unmarshal(payload, into); err != nil {
		return fmt.Errorf("%w: payload: %w", job.ErrPermanent, err)
	}
	return nil
}
