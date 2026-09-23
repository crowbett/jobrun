package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/crowbett/jobrun/internal/job"
	"github.com/crowbett/jobrun/pkg/jobworker"
)

func TestParse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		args    []string
		wantErr bool
		check   func(options) bool
	}{
		{name: "defaults", args: nil, check: func(o options) bool { return o.concurrency == jobworker.DefaultConcurrency && o.drain > 0 }},
		{name: "flags", args: []string{"-addr", "h:1", "-queues", "a,b", "-id", "w1", "-concurrency", "3"}, check: func(o options) bool {
			return o.addr == "h:1" && o.queues == "a,b" && o.id == "w1" && o.concurrency == 3
		}},
		{name: "zero concurrency", args: []string{"-concurrency", "0"}, wantErr: true},
		{name: "huge concurrency", args: []string{"-concurrency", "5000"}, wantErr: true},
		{name: "stray argument", args: []string{"extra"}, wantErr: true},
		{name: "unknown flag", args: []string{"-nope"}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o, err := parse(tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if err == nil && !tc.check(o) {
				t.Fatalf("options = %+v", o)
			}
		})
	}
}

func TestSplitList(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"a", []string{"a"}},
		{" a , b ,,c ", []string{"a", "b", "c"}},
		{",,,", nil},
		{"default,bench", []string{"default", "bench"}},
	}
	for _, tc := range tests {
		if got := splitList(tc.in); !slices.Equal(got, tc.want) {
			t.Errorf("splitList(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestAdapt: the in-process handlers run on remote jobs; job.ErrPermanent
// becomes jobworker.ErrPermanent so the job goes straight to dead.
func TestAdapt(t *testing.T) {
	t.Parallel()
	id := job.NewID()
	tests := []struct {
		name     string
		inner    error
		jobID    string
		wantPerm bool
		wantErr  bool
	}{
		{name: "ok", jobID: id.String()},
		{name: "transient", inner: errors.New("flaky"), jobID: id.String(), wantErr: true},
		{name: "permanent", inner: fmt.Errorf("%w: bad", job.ErrPermanent), jobID: id.String(), wantErr: true, wantPerm: true},
		{name: "bad id", jobID: "not-a-uuid", wantErr: true, wantPerm: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var got job.Job
			h := adapt(job.HandlerFunc(func(_ context.Context, j job.Job) error {
				got = j
				return tc.inner
			}))
			err := h.Handle(context.Background(), jobworker.Job{ID: tc.jobID, Queue: "q", Type: "t", Attempt: 2, LeaseToken: 7, Payload: []byte(`{}`)})
			if (err != nil) != tc.wantErr || errors.Is(err, jobworker.ErrPermanent) != tc.wantPerm {
				t.Fatalf("err = %v", err)
			}
			if tc.jobID == id.String() && (got.ID != id || got.Attempts != 2 || got.LeaseToken != 7 || got.Queue != "q") {
				t.Fatalf("inner job = %+v", got)
			}
		})
	}
}
