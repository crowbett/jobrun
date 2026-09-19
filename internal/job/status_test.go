package job

import (
	"errors"
	"fmt"
	"testing"
)

type edge struct{ from, to Status }

// legalEdges is the transition table restated independently of status.go.
var legalEdges = map[edge]bool{
	{StatusUnspecified, StatusQueued}: true, // enqueue
	{StatusQueued, StatusLeased}:      true, // claim
	{StatusQueued, StatusCancelled}:   true, // cancel
	{StatusLeased, StatusSucceeded}:   true, // complete
	{StatusLeased, StatusQueued}:      true, // fail with retries left / reaped
	{StatusLeased, StatusDead}:        true, // permanent / exhausted
	{StatusLeased, StatusCancelled}:   true, // cooperative cancel
	{StatusFailed, StatusQueued}:      true, // retry (reserved status)
	{StatusDead, StatusQueued}:        true, // retry
	{StatusCancelled, StatusQueued}:   true, // retry
}

func TestStatusTransitionTable(t *testing.T) {
	t.Parallel()
	all := append([]Status{StatusUnspecified}, Statuses...)
	for _, from := range all {
		for _, to := range all {
			t.Run(fmt.Sprintf("%s->%s", from, to), func(t *testing.T) {
				t.Parallel()
				want := legalEdges[edge{from, to}]
				if got := from.CanTransition(to); got != want {
					t.Fatalf("CanTransition = %v, want %v", got, want)
				}
				err := from.Transition(to)
				if want && err != nil {
					t.Fatalf("Transition: unexpected %v", err)
				}
				if !want && !errors.Is(err, ErrInvalidTransition) {
					t.Fatalf("Transition: want ErrInvalidTransition, got %v", err)
				}
			})
		}
	}
	if n := len(legalEdges); n != 10 {
		t.Fatalf("table has %d edges, expected 10", n)
	}
}

func TestStatusOutOfRange(t *testing.T) {
	t.Parallel()
	bad := Status(99)
	if bad.Valid() || bad.CanTransition(StatusQueued) || StatusQueued.CanTransition(bad) {
		t.Fatal("out-of-range status must be invalid and untransitionable")
	}
	if got := bad.String(); got != "Status(99)" {
		t.Fatalf("String = %q", got)
	}
	if _, err := bad.MarshalText(); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("MarshalText: %v", err)
	}
}

func TestStatusStringParseRoundTrip(t *testing.T) {
	t.Parallel()
	want := map[Status]string{
		StatusUnspecified: "unspecified", StatusQueued: "queued", StatusLeased: "leased",
		StatusSucceeded: "succeeded", StatusFailed: "failed", StatusDead: "dead", StatusCancelled: "cancelled",
	}
	for s, name := range want {
		if s.String() != name {
			t.Errorf("%d.String() = %q, want %q", int(s), s.String(), name)
		}
		got, err := ParseStatus(name)
		if err != nil || got != s {
			t.Errorf("ParseStatus(%q) = %v, %v", name, got, err)
		}
		var u Status
		if err := u.UnmarshalText([]byte(name)); err != nil || u != s {
			t.Errorf("UnmarshalText(%q) = %v, %v", name, u, err)
		}
	}
	if _, err := ParseStatus("QUEUED"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("ParseStatus is case-sensitive by design, got %v", err)
	}
}

func TestStatusTerminal(t *testing.T) {
	t.Parallel()
	for _, s := range Statuses {
		want := s == StatusSucceeded || s == StatusDead || s == StatusCancelled
		if s.Terminal() != want {
			t.Errorf("%s.Terminal() = %v", s, s.Terminal())
		}
	}
}
