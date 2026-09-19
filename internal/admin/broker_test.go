package admin

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/crowbett/jobrun/internal/job"
)

var (
	fixedNow = time.Date(2026, 9, 19, 12, 0, 1, 500_000_000, time.UTC)
	testID   = uuid.MustParse("019210a0-1234-7abc-8def-0123456789ab")
)

func ev(queue string, from, to job.Status) job.Event {
	return job.Event{JobID: testID, Queue: queue, Type: "t", From: from, To: to, Attempt: 1, At: fixedNow}
}

func TestBrokerFanOutAndCancel(t *testing.T) {
	t.Parallel()
	b := NewBroker(nil)
	a, cancelA := b.Subscribe(4)
	c, cancelC := b.Subscribe(4)
	if b.Subscribers() != 2 {
		t.Fatalf("Subscribers = %d, want 2", b.Subscribers())
	}

	b.Publish(ev("q", job.StatusQueued, job.StatusLeased))
	for name, ch := range map[string]<-chan job.Event{"a": a, "c": c} {
		select {
		case e := <-ch:
			if e.To != job.StatusLeased {
				t.Errorf("%s got %+v", name, e)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s did not receive", name)
		}
	}

	cancelA()
	cancelA() // idempotent
	if _, ok := <-a; ok {
		t.Error("cancelled channel is not closed")
	}
	if b.Subscribers() != 1 {
		t.Fatalf("Subscribers = %d after cancel, want 1", b.Subscribers())
	}
	b.Publish(ev("q", job.StatusLeased, job.StatusSucceeded)) // must not panic on the closed one
	if e := <-c; e.To != job.StatusSucceeded {
		t.Errorf("c got %+v", e)
	}
	cancelC()
	if b.Subscribers() != 0 || b.Dropped() != 0 {
		t.Errorf("Subscribers=%d Dropped=%d, want 0/0", b.Subscribers(), b.Dropped())
	}
}

func TestBrokerDropsForSlowSubscriberWithoutBlocking(t *testing.T) {
	t.Parallel()
	b := NewBroker(nil)
	slow, cancelSlow := b.Subscribe(2)
	defer cancelSlow()
	fast, cancelFast := b.Subscribe(16)
	defer cancelFast()

	done := make(chan struct{})
	go func() {
		for i := range 10 {
			b.Publish(ev("q", job.StatusQueued, job.StatusLeased))
			_ = i
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish blocked on a full subscriber")
	}
	if got := b.Dropped(); got != 8 {
		t.Errorf("Dropped = %d, want 8 (10 published, buffer 2)", got)
	}
	if len(slow) != 2 || len(fast) != 10 {
		t.Errorf("buffered slow=%d fast=%d, want 2/10", len(slow), len(fast))
	}
}

func TestBrokerCounters(t *testing.T) {
	t.Parallel()
	b := NewBroker(nil)
	seq := []job.Event{
		ev("a", job.StatusUnspecified, job.StatusQueued), // not counted
		ev("a", job.StatusQueued, job.StatusLeased),      // not counted
		ev("a", job.StatusLeased, job.StatusQueued),      // retried
		ev("a", job.StatusLeased, job.StatusSucceeded),
		ev("a", job.StatusLeased, job.StatusSucceeded),
		ev("a", job.StatusLeased, job.StatusDead),
		ev("b", job.StatusQueued, job.StatusCancelled),
		ev("b", job.StatusLeased, job.StatusDead),
		ev("b", job.StatusDead, job.StatusQueued), // Retry RPC: not a retry outcome
	}
	for _, e := range seq {
		b.Publish(e)
	}
	c := b.Counters()
	want := map[JobKey]uint64{
		{"a", OutcomeRetried}: 1, {"a", OutcomeSucceeded}: 2, {"a", OutcomeDead}: 1,
		{"b", OutcomeCancelled}: 1, {"b", OutcomeDead}: 1,
	}
	if len(c.Jobs) != len(want) {
		t.Fatalf("Jobs = %v, want %v", c.Jobs, want)
	}
	for k, n := range want {
		if c.Jobs[k] != n {
			t.Errorf("Jobs[%+v] = %d, want %d", k, c.Jobs[k], n)
		}
	}
	if c.Dead != 2 || c.LeaseExpired != 0 || c.SchedulerTicks != 0 {
		t.Errorf("counters = %+v", c)
	}
	c.Jobs[JobKey{"z", "x"}] = 1 // a copy: must not leak back
	if len(b.Counters().Jobs) != len(want) {
		t.Error("Counters returned the live map")
	}
}
