package job

import (
	"context"
	"testing"
	"time"
)

func TestPollOnlyTicksAndCloses(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	d := PollOnly{Every: 5 * time.Millisecond}
	if err := d.Ready(ctx, "q"); err != nil {
		t.Fatal(err)
	}
	ch := d.Wait(ctx, "q")
	for i := 0; i < 3; i++ {
		select {
		case _, ok := <-ch:
			if !ok {
				t.Fatal("channel closed before cancel")
			}
		case <-time.After(time.Second):
			t.Fatal("no tick within 1s")
		}
	}
	cancel()
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return // closed after cancel
			}
		case <-deadline:
			t.Fatal("channel not closed after cancel")
		}
	}
}

func TestPollOnlyDefaultInterval(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := PollOnly{}.Wait(ctx, "q")
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("default 200ms ticker did not fire")
	}
}
