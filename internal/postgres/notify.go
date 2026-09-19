package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/crowbett/jobrun/internal/job"
	"github.com/crowbett/jobrun/internal/postgres/sqlc"
)

// NotifyChannel is the LISTEN/NOTIFY channel; the payload is the queue name.
const NotifyChannel = "jobrun_ready"

// Notifier defaults.
const (
	// DefaultPollFloor is how often Wait ticks regardless of notifications.
	DefaultPollFloor = 200 * time.Millisecond
	// reconnect backoff for the listener connection.
	notifierMinBackoff = time.Second
	notifierMaxBackoff = 30 * time.Second
)

// Notifier implements job.Dispatcher over Postgres LISTEN/NOTIFY. Producers
// signal a queue with Ready (the Store also does it from inside its enqueue
// transactions, which is the path that cannot race the row); Run holds one
// dedicated connection on LISTEN and coalesces every notification into a
// 1-slot channel per waiter. Wait always also ticks at the poll floor, so a
// dropped or missed notification (disconnect, NOTIFY storm) costs latency, never
// correctness.
type Notifier struct {
	pool      *pgxpool.Pool
	q         *sqlc.Queries
	log       *slog.Logger
	channel   string
	pollFloor time.Duration

	mu      sync.Mutex
	waiters map[string]map[*waiter]struct{}
}

type waiter struct{ ch chan struct{} }

var _ job.Dispatcher = (*Notifier)(nil)

// NotifierOption configures a Notifier.
type NotifierOption func(*Notifier)

// WithPollFloor sets the Wait tick interval (default DefaultPollFloor).
func WithPollFloor(d time.Duration) NotifierOption {
	return func(n *Notifier) {
		if d > 0 {
			n.pollFloor = d
		}
	}
}

// WithChannel sets the LISTEN/NOTIFY channel (default NotifyChannel; must
// match the Store's WithNotifyChannel).
func WithChannel(name string) NotifierOption {
	return func(n *Notifier) {
		if name != "" {
			n.channel = name
		}
	}
}

// NewNotifier returns a Notifier over pool. The listener connection is opened
// by Run from the pool's connection config (it is not a pool connection).
func NewNotifier(pool *pgxpool.Pool, log *slog.Logger, opts ...NotifierOption) *Notifier {
	n := &Notifier{
		pool:      pool,
		q:         sqlc.New(pool),
		log:       log,
		channel:   NotifyChannel,
		pollFloor: DefaultPollFloor,
		waiters:   make(map[string]map[*waiter]struct{}),
	}
	for _, o := range opts {
		o(n)
	}
	return n
}

// Ready tells consumers that work exists on queue with a standalone NOTIFY.
// Producers that write through the Store do not need it: the Store notifies
// inside its transaction.
func (n *Notifier) Ready(ctx context.Context, queue string) error {
	if err := n.q.NotifyReady(ctx, sqlc.NotifyReadyParams{Channel: n.channel, Queue: queue}); err != nil {
		return fmt.Errorf("notify %s: %w", queue, err)
	}
	return nil
}

// Wait returns a channel that receives (coalesced, one pending signal at most)
// whenever queue is notified and at least every poll floor; it is closed when
// ctx is done.
func (n *Notifier) Wait(ctx context.Context, queue string) <-chan struct{} {
	w := &waiter{ch: make(chan struct{}, 1)}
	n.mu.Lock()
	set := n.waiters[queue]
	if set == nil {
		set = make(map[*waiter]struct{})
		n.waiters[queue] = set
	}
	set[w] = struct{}{}
	n.mu.Unlock()

	go func() {
		t := time.NewTicker(n.pollFloor)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				n.mu.Lock()
				delete(n.waiters[queue], w)
				if len(n.waiters[queue]) == 0 {
					delete(n.waiters, queue)
				}
				n.mu.Unlock()
				close(w.ch) // no sender can reach w after the removal above
				return
			case <-t.C:
				w.signal()
			}
		}
	}()
	return w.ch
}

func (w *waiter) signal() {
	select {
	case w.ch <- struct{}{}:
	default: // a signal is already pending; coalesce
	}
}

// signal wakes every waiter on queue.
func (n *Notifier) signal(queue string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for w := range n.waiters[queue] {
		w.signal()
	}
}

// signalAll wakes every waiter on every queue (after a (re)connect, so work
// that arrived while the listener was down is picked up at once).
func (n *Notifier) signalAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, set := range n.waiters {
		for w := range set {
			w.signal()
		}
	}
}

// Run holds the listener connection until ctx is done, reconnecting with
// backoff (1s doubling to 30s) after any failure. It returns nil on ctx
// cancellation; the poll floor covers every gap.
func (n *Notifier) Run(ctx context.Context) error {
	backoff := notifierMinBackoff
	for {
		start := time.Now()
		err := n.listen(ctx)
		select {
		case <-ctx.Done():
			return nil // a clean stop, whatever the connection reported
		default:
		}
		if time.Since(start) > notifierMaxBackoff {
			backoff = notifierMinBackoff // a long healthy session resets the ladder
		}
		n.log.WarnContext(ctx, "notifier listener lost, reconnecting",
			slog.String("err", err.Error()), slog.Duration("backoff", backoff))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, notifierMaxBackoff)
	}
}

// listen opens the dedicated connection, LISTENs, wakes every waiter once and
// then forwards notifications until the connection or ctx fails.
func (n *Notifier) listen(ctx context.Context) error {
	cfg := n.pool.Config().ConnConfig.Copy()
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connect listener: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()
	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{n.channel}.Sanitize()); err != nil {
		return fmt.Errorf("listen %s: %w", n.channel, err)
	}
	n.log.InfoContext(ctx, "notifier listening", slog.String("channel", n.channel))
	n.signalAll()
	for {
		nt, err := conn.WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("wait for notification: %w", err)
		}
		n.signal(nt.Payload)
	}
}
