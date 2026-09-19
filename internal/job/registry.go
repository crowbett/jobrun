package job

import (
	"context"
	"fmt"
	"slices"
	"sync"
)

// Handler runs a job. Returning nil completes it; returning an error fails the
// attempt (retried by Policy unless the error wraps ErrPermanent); ctx is
// cancelled when the lease is lost, the job is cancelled, JobTimeout fires or
// the process is draining.
type Handler interface {
	Handle(ctx context.Context, j Job) error
}

// TxHandler opts into exactly-once effects: the returned Commit runs inside the
// completion transaction together with the status update and the
// processed_effects row, so the effect and the completion are atomic.
type TxHandler interface {
	HandleTx(ctx context.Context, j Job) (Commit, error)
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(context.Context, Job) error

// Handle calls f.
func (f HandlerFunc) Handle(ctx context.Context, j Job) error { return f(ctx, j) }

// TxHandlerFunc adapts a function to TxHandler.
type TxHandlerFunc func(context.Context, Job) (Commit, error)

// HandleTx calls f.
func (f TxHandlerFunc) HandleTx(ctx context.Context, j Job) (Commit, error) { return f(ctx, j) }

// Registry maps job types to handlers. Registration happens before Pool.Run;
// lookups are concurrent-safe.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]any // Handler or TxHandler
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]any)}
}

// Register binds typ to h. It fails with ErrInvalidArgument for a bad type name
// or nil handler and ErrDuplicateHandler when typ is already bound.
func (r *Registry) Register(typ string, h Handler) error {
	if h == nil {
		return fmt.Errorf("%w: nil handler for %q", ErrInvalidArgument, typ)
	}
	return r.add(typ, h)
}

// RegisterTx binds typ to a TxHandler. Same errors as Register.
func (r *Registry) RegisterTx(typ string, h TxHandler) error {
	if h == nil {
		return fmt.Errorf("%w: nil handler for %q", ErrInvalidArgument, typ)
	}
	return r.add(typ, h)
}

func (r *Registry) add(typ string, h any) error {
	if err := ValidateName("type", typ); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.handlers[typ]; dup {
		return fmt.Errorf("%w: %q", ErrDuplicateHandler, typ)
	}
	r.handlers[typ] = h
	return nil
}

// Lookup returns the handler bound to typ, which is a Handler or a TxHandler
// (type-switch on it), and whether one exists.
func (r *Registry) Lookup(typ string) (any, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[typ]
	return h, ok
}

// Types returns the registered type names, sorted.
func (r *Registry) Types() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.handlers))
	for t := range r.handlers {
		out = append(out, t)
	}
	slices.Sort(out)
	return out
}
