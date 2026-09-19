package job

import (
	"context"
	"errors"
	"slices"
	"testing"
)

func TestRegistryRegisterLookupTypes(t *testing.T) {
	t.Parallel()
	r := NewRegistry()
	noop := HandlerFunc(func(context.Context, Job) error { return nil })
	tx := TxHandlerFunc(func(context.Context, Job) (Commit, error) { return nil, nil })

	if err := r.Register("sleep", noop); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterTx("effect", tx); err != nil {
		t.Fatal(err)
	}
	if err := r.Register("noop", noop); err != nil {
		t.Fatal(err)
	}

	h, ok := r.Lookup("sleep")
	if !ok {
		t.Fatal("sleep not found")
	}
	if _, isHandler := h.(Handler); !isHandler {
		t.Fatalf("sleep is %T, want Handler", h)
	}
	h, ok = r.Lookup("effect")
	if !ok {
		t.Fatal("effect not found")
	}
	if _, isTx := h.(TxHandler); !isTx {
		t.Fatalf("effect is %T, want TxHandler", h)
	}
	if _, ok := r.Lookup("missing"); ok {
		t.Fatal("missing type must not be found")
	}
	if got, want := r.Types(), []string{"effect", "noop", "sleep"}; !slices.Equal(got, want) {
		t.Fatalf("Types = %v, want %v", got, want)
	}
}

func TestRegistryErrors(t *testing.T) {
	t.Parallel()
	r := NewRegistry()
	noop := HandlerFunc(func(context.Context, Job) error { return nil })
	tx := TxHandlerFunc(func(context.Context, Job) (Commit, error) { return nil, nil })
	if err := r.Register("dup", noop); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		do   func() error
		want error
	}{
		{"duplicate Register", func() error { return r.Register("dup", noop) }, ErrDuplicateHandler},
		{"duplicate via RegisterTx", func() error { return r.RegisterTx("dup", tx) }, ErrDuplicateHandler},
		{"empty type", func() error { return r.Register("", noop) }, ErrInvalidArgument},
		{"bad type chars", func() error { return r.Register("Send Mail", noop) }, ErrInvalidArgument},
		{"nil handler", func() error { return r.Register("nil", nil) }, ErrInvalidArgument},
		{"nil tx handler", func() error { return r.RegisterTx("niltx", nil) }, ErrInvalidArgument},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := tc.do(); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
	if got := r.Types(); len(got) != 1 || got[0] != "dup" {
		t.Fatalf("failed registrations must not leak: %v", got)
	}
}

func TestHandlerFuncAdapters(t *testing.T) {
	t.Parallel()
	called := 0
	var h Handler = HandlerFunc(func(context.Context, Job) error { called++; return ErrPermanent })
	if err := h.Handle(context.Background(), Job{}); !errors.Is(err, ErrPermanent) || called != 1 {
		t.Fatalf("Handle = %v, called %d", err, called)
	}
	var th TxHandler = TxHandlerFunc(func(context.Context, Job) (Commit, error) {
		return func(context.Context, Execer) error { called++; return nil }, nil
	})
	c, err := th.HandleTx(context.Background(), Job{})
	if err != nil || c == nil {
		t.Fatalf("HandleTx = %v, %v", c, err)
	}
	if err := c(context.Background(), nil); err != nil || called != 2 {
		t.Fatalf("commit = %v, called %d", err, called)
	}
}
