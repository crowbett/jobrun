package worker

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/crowbett/jobrun/internal/job"
	"github.com/crowbett/jobrun/internal/obs"
)

// installExporter swaps the global tracer provider for one that exports
// synchronously into memory. Not parallel-safe: callers must not t.Parallel.
func installExporter(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(context.Background())
	})
	return exp
}

func spansFor(exp *tracetest.InMemoryExporter, id job.ID) map[string]tracetest.SpanStub {
	out := map[string]tracetest.SpanStub{}
	spans := exp.GetSpans()
	for i := range spans {
		for _, a := range spans[i].Attributes {
			if a.Key == obs.AttrJobID && a.Value.AsString() == id.String() {
				out[spans[i].Name] = spans[i]
			}
		}
	}
	return out
}

// TestRunSpanIsChildOfEnqueueTrace: a job carrying the traceparent captured at
// enqueue runs under a jobrun.run span in that trace, parented to the enqueue
// span, and the completion span is a child of the run span. A job without a
// traceparent gets a fresh root trace.
func TestRunSpanIsChildOfEnqueueTrace(t *testing.T) { //nolint:paralleltest // swaps the global tracer provider
	exp := installExporter(t)
	const (
		traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
		spanID  = "00f067aa0ba902b7"
	)
	tests := []struct {
		name        string
		traceparent string
		wantParent  bool
	}{
		{name: "traced producer", traceparent: "00-" + traceID + "-" + spanID + "-01", wantParent: true},
		{name: "untraced producer", traceparent: ""},
		{name: "malformed traceparent is ignored", traceparent: "00-zz-yy-01"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			var handlerSpan trace.SpanContext
			reg := registry(t, "traced", job.HandlerFunc(func(ctx context.Context, _ job.Job) error {
				handlerSpan = trace.SpanContextFromContext(ctx)
				return nil
			}))
			j := newJob("traced")
			j.TraceParent = tc.traceparent
			store.add(j)
			p := NewPool(store, nil, reg, nil, nil, fastConfig())
			stop := startPool(t, p)
			recv(t, store.completes, "complete")
			if err := stop(); err != nil {
				t.Fatal(err)
			}

			spans := spansFor(exp, j.ID)
			run, ok := spans[obs.SpanRun]
			if !ok {
				t.Fatalf("no %s span; got %v", obs.SpanRun, exp.GetSpans())
			}
			if run.SpanContext.SpanID() != handlerSpan.SpanID() {
				t.Fatal("handler ctx does not carry the run span")
			}
			if tc.wantParent {
				if run.SpanContext.TraceID().String() != traceID || run.Parent.SpanID().String() != spanID || !run.Parent.IsRemote() {
					t.Fatalf("run span trace %s parent %s (remote %v), want trace %s parent %s",
						run.SpanContext.TraceID(), run.Parent.SpanID(), run.Parent.IsRemote(), traceID, spanID)
				}
			} else if run.Parent.IsValid() {
				t.Fatalf("run span has parent %v, want a root span", run.Parent)
			}
			done, ok := spans[obs.SpanComplete]
			if !ok || done.Parent.SpanID() != run.SpanContext.SpanID() || done.SpanContext.TraceID() != run.SpanContext.TraceID() {
				t.Fatalf("complete span %+v is not a child of the run span", done.Parent)
			}
		})
	}
}

// TestFencedRunSpanRecordsLeaseLost: the run span of a fenced job ends with an
// error status so the trace shows why no completion followed.
func TestFencedRunSpanRecordsLeaseLost(t *testing.T) { //nolint:paralleltest // swaps the global tracer provider
	exp := installExporter(t)
	store := newFakeStore()
	reg := registry(t, "fenced", job.HandlerFunc(func(ctx context.Context, _ job.Job) error {
		<-ctx.Done()
		return nil
	}))
	j := newJob("fenced")
	store.drop[j.ID] = true
	store.add(j)
	p := NewPool(store, nil, reg, nil, nil, fastConfig())
	stop := startPool(t, p)
	deadline := time.Now().Add(5 * time.Second)
	for p.Stats().Fenced == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	run, ok := spansFor(exp, j.ID)[obs.SpanRun]
	if !ok || run.Status.Code.String() != "Error" {
		t.Fatalf("run span = %+v", run.Status)
	}
	if _, ok := spansFor(exp, j.ID)[obs.SpanComplete]; ok {
		t.Fatal("a fenced job must not have a complete span")
	}
}
