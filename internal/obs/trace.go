package obs

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/crowbett/jobrun/internal/job"
)

// TracerName is the instrumentation scope of every jobrun span.
const TracerName = "jobrun"

// Span names. docs/PLAN.md, Observability.
const (
	SpanEnqueue  = "jobrun.enqueue"
	SpanClaim    = "jobrun.claim"
	SpanRun      = "jobrun.run"
	SpanComplete = "jobrun.complete"
)

// Attribute keys on jobrun spans.
const (
	AttrQueue       = attribute.Key("jobrun.queue")
	AttrType        = attribute.Key("jobrun.type")
	AttrJobID       = attribute.Key("jobrun.job.id")
	AttrAttempt     = attribute.Key("jobrun.attempt")
	AttrJobsClaimed = attribute.Key("jobrun.jobs.claimed")
)

// Tracer returns the jobrun tracer from the global provider (a no-op until
// Setup runs with Enabled).
func Tracer() trace.Tracer { return otel.Tracer(TracerName) }

// StartEnqueue opens a jobrun.enqueue producer span around Store.Enqueue.
func StartEnqueue(ctx context.Context, j job.Job) (context.Context, trace.Span) {
	return Tracer().Start(ctx, SpanEnqueue,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(AttrQueue.String(j.Queue), AttrType.String(j.Type), AttrJobID.String(j.ID.String())))
}

// StartClaim opens a jobrun.claim span around one Store.Claim; call SetClaimed
// with the batch size before End.
func StartClaim(ctx context.Context, queue string) (context.Context, trace.Span) {
	return Tracer().Start(ctx, SpanClaim,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(AttrQueue.String(queue)))
}

// SetClaimed records how many jobs a claim returned.
func SetClaimed(span trace.Span, n int) { span.SetAttributes(AttrJobsClaimed.Int(n)) }

// traceContext is the W3C propagator, used directly (not the global one) so a
// traceparent is written and read even when OTel export is disabled.
var traceContext = propagation.TraceContext{}

// TraceParent returns the W3C traceparent of the span in ctx, or "" when ctx
// carries no valid span context (tracing disabled, untraced caller).
func TraceParent(ctx context.Context) string {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return ""
	}
	c := propagation.MapCarrier{}
	traceContext.Inject(ctx, c)
	return c.Get("traceparent")
}

// WithTraceParent returns ctx carrying tp as its remote parent span context.
// An empty or malformed tp returns ctx unchanged.
func WithTraceParent(ctx context.Context, tp string) context.Context {
	if tp == "" {
		return ctx
	}
	return traceContext.Extract(ctx, propagation.MapCarrier{"traceparent": tp})
}

// StartRun opens a jobrun.run consumer span around a handler invocation. When
// the job carries the traceparent captured at enqueue, the run span is its
// child, so enqueue -> run (-> complete) is one trace; the claim's own
// context is not a parent.
func StartRun(ctx context.Context, j job.Job) (context.Context, trace.Span) {
	ctx = WithTraceParent(ctx, j.TraceParent)
	return Tracer().Start(ctx, SpanRun,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			AttrQueue.String(j.Queue), AttrType.String(j.Type),
			AttrJobID.String(j.ID.String()), AttrAttempt.Int64(int64(j.Attempts)),
		))
}

// StartComplete opens a jobrun.complete span around Store.Complete (the
// handler's Commit plus the owner-guarded update).
func StartComplete(ctx context.Context, j job.Job) (context.Context, trace.Span) {
	return Tracer().Start(ctx, SpanComplete,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(AttrQueue.String(j.Queue), AttrType.String(j.Type), AttrJobID.String(j.ID.String())))
}

// End records err (if any) on span and ends it. Use with defer:
//
//	ctx, span := obs.StartRun(ctx, j)
//	defer func() { obs.End(span, err) }()
func End(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
