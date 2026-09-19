package obs

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
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

// StartRun opens a jobrun.run consumer span around a handler invocation.
func StartRun(ctx context.Context, j job.Job) (context.Context, trace.Span) {
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
