package obs

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/crowbett/jobrun/internal/job"
)

func TestSetupDisabledIsNoop(t *testing.T) {
	t.Parallel()
	stop, err := Setup(t.Context(), Config{Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	if err := stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := RegisterSnapshot(func() Snapshot { return Snapshot{Claimed: 1} }); err != nil {
		t.Fatalf("RegisterSnapshot on the no-op meter: %v", err)
	}
	if err := RegisterQueueDepth(func() []job.DepthRow { return nil }); err != nil {
		t.Fatalf("RegisterQueueDepth on the no-op meter: %v", err)
	}
}

func TestEndpointOptions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		endpoint     string
		wantInsecure bool
		wantURL      string
	}{
		{"localhost:4317", true, "http://localhost:4317"},
		{"http://collector:4317", true, "http://collector:4317"},
		{"https://otel.example.com", false, "https://otel.example.com"},
		{"HTTPS://otel.example.com", false, "HTTPS://otel.example.com"},
		{"", true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.endpoint, func(t *testing.T) {
			t.Parallel()
			if got := insecure(tt.endpoint); got != tt.wantInsecure {
				t.Errorf("insecure = %v, want %v", got, tt.wantInsecure)
			}
			if tt.endpoint != "" {
				if got := normalizeEndpoint(tt.endpoint); got != tt.wantURL {
					t.Errorf("normalizeEndpoint = %q, want %q", got, tt.wantURL)
				}
			}
			if n := len(traceOptions(tt.endpoint)); n < 1 || n > 3 {
				t.Errorf("traceOptions returned %d options", n)
			}
		})
	}
}

func TestLogHandlerAddsTraceIDs(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	log := slog.New(NewLogHandler(slog.NewJSONHandler(&buf, nil)).WithAttrs([]slog.Attr{slog.String("svc", "x")}).WithGroup("g"))

	log.InfoContext(t.Context(), "no span")
	if strings.Contains(buf.String(), TraceIDKey) {
		t.Errorf("trace_id present without a span: %s", buf.String())
	}

	buf.Reset()
	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("t").Start(t.Context(), "s")
	log.InfoContext(ctx, "with span")
	span.End()
	out := buf.String()
	if !strings.Contains(out, span.SpanContext().TraceID().String()) || !strings.Contains(out, span.SpanContext().SpanID().String()) {
		t.Errorf("log lacks the span ids: %s", out)
	}
	if !strings.Contains(out, `"svc":"x"`) {
		t.Errorf("WithAttrs lost: %s", out)
	}
}

// TestSpanHelpers installs a recording provider as the global one (the helpers
// read otel.Tracer), so it is deliberately not parallel.
func TestSpanHelpers(t *testing.T) { //nolint:paralleltest // swaps the global tracer provider
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	j := job.Job{ID: job.NewID(), Queue: "default", Type: "email", Attempts: 2}

	ctx, parent := Tracer().Start(t.Context(), "parent")
	_, s1 := StartEnqueue(ctx, j)
	End(s1, nil)
	_, s2 := StartClaim(ctx, "default")
	SetClaimed(s2, 7)
	End(s2, nil)
	_, s3 := StartRun(ctx, j)
	End(s3, errors.New("boom"))
	_, s4 := StartComplete(ctx, j)
	End(s4, nil)
	parent.End()

	ended := rec.Ended()
	if len(ended) != 5 {
		t.Fatalf("spans = %d, want 5", len(ended))
	}
	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range ended {
		byName[s.Name()] = s
	}
	has := func(s sdktrace.ReadOnlySpan, kv attribute.KeyValue) bool {
		for _, a := range s.Attributes() {
			if a == kv {
				return true
			}
		}
		return false
	}
	if s := byName[SpanEnqueue]; s == nil || s.SpanKind() != trace.SpanKindProducer || !has(s, AttrJobID.String(j.ID.String())) || !has(s, AttrQueue.String("default")) {
		t.Errorf("enqueue span = %+v", s)
	}
	if s := byName[SpanClaim]; s == nil || !has(s, AttrJobsClaimed.Int(7)) {
		t.Errorf("claim span lacks jobs.claimed")
	}
	if s := byName[SpanRun]; s == nil || s.SpanKind() != trace.SpanKindConsumer || s.Status().Code != codes.Error || !has(s, AttrAttempt.Int64(2)) || len(s.Events()) != 1 {
		t.Errorf("run span = %+v", s)
	}
	if s := byName[SpanComplete]; s == nil || s.Status().Code != codes.Unset {
		t.Errorf("complete span = %+v", s)
	}
}
