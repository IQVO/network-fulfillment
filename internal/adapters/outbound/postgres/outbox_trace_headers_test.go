package postgres

import (
	"context"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TestTraceHeaders_NoSpanAddsNothing: a publish outside any span (a test, a
// background job without tracing) stores only the encoder's headers.
func TestTraceHeaders_NoSpanAddsNothing(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	if got := traceHeaders(context.Background()); len(got) != 0 {
		t.Fatalf("traceHeaders without a span = %+v, want none", got)
	}
}

// TestTraceHeaders_SpanInjectsTraceparent: inside a span the W3C
// traceparent of that span is produced (gap fix D27, ADR 0022).
func TestTraceHeaders_SpanInjectsTraceparent(t *testing.T) {
	prevProp, prevTP := otel.GetTextMapPropagator(), otel.GetTracerProvider()
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTextMapPropagator(prevProp)
		otel.SetTracerProvider(prevTP)
	})

	ctx, span := tp.Tracer("t").Start(context.Background(), "op")
	defer span.End()
	got := traceHeaders(ctx)
	if len(got) != 1 || got[0].Key != "traceparent" {
		t.Fatalf("traceHeaders = %+v, want exactly one traceparent", got)
	}
	sc := span.SpanContext()
	want := "00-" + sc.TraceID().String() + "-" + sc.SpanID().String() + "-01"
	if string(got[0].Value) != want {
		t.Fatalf("traceparent = %q, want %q", got[0].Value, want)
	}
}

// TestWithTraceHeaders_EncoderKeyWinsAndInputUntouched: the CloudEvents
// content-type stays first, an encoder-set key is never overwritten, and the
// encoder's slice is not mutated.
func TestWithTraceHeaders_EncoderKeyWinsAndInputUntouched(t *testing.T) {
	encoder := []kafkago.Header{
		{Key: "content-type", Value: []byte("application/cloudevents+json")},
		{Key: "traceparent", Value: []byte("from-encoder")},
	}
	trace := []kafkago.Header{
		{Key: "traceparent", Value: []byte("from-span")},
		{Key: "tracestate", Value: []byte("k=v")},
	}
	got := withTraceHeaders(encoder, trace)
	if len(got) != 3 || got[0].Key != "content-type" || string(got[1].Value) != "from-encoder" || got[2].Key != "tracestate" {
		t.Fatalf("withTraceHeaders = %+v, want content-type, the encoder's traceparent, then tracestate", got)
	}
	if len(encoder) != 2 {
		t.Fatalf("input slice mutated: %+v", encoder)
	}
	if same := withTraceHeaders(encoder, nil); len(same) != 2 {
		t.Fatalf("no trace headers must return the encoder headers unchanged, got %+v", same)
	}
}
