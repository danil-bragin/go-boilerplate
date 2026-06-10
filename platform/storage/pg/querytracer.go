package pg

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// tracerMeterName is the otel instrumentation scope for pg query metrics.
const tracerMeterName = "storage.pg"

// sqlcNameRE extracts the query name from sqlc's leading comment, which sqlc
// always embeds as the first line of every generated SQL constant:
//
//	-- name: GetOrderView :one
var sqlcNameRE = regexp.MustCompile(`^-- name: (\S+)`)

// sqlcNamePrefix is the cheap pre-check before running sqlcNameRE.
const sqlcNamePrefix = "-- name: "

// queryNameCache memoizes SQL text → query label. Only sqlc-named queries are
// cached: their SQL constants are a small, fixed set, so the map is bounded by
// the sqlc registry. Un-named (raw/dynamic) SQL is NOT cached — it would grow
// the map unboundedly — and instead pays a prefix check per call, which fails
// fast without the regexp.
var queryNameCache sync.Map // string(SQL) → string(name)

// queryName returns the sqlc query name parsed from sql's leading
// `-- name: X :kind` comment, or "raw" when absent (bounded fallback label).
func queryName(sql string) string {
	if !strings.HasPrefix(sql, sqlcNamePrefix) {
		return "raw"
	}
	if v, ok := queryNameCache.Load(sql); ok {
		return v.(string) //nolint:forcetypeassert // cache stores strings only
	}
	name := "raw"
	if m := sqlcNameRE.FindStringSubmatch(sql); m != nil {
		name = m[1]
	}
	queryNameCache.Store(sql, name)
	return name
}

// queryTracer records a pg.query.duration histogram sample (SECONDS) for
// every query, batch, and CopyFrom executed through the pool it is installed
// on. Labels are bounded: query (sqlc name or "raw") and pool (the ROLE the
// tracer's pool was built for — "writer" or "reader"; note a writer pool
// serving reads when no replica is configured still reports "writer").
//
// The tracer is allocation-light by design: the start hooks stash a single
// value (start time + parsed label) in the context; the end hooks record via
// a per-tracer cache of pre-encoded attribute sets and do not allocate at all
// (asserted by TestQueryTracer_EndPathAllocs).
type queryTracer struct {
	pool attribute.KeyValue
	hist metric.Float64Histogram

	// attrs caches the pre-encoded {query, pool} option slice per query name.
	// The pool label is fixed per tracer instance, so the name alone keys the
	// cache; cardinality is bounded by the sqlc registry plus the fixed
	// fallback labels (raw/batch/copyfrom).
	attrs sync.Map // string(query name) → []metric.RecordOption
}

// newQueryTracer creates the tracer for one pool role from the GLOBAL otel
// meter (same pattern as the messaging metrics: nil-degrade on creation
// failure — metrics must never break queries).
func newQueryTracer(role string) *queryTracer {
	t := &queryTracer{pool: attribute.String("pool", role)}
	if h, err := otel.Meter(tracerMeterName).Float64Histogram(
		"pg.query.duration",
		metric.WithDescription("Database query duration by sqlc query name and pool role"),
		metric.WithUnit("s"),
	); err == nil {
		t.hist = h
	}
	return t
}

// queryStartKey carries queryStart through the ctx between start/end hooks.
type queryStartKey struct{}

type queryStart struct {
	at   time.Time
	name string
}

func (t *queryTracer) start(ctx context.Context, name string) context.Context {
	if t.hist == nil {
		return ctx
	}
	return context.WithValue(ctx, queryStartKey{}, queryStart{at: time.Now(), name: name})
}

func (t *queryTracer) end(ctx context.Context) {
	if t.hist == nil {
		return
	}
	s, ok := ctx.Value(queryStartKey{}).(queryStart)
	if !ok {
		return
	}
	t.hist.Record(ctx, time.Since(s.at).Seconds(), t.recordOpts(s.name)...)
}

// recordOpts returns the cached {query, pool} options for a query name,
// building and caching them on first use.
func (t *queryTracer) recordOpts(name string) []metric.RecordOption {
	if v, ok := t.attrs.Load(name); ok {
		return v.([]metric.RecordOption) //nolint:forcetypeassert // cache stores []metric.RecordOption only
	}
	opts := []metric.RecordOption{metric.WithAttributeSet(attribute.NewSet(
		attribute.String("query", name),
		t.pool,
	))}
	v, _ := t.attrs.LoadOrStore(name, opts)
	return v.([]metric.RecordOption) //nolint:forcetypeassert // cache stores []metric.RecordOption only
}

// TraceQueryStart implements pgx.QueryTracer.
func (t *queryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return t.start(ctx, queryName(data.SQL))
}

// TraceQueryEnd implements pgx.QueryTracer.
func (t *queryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	t.end(ctx)
}

// TraceBatchStart implements pgx.BatchTracer. A batch spans multiple queries,
// so the whole round-trip is recorded under the fixed label "batch".
func (t *queryTracer) TraceBatchStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchStartData) context.Context {
	return t.start(ctx, "batch")
}

// TraceBatchQuery implements pgx.BatchTracer (per-query completion carries no
// per-query timing, so it is a no-op; the batch is measured end-to-end).
func (t *queryTracer) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {}

// TraceBatchEnd implements pgx.BatchTracer.
func (t *queryTracer) TraceBatchEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceBatchEndData) {
	t.end(ctx)
}

// TraceCopyFromStart implements pgx.CopyFromTracer; COPY is recorded under
// the fixed label "copyfrom" (table names would be redundant cardinality —
// the per-table split belongs in traces, not metrics).
func (t *queryTracer) TraceCopyFromStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceCopyFromStartData) context.Context {
	return t.start(ctx, "copyfrom")
}

// TraceCopyFromEnd implements pgx.CopyFromTracer.
func (t *queryTracer) TraceCopyFromEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceCopyFromEndData) {
	t.end(ctx)
}
