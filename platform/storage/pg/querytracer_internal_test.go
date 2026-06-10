package pg

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/noop"
)

// TestQueryName pins the sqlc leading-comment parse: `-- name: X :kind` on
// the first line yields X; anything else — raw SQL, malformed comments,
// non-leading comments — yields the bounded fallback label "raw".
func TestQueryName(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "sqlc one",
			sql:  "-- name: GetOrderView :one\nselect order_id from orders_read where order_id = $1",
			want: "GetOrderView",
		},
		{
			name: "sqlc execrows with blank line",
			sql:  "-- name: MarkPaid :execrows\n\ninsert into orders_read values ($1)",
			want: "MarkPaid",
		},
		{
			name: "sqlc many",
			sql:  "-- name: FetchUnpublished :many\nselect id from outbox",
			want: "FetchUnpublished",
		},
		{
			name: "raw sql",
			sql:  "select 1",
			want: "raw",
		},
		{
			name: "empty",
			sql:  "",
			want: "raw",
		},
		{
			name: "comment not first",
			sql:  "select 1;\n-- name: Sneaky :one",
			want: "raw",
		},
		{
			name: "leading whitespace disqualifies",
			sql:  "  -- name: Indented :one\nselect 1",
			want: "raw",
		},
		{
			name: "malformed missing space",
			sql:  "-- name:NoSpace :one\nselect 1",
			want: "raw",
		},
		{
			name: "plain comment",
			sql:  "-- just a comment\nselect 1",
			want: "raw",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, queryName(tc.sql))
			// Second call exercises the cache path and must agree.
			require.Equal(t, tc.want, queryName(tc.sql))
		})
	}
}

// TestQueryTracer_EndPathAllocs: the per-query record path must not allocate
// once the {query, pool} attribute set is cached — every query/batch/COPY end
// hook runs it. A noop instrument isolates the tracer's own path from SDK
// aggregation internals.
func TestQueryTracer_EndPathAllocs(t *testing.T) {
	// NOT parallel: testing.AllocsPerRun forbids parallel tests.
	tr := &queryTracer{pool: attribute.String("pool", "writer")}
	h, err := noop.NewMeterProvider().Meter(tracerMeterName).Float64Histogram("pg.query.duration")
	require.NoError(t, err)
	tr.hist = h

	ctx := tr.start(context.Background(), "GetOrderView")
	tr.end(ctx) // warm the attribute cache

	allocs := testing.AllocsPerRun(1000, func() { tr.end(ctx) })
	require.Zero(t, allocs, "cached end path must not allocate")
}
