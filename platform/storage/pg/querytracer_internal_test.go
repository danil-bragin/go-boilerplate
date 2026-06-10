package pg

import (
	"testing"

	"github.com/stretchr/testify/require"
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
