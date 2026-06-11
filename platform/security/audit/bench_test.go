package audit_test

import (
	"context"
	"fmt"
	"testing"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/security/audit"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/stretchr/testify/require"
)

// benchEntry is a representative audit entry (a few metadata keys, mirroring a
// real command audit).
func benchEntry(i int) audit.Entry {
	return audit.Entry{
		Actor:    "bench-actor",
		Action:   "order:create",
		Subject:  fmt.Sprintf("order-%d", i),
		Metadata: map[string]string{"ip": "10.0.0.1", "seq": fmt.Sprintf("%d", i)},
	}
}

// BenchmarkRecord_PlainInsert is the BASELINE: a bare audit_log insert with NO
// hash chain (raw SQL on the writer pool). It isolates the cost the hash-chain
// adds in BenchmarkRecord_HashChain.
func BenchmarkRecord_PlainInsert(b *testing.B) {
	pool := newBenchPool(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		e := benchEntry(i)
		_, err := pool.Writer().Exec(ctx,
			`insert into audit_log (actor, action, subject, metadata) values ($1, $2, $3, '{}'::jsonb)`,
			e.Actor, e.Action, e.Subject)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRecord_HashChain measures Record with the global hash chain
// (chain-head FOR UPDATE + sha256 + head advance) under a single writer.
func BenchmarkRecord_HashChain(b *testing.B) {
	pool := newBenchPool(b)
	store := audit.NewPgStore(pool)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if err := pg.RunInTx(ctx, pool, func(ctx context.Context) error {
			return store.Record(ctx, benchEntry(i))
		}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRecord_HashChainParallel measures the CONTENTION of the single
// global chain: many goroutines all serialize on the chain-head FOR UPDATE.
// Compare ns/op against the single-writer HashChain bench to read the
// throughput ceiling the global chain imposes.
func BenchmarkRecord_HashChainParallel(b *testing.B) {
	pool := newBenchPool(b)
	store := audit.NewPgStore(pool)
	ctx := context.Background()
	b.ResetTimer()
	var ctr int
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ctr++
			if err := pg.RunInTx(ctx, pool, func(ctx context.Context) error {
				return store.Record(ctx, benchEntry(ctr))
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// newBenchPool mirrors newPool but for benchmarks (which have *testing.B).
func newBenchPool(b *testing.B) *pg.Pool {
	b.Helper()
	if testing.Short() {
		b.Skip("benchmark requires Docker (postgres container)")
	}
	dsn := pgtest.NewDSN(b)
	ctx := context.Background()
	require.NoError(b, pg.Migrate(ctx, dsn, migrations, "migrations"))
	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn)})
	require.NoError(b, err)
	b.Cleanup(func() { _ = pool.Close(ctx) })
	return pool
}
