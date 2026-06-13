package pg

import (
	"context"
	"errors"
)

type shardKeyCtxKey struct{}

// WithShardKey binds the aggregate shard key to ctx. ShardedPool.RunInTx /
// FromContext / FromContextRead route to Resolve(key). Consumers set it from the
// Kafka record key; the gateway sets it from the aggregate id at the edge.
func WithShardKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, shardKeyCtxKey{}, key)
}

func shardKeyFrom(ctx context.Context) (string, bool) {
	k, ok := ctx.Value(shardKeyCtxKey{}).(string)
	return k, ok && k != ""
}

// errNoShardKey is returned by keyed operations when ctx carries no shard key.
// Fail-closed: a missing key is a wiring bug, never a silent shard-0 write.
var errNoShardKey = errors.New("pg: no shard key in context (use WithShardKey before a sharded operation)")

// RunInTx resolves the shard from the ctx shard key and runs fn in a transaction
// on that shard, delegating to the single-pool RunInTx. The ambient-tx context
// key is shared with RunInTx, so FromContext inside fn observes the transaction.
func (sp *ShardedPool) RunInTx(ctx context.Context, fn func(ctx context.Context) error) error {
	key, ok := shardKeyFrom(ctx)
	if !ok {
		return errNoShardKey
	}
	return RunInTx(ctx, sp.Resolve(key), fn)
}

// FromContext returns the ambient transaction if one is open, else the writer of
// the shard resolved from the ctx shard key. Fail-closed when no key is set. It
// only needs the key to pick the pool: the single-pool FromContext already
// returns the ambient tx when present and the pool otherwise. Within one request
// the shard key is stable, so Resolve(key) returns the same pool the open tx was
// started on, and the tx is returned correctly.
func (sp *ShardedPool) FromContext(ctx context.Context) (DBTX, error) {
	key, ok := shardKeyFrom(ctx)
	if !ok {
		return nil, errNoShardKey
	}
	return FromContext(ctx, sp.Resolve(key)), nil
}

// FromContextRead is the reader-pool variant of FromContext.
func (sp *ShardedPool) FromContextRead(ctx context.Context) (DBTX, error) {
	key, ok := shardKeyFrom(ctx)
	if !ok {
		return nil, errNoShardKey
	}
	return FromContextRead(ctx, sp.Resolve(key)), nil
}
