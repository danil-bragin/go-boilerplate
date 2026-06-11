package outbox_test

import (
	"context"
	"embed"
	"encoding/json"
	"testing"

	"go-boilerplate/platform/config"
	"go-boilerplate/platform/messaging/msgctx"
	"go-boilerplate/platform/messaging/outbox"
	"go-boilerplate/platform/security/auth"
	"go-boilerplate/platform/storage/pg"
	"go-boilerplate/platform/storage/pg/pgtest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

//go:embed migrations/*.sql
var migrations embed.FS

func newPoolWithSchema(t *testing.T) *pg.Pool {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test requires Docker (postgres container)")
	}
	dsn := pgtest.SharedDSN(t)
	ctx := context.Background()
	require.NoError(t, pg.Migrate(ctx, dsn, migrations, "migrations"))
	pool, err := pg.New(ctx, pg.Config{DSN: config.Secret(dsn)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close(ctx) })
	return pool
}

func TestEnqueue_WritesRowWithinTx(t *testing.T) {
	pool := newPoolWithSchema(t)
	ctx := context.Background()
	repo := outbox.NewRepository(pool)

	id := uuid.New()
	err := pg.RunInTx(ctx, pool, func(ctx context.Context) error {
		return repo.Enqueue(ctx, outbox.Message{
			ID:            id,
			AggregateType: "order",
			AggregateID:   "42",
			EventType:     "OrderCreated",
			Payload:       []byte(`{"id":"42"}`),
		})
	})
	require.NoError(t, err)

	var count int
	require.NoError(t, pool.Reader().QueryRow(
		ctx,
		`select count(*) from outbox where id=$1 and published_at is null`, id,
	).Scan(&count))
	require.Equal(t, 1, count)
}

func TestEnqueue_RolledBackWithFailedTx(t *testing.T) {
	pool := newPoolWithSchema(t)
	ctx := context.Background()
	repo := outbox.NewRepository(pool)

	id := uuid.New()
	_ = pg.RunInTx(ctx, pool, func(ctx context.Context) error {
		_ = repo.Enqueue(ctx, outbox.Message{
			ID: id, AggregateType: "order", AggregateID: "1",
			EventType: "X", Payload: []byte(`{}`),
		})
		return context.Canceled // force rollback
	})

	var count int
	require.NoError(t, pool.Reader().QueryRow(ctx,
		`select count(*) from outbox where id=$1`, id).Scan(&count))
	require.Equal(t, 0, count, "enqueue must roll back with the business tx")
}

func TestEnqueue_StampsCorrelationCausationFromContext(t *testing.T) {
	pool := newPoolWithSchema(t)
	ctx := context.Background()
	repo := outbox.NewRepository(pool)

	readHeaders := func(id uuid.UUID) map[string]string {
		var raw []byte
		require.NoError(t, pool.Reader().QueryRow(ctx,
			`select headers from outbox where id=$1`, id).Scan(&raw))
		var h map[string]string
		require.NoError(t, json.Unmarshal(raw, &h))
		return h
	}

	t.Run("from consumer context", func(t *testing.T) {
		id := uuid.New()
		msgCtx := msgctx.WithParentMessageID(msgctx.WithCorrelationID(ctx, "root-cmd"), "parent-msg")
		require.NoError(t, pg.RunInTx(msgCtx, pool, func(ctx context.Context) error {
			return repo.Enqueue(ctx, outbox.Message{
				ID: id, Topic: "orders.events", AggregateType: "order",
				AggregateID: "1", EventType: "X", Payload: []byte(`{}`),
			})
		}))
		h := readHeaders(id)
		require.Equal(t, "root-cmd", h["correlation-id"], "correlation-id must be stamped from ctx")
		require.Equal(t, "parent-msg", h["causation-id"], "causation-id must be the parent message id")
	})

	t.Run("no chain in ctx: correlation defaults to own id, no causation", func(t *testing.T) {
		id := uuid.New()
		require.NoError(t, pg.RunInTx(ctx, pool, func(ctx context.Context) error {
			return repo.Enqueue(ctx, outbox.Message{
				ID: id, Topic: "orders.events", AggregateType: "order",
				AggregateID: "2", EventType: "X", Payload: []byte(`{}`),
			})
		}))
		h := readHeaders(id)
		require.Equal(t, id.String(), h["correlation-id"], "chain root: correlation = own message id")
		_, hasCausation := h["causation-id"]
		require.False(t, hasCausation, "no parent in ctx → no causation-id header")
	})

	t.Run("explicit headers win over ctx", func(t *testing.T) {
		id := uuid.New()
		msgCtx := msgctx.WithParentMessageID(msgctx.WithCorrelationID(ctx, "ctx-corr"), "ctx-parent")
		require.NoError(t, pg.RunInTx(msgCtx, pool, func(ctx context.Context) error {
			return repo.Enqueue(ctx, outbox.Message{
				ID: id, Topic: "orders.events", AggregateType: "order",
				AggregateID: "3", EventType: "X", Payload: []byte(`{}`),
				Headers: []byte(`{"correlation-id":"explicit-corr","causation-id":"explicit-parent","custom":"v"}`),
			})
		}))
		h := readHeaders(id)
		require.Equal(t, "explicit-corr", h["correlation-id"])
		require.Equal(t, "explicit-parent", h["causation-id"])
		require.Equal(t, "v", h["custom"], "pre-existing custom headers must survive stamping")
	})

	t.Run("stamps principal from ctx (multi-hop attribution)", func(t *testing.T) {
		id := uuid.New()
		principalCtx := auth.Into(ctx, auth.Principal{Subject: "user-42", Roles: []string{"user", "admin"}})
		require.NoError(t, pg.RunInTx(principalCtx, pool, func(ctx context.Context) error {
			return repo.Enqueue(ctx, outbox.Message{
				ID: id, Topic: "orders.events", AggregateType: "order",
				AggregateID: "4", EventType: "X", Payload: []byte(`{}`),
			})
		}))
		h := readHeaders(id)
		require.Equal(t, "user-42", h[auth.HeaderPrincipalSub],
			"principal-sub must be stamped from ctx so downstream audit attributes the real actor")
		require.Equal(t, `["user","admin"]`, h[auth.HeaderPrincipalRoles],
			"principal-roles must be JSON-encoded from ctx")
	})

	t.Run("no principal in ctx: no principal headers", func(t *testing.T) {
		id := uuid.New()
		require.NoError(t, pg.RunInTx(ctx, pool, func(ctx context.Context) error {
			return repo.Enqueue(ctx, outbox.Message{
				ID: id, Topic: "orders.events", AggregateType: "order",
				AggregateID: "5", EventType: "X", Payload: []byte(`{}`),
			})
		}))
		h := readHeaders(id)
		_, hasSub := h[auth.HeaderPrincipalSub]
		require.False(t, hasSub, "no principal in ctx → no principal-sub header")
	})

	t.Run("explicit principal header wins over ctx", func(t *testing.T) {
		id := uuid.New()
		principalCtx := auth.Into(ctx, auth.Principal{Subject: "ctx-user", Roles: []string{"user"}})
		require.NoError(t, pg.RunInTx(principalCtx, pool, func(ctx context.Context) error {
			return repo.Enqueue(ctx, outbox.Message{
				ID: id, Topic: "orders.events", AggregateType: "order",
				AggregateID: "6", EventType: "X", Payload: []byte(`{}`),
				Headers: []byte(`{"principal-sub":"explicit-user"}`),
			})
		}))
		h := readHeaders(id)
		require.Equal(t, "explicit-user", h[auth.HeaderPrincipalSub],
			"an explicit principal-sub must not be overwritten by the ctx principal")
	})
}
