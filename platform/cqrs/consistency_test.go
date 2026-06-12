package cqrs_test

import (
	"testing"

	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/storage/pg"

	"github.com/stretchr/testify/require"
)

func TestConsistencyPolicy_PresetsAndOverrides(t *testing.T) {
	require.Equal(t, cqrs.ConsistencyPolicy{Transactional: true, SyncAudit: true, SyncRYW: true, SyncProjection: true}, cqrs.Strong)
	require.True(t, cqrs.Eventual.Transactional, "Eventual keeps the tx (atomicity) by default")
	require.False(t, cqrs.Eventual.SyncAudit)

	p := cqrs.Strong.With(cqrs.SyncAudit(false))
	require.False(t, p.SyncAudit)
	require.True(t, p.Transactional, "override touches only the named axis")

	e := cqrs.Eventual.With(cqrs.Transactional(false))
	require.False(t, e.Transactional)
}

func TestWithConsistency_GatesTransaction(t *testing.T) {
	pgStub := &pg.Pool{}
	withTx := cqrs.StandardPipeline[int, int]("x").WithConsistency(cqrs.Strong, pgStub).Behaviors()
	noTx := cqrs.StandardPipeline[int, int]("x").WithConsistency(cqrs.Eventual.With(cqrs.Transactional(false)), pgStub).Behaviors()
	require.Greater(t, len(withTx), len(noTx), "Transactional=true contributes the Transaction behavior")
}

func TestWithOutbox_GuardPanicsOnTxOffPlusOutbox(t *testing.T) {
	pgStub := &pg.Pool{}
	require.Panics(t, func() {
		cqrs.StandardPipeline[int, int]("x").
			WithOutbox().
			WithConsistency(cqrs.Eventual.With(cqrs.Transactional(false)), pgStub)
	}, "Transactional:false + outbox must panic — it breaks effectively-once")
}
