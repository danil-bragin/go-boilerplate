package app

import (
	"context"
	"fmt"

	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/security/audit"
	"go-boilerplate/platform/storage/pg"
)

// RecordProductView is the A2 demo command that proves per-command consistency
// differentiation. It is a SINGLE local insert with NO outbox, so it runs with
// ConsistencyPolicy Eventual.With(Transactional(false)): no wrapping
// transaction (a lone write is already atomic) and audit handed off
// asynchronously (best-effort). It deliberately contrasts with the order flow,
// which is Strong/effectively-once and Kafka-choreographed through the outbox.
type RecordProductView struct {
	ProductID string `validate:"required"`
	Viewer    string `validate:"required"`
}

// RecordProductViewHandler returns the raw (undecorated) handler: one INSERT
// into product_views. No transaction wrap is needed — a single write is atomic,
// and there is no outbox row to keep atomic with it. The caller wraps this with
// DecorateRecordProductView before use.
func RecordProductViewHandler(pool *pg.Pool) cqrs.HandlerFunc[RecordProductView, struct{}] {
	return func(ctx context.Context, c RecordProductView) (struct{}, error) {
		_, err := pg.FromContext(ctx, pool).Exec(ctx,
			`insert into product_views (product_id, viewer) values ($1, $2)`,
			c.ProductID, c.Viewer)
		if err != nil {
			return struct{}{}, fmt.Errorf("app: record product view: %w", err)
		}
		return struct{}{}, nil
	}
}

// DecorateRecordProductView wires the Eventual pipeline for the demo command:
// no transaction (single write, no outbox ⇒ Transactional:false is legitimate,
// not a guard violation) plus best-effort async audit via the buffered writer.
// WithConsistency receives a nil pool because Transactional:false means the
// Transaction behavior is never added — the pool would be unused.
func DecorateRecordProductView(raw cqrs.HandlerFunc[RecordProductView, struct{}], w *audit.BufferedAuditWriter) cqrs.HandlerFunc[RecordProductView, struct{}] {
	policy := cqrs.Eventual.With(cqrs.Transactional(false))
	return cqrs.StandardPipeline[RecordProductView, struct{}]("RecordProductView").
		WithConsistency(policy, nil).
		Use(audit.AsyncAudit[RecordProductView, struct{}](w, "product:view", func(c RecordProductView) string { return c.ProductID })).
		Decorate(raw)
}
