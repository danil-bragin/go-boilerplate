package attachments

import (
	"context"
	"errors"
	"fmt"

	"go-boilerplate/platform/storage/pg"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	storegen "go-boilerplate/examples/gateway/internal/store/gen"
)

// StoreOwnerLookup returns an OwnerLookup backed by the gateway read model:
// it resolves orders_read.customer_id for the given order id. Wire it when
// constructing the handler:
//
//	attachments.New(store, flags.Bool, attachments.WithOwnerLookup(
//	    attachments.StoreOwnerLookup(pool)))
func StoreOwnerLookup(pool *pg.Pool) OwnerLookup {
	return func(ctx context.Context, orderID string) (string, error) {
		id, err := uuid.Parse(orderID)
		if err != nil {
			return "", ErrOwnerNotFound
		}
		row, err := storegen.New(pg.FromContextRead(ctx, pool)).GetOrderView(ctx, id)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return "", ErrOwnerNotFound
			}
			return "", fmt.Errorf("attachments: owner lookup: %w", err)
		}
		return row.CustomerID, nil
	}
}
