// Command gateway is the HTTP edge service: accepts REST orders, publishes
// CreateOrderCommand to Kafka, and serves a read model built from event
// projections. servicekit.Main handles automaxprocs, signal handling, and
// teardown.
package main

import (
	"context"

	"go-boilerplate/examples/gateway"
	"go-boilerplate/platform/servicekit"
)

func main() {
	servicekit.Main(func(ctx context.Context) (servicekit.App, error) {
		return gateway.NewApp(ctx)
	})
}
