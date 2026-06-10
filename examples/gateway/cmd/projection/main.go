// Command projection runs the gateway read-model projection as a standalone
// consumer-only service (no public HTTP — admin server only). Deploy it with
// GATEWAY_EMBEDDED_PROJECTION=false on the gateway to split the edge from the
// read-model builder; both modes share the "gateway-projection" consumer
// group and inbox, so the handover is safe.
// servicekit.Main handles automaxprocs, signal handling, and teardown.
package main

import (
	"context"

	"go-boilerplate/examples/gateway"
	"go-boilerplate/platform/servicekit"
)

func main() {
	servicekit.Main(func(ctx context.Context) (servicekit.App, error) {
		return gateway.NewProjectionApp(ctx)
	})
}
