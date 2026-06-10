// Command notifications is the notifications service entry point.
// servicekit.Main handles automaxprocs, signal handling, and teardown.
package main

import (
	"context"

	"go-boilerplate/examples/notifications"
	"go-boilerplate/platform/servicekit"
)

func main() {
	servicekit.Main(func(ctx context.Context) (servicekit.App, error) {
		return notifications.NewApp(ctx)
	})
}
