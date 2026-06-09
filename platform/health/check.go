package health

import "context"

// CheckFunc is a plain function type that satisfies the Check function
// signature. It lets callers register dependency health checks without
// importing the concrete dependency package, avoiding import cycles.
//
// Example — register a pg pool check without importing platform/pg:
//
//	h.AddReadiness("postgres", health.CheckFunc(func(ctx context.Context) error {
//	    return pool.HealthCheck(ctx)
//	}))
//
// Example — register a Kafka check:
//
//	h.AddReadiness("kafka", health.CheckFunc(func(ctx context.Context) error {
//	    return kafkaClient.Ping(ctx)
//	}))
type CheckFunc func(ctx context.Context) error

// Check implements the Check function signature by calling f.
func (f CheckFunc) Check(ctx context.Context) error { return f(ctx) }
