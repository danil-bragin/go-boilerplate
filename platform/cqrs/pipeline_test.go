package cqrs_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go-boilerplate/platform/cqrs"
	"go-boilerplate/platform/security/auth"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

// pipeQuery is a struct command so the Validation behavior exercises
// struct-tag validation.
type pipeQuery struct {
	ID string `validate:"required"`
}

// pipeCache is a minimal in-memory cqrs.Cache for pipeline tests.
type pipeCache struct {
	mu    sync.Mutex
	data  map[string][]byte
	loads int
}

func newPipeCache() *pipeCache { return &pipeCache{data: map[string][]byte{}} }

func (c *pipeCache) Get(_ context.Context, key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.data[key]
	return v, ok
}

func (c *pipeCache) Set(_ context.Context, key string, value []byte, _ time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = value
}

func (c *pipeCache) Delete(_ context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.data, key)
	return nil
}

func (c *pipeCache) GetOrLoad(
	ctx context.Context, key string, ttl time.Duration,
	load func(ctx context.Context) ([]byte, error),
) ([]byte, error) {
	if v, ok := c.Get(ctx, key); ok {
		return v, nil
	}
	c.mu.Lock()
	c.loads++
	c.mu.Unlock()
	v, err := load(ctx)
	if err != nil {
		return nil, err
	}
	c.Set(ctx, key, v, ttl)
	return v, nil
}

// TestStandardPipeline_CoreStack: the pipeline applies Tracing (outermost),
// Logging, Metrics, and Validation. Proven behaviorally:
//   - a Use()'d probe behavior observes an active span in ctx (Tracing wraps it),
//   - an invalid command is rejected by Validation BEFORE the probe runs.
func TestStandardPipeline_CoreStack(t *testing.T) {
	exporter := withTestTracer(t)

	probeRan := false
	var probeSawSpan bool
	probe := func(next cqrs.HandlerFunc[pipeQuery, string]) cqrs.HandlerFunc[pipeQuery, string] {
		return func(ctx context.Context, q pipeQuery) (string, error) {
			probeRan = true
			probeSawSpan = trace.SpanFromContext(ctx).SpanContext().IsValid()
			return next(ctx, q)
		}
	}

	h := cqrs.StandardPipeline[pipeQuery, string]("PipeTest").
		Use(probe).
		Decorate(func(_ context.Context, q pipeQuery) (string, error) {
			return "ok:" + q.ID, nil
		})

	// Valid command: full stack runs, probe sees the Tracing span.
	res, err := h(context.Background(), pipeQuery{ID: "1"})
	require.NoError(t, err)
	assert.Equal(t, "ok:1", res)
	assert.True(t, probeRan)
	assert.True(t, probeSawSpan, "Tracing must be OUTERMOST so Use()'d behaviors run inside the span")

	spans := exporter.GetSpans()
	require.NotEmpty(t, spans)
	assert.Equal(t, "PipeTest", spans[0].Name)

	// Invalid command: Validation rejects before the probe runs.
	probeRan = false
	_, err = h(context.Background(), pipeQuery{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "validation")
	assert.False(t, probeRan, "Validation must run BEFORE domain behaviors")
}

// TestStandardPipeline_WithDeadline: the deadline option installs a ctx
// deadline visible to the handler and surfaces ErrDeadlineExceeded.
func TestStandardPipeline_WithDeadline(t *testing.T) {
	withTestTracer(t)

	h := cqrs.StandardPipeline[pipeQuery, string]("PipeDeadline").
		WithDeadline(15 * time.Millisecond).
		Decorate(func(ctx context.Context, _ pipeQuery) (string, error) {
			<-ctx.Done()
			return "", ctx.Err()
		})

	_, err := h(context.Background(), pipeQuery{ID: "1"})
	require.Error(t, err)
	assert.ErrorIs(t, err, cqrs.ErrDeadlineExceeded)
}

// TestStandardPipeline_WithCache: the cache option short-circuits the second
// identical query (handler called once).
func TestStandardPipeline_WithCache(t *testing.T) {
	withTestTracer(t)

	cache := newPipeCache()
	calls := 0

	h := cqrs.StandardPipeline[pipeQuery, string]("PipeCache").
		WithCache(cache, func(q pipeQuery) string { return "k:" + q.ID }, time.Minute).
		Decorate(func(_ context.Context, q pipeQuery) (string, error) {
			calls++
			return "v:" + q.ID, nil
		})

	for range 2 {
		res, err := h(context.Background(), pipeQuery{ID: "7"})
		require.NoError(t, err)
		assert.Equal(t, "v:7", res)
	}
	assert.Equal(t, 1, calls, "second call must be served from cache")
}

// recordingPolicy records Authorize calls and returns a configured error.
type recordingPolicy struct {
	action   string
	resource any
	sub      string
	err      error
}

func (p *recordingPolicy) Authorize(_ context.Context, pr auth.Principal, action string, resource any) error {
	p.action = action
	p.resource = resource
	p.sub = pr.Subject
	return p.err
}

// TestStandardPipeline_WithAuthz covers: no principal → ErrUnauthenticated;
// policy denial propagates; allowed call passes the command as the resource.
func TestStandardPipeline_WithAuthz(t *testing.T) {
	withTestTracer(t)

	policy := &recordingPolicy{}
	handlerRan := false

	h := cqrs.StandardPipeline[pipeQuery, string]("PipeAuthz").
		WithAuthz(policy, "order:read").
		Decorate(func(_ context.Context, _ pipeQuery) (string, error) {
			handlerRan = true
			return "ok", nil
		})

	// No principal in ctx → ErrUnauthenticated, handler never runs.
	_, err := h(context.Background(), pipeQuery{ID: "1"})
	require.ErrorIs(t, err, cqrs.ErrUnauthenticated)
	assert.False(t, handlerRan)

	// Principal present, policy allows → handler runs, resource = command.
	ctx := auth.Into(context.Background(), auth.Principal{Subject: "alice"})
	res, err := h(ctx, pipeQuery{ID: "1"})
	require.NoError(t, err)
	assert.Equal(t, "ok", res)
	assert.True(t, handlerRan)
	assert.Equal(t, "order:read", policy.action)
	assert.Equal(t, "alice", policy.sub)
	assert.Equal(t, pipeQuery{ID: "1"}, policy.resource)

	// Policy denies → its error propagates.
	denied := errors.New("nope")
	policy.err = denied
	_, err = h(ctx, pipeQuery{ID: "1"})
	require.ErrorIs(t, err, denied)
}
