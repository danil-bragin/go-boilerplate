package pg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool holds the writer pool and (optionally distinct) reader pool.
// When no ReaderDSN is configured, Reader() returns the writer pool.
type Pool struct {
	writer *pgxpool.Pool
	reader *pgxpool.Pool
}

// New builds writer (and optional reader) pools and verifies connectivity.
func New(ctx context.Context, cfg Config) (*Pool, error) {
	wc, err := cfg.buildPoolConfig(cfg.DSN)
	if err != nil {
		return nil, err
	}
	writer, err := pgxpool.NewWithConfig(ctx, wc)
	if err != nil {
		return nil, fmt.Errorf("pg: new writer pool: %w", err)
	}
	if err := writer.Ping(ctx); err != nil {
		writer.Close()
		return nil, fmt.Errorf("pg: ping writer: %w", err)
	}

	p := &Pool{writer: writer, reader: writer}

	if cfg.ReaderDSN != "" {
		rc, err := cfg.buildPoolConfig(cfg.ReaderDSN)
		if err != nil {
			writer.Close()
			return nil, err
		}
		reader, err := pgxpool.NewWithConfig(ctx, rc)
		if err != nil {
			writer.Close()
			return nil, fmt.Errorf("pg: new reader pool: %w", err)
		}
		if err := reader.Ping(ctx); err != nil {
			writer.Close()
			reader.Close()
			return nil, fmt.Errorf("pg: ping reader: %w", err)
		}
		p.reader = reader
	}
	return p, nil
}

// Writer returns the writer pool (use for writes and read-your-writes).
func (p *Pool) Writer() *pgxpool.Pool { return p.writer }

// Reader returns the reader pool (the writer pool if no replica configured).
func (p *Pool) Reader() *pgxpool.Pool { return p.reader }

// HealthCheck pings the writer (and reader, if distinct). Suitable for
// registering with platform/health.AddReadiness.
func (p *Pool) HealthCheck(ctx context.Context) error {
	if err := p.writer.Ping(ctx); err != nil {
		return fmt.Errorf("pg: writer unhealthy: %w", err)
	}
	if p.reader != p.writer {
		if err := p.reader.Ping(ctx); err != nil {
			return fmt.Errorf("pg: reader unhealthy: %w", err)
		}
	}
	return nil
}

// Close closes both pools. Safe to register with run.Closer.
func (p *Pool) Close(_ context.Context) error {
	if p.reader != p.writer && p.reader != nil {
		p.reader.Close()
	}
	if p.writer != nil {
		p.writer.Close()
	}
	return nil
}
