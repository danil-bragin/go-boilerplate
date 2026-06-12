package outbox

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"go-boilerplate/platform/clock"
	"go-boilerplate/platform/storage/pg"
)

// PartitionMode selects outbox retention behaviour (ADR-0016).
type PartitionMode string

const (
	// ModeSimple is the default: every row lands in the DEFAULT partition and
	// retention is age-based DELETE (Cleaner). No maintenance worker runs.
	ModeSimple PartitionMode = "simple"
	// ModePartitioned enables time-range partition maintenance: the worker
	// pre-creates future partitions and DETACH+DROPs expired ones.
	ModePartitioned PartitionMode = "partitioned"
)

// PartitionConfig configures the opt-in outbox partition maintenance worker.
// All durations are wall-clock; Mode gates whether the worker is wired at all
// (see servicekit.AddOutboxPartitionMaintenance).
type PartitionConfig struct {
	Mode      PartitionMode `env:"OUTBOX_PARTITION_MODE"      envDefault:"simple"`
	Interval  time.Duration `env:"OUTBOX_PARTITION_INTERVAL"  envDefault:"720h"`  // ~1 month
	Retention time.Duration `env:"OUTBOX_PARTITION_RETENTION" envDefault:"2160h"` // ~3 months
	Lookahead int           `env:"OUTBOX_PARTITION_LOOKAHEAD" envDefault:"2"`     // future partitions to pre-create
}

// partitionNameRe matches partition names this manager generates. DropExpired
// only ever DETACH/DROPs names that match it, so a name parsed back from the
// catalog can be safely interpolated into DDL (no injection surface) and the
// DEFAULT partition (outbox_default) is structurally excluded.
var partitionNameRe = regexp.MustCompile(`^outbox_p\d{8}t\d{6}$`)

const partitionTimeLayout = "20060102t150405"

// PartitionManager creates and prunes outbox range partitions. It is intended
// to run as a SINGLE-ACTIVE periodic worker (leader-elected), so no two
// instances issue partition DDL concurrently.
type PartitionManager struct {
	pool    *pg.Pool
	cfg     PartitionConfig
	clk     clock.Clock
	metrics partitionMetrics
	onError func(error)
}

// NewPartitionManager returns a manager for cfg. A zero/garbage Interval or
// Lookahead is clamped to a safe default so a misconfigured knob can never
// disable pre-creation and starve inserts into the DEFAULT partition.
func NewPartitionManager(pool *pg.Pool, cfg PartitionConfig, opts ...PartitionOption) *PartitionManager {
	if cfg.Interval <= 0 {
		cfg.Interval = 720 * time.Hour
	}
	if cfg.Lookahead < 1 {
		cfg.Lookahead = 1
	}
	pm := &PartitionManager{pool: pool, cfg: cfg, clk: clock.System{}, metrics: newPartitionMetrics()}
	for _, o := range opts {
		o(pm)
	}
	return pm
}

// PartitionOption configures a PartitionManager.
type PartitionOption func(*PartitionManager)

// WithClock injects a clock (tests advance time to exercise rotation).
func WithClock(c clock.Clock) PartitionOption {
	return func(pm *PartitionManager) {
		if c != nil {
			pm.clk = c
		}
	}
}

// WithPartitionOnError registers a callback for non-fatal maintenance errors.
func WithPartitionOnError(fn func(error)) PartitionOption {
	return func(pm *PartitionManager) { pm.onError = fn }
}

// bounds returns the [lo, hi) partition window containing t, aligned to the
// configured interval against the Unix epoch so windows are contiguous and
// deterministic across processes.
func (pm *PartitionManager) bounds(t time.Time) (lo, hi time.Time) {
	lo = t.UTC().Truncate(pm.cfg.Interval)
	return lo, lo.Add(pm.cfg.Interval)
}

func partitionName(lo time.Time) string {
	return "outbox_p" + lo.UTC().Format(partitionTimeLayout)
}

// Maintain runs one maintenance cycle: ensure future partitions exist, then
// drop expired ones. It is the func wired into the periodic worker. Individual
// errors are reported via the OnError hook and folded into the returned error
// (joined) so the worker's own logging still sees them, but ensure/drop are
// independent — a drop failure never blocks pre-creation.
func (pm *PartitionManager) Maintain(ctx context.Context) error {
	start := pm.clk.Now()
	errEnsure := pm.EnsurePartitions(ctx, start)
	errDrop := pm.DropExpired(ctx, start)
	pm.metrics.recordRun(ctx, pm.clk.Now().Sub(start))
	if err := errors.Join(errEnsure, errDrop); err != nil {
		if pm.onError != nil {
			pm.onError(err)
		}
		return err
	}
	if err := pm.observe(ctx); err != nil && pm.onError != nil {
		pm.onError(err)
	}
	return nil
}

// EnsurePartitions creates the partition containing now plus Lookahead future
// partitions, idempotently (CREATE TABLE IF NOT EXISTS). Inserts therefore
// always find a target partition ahead of the DEFAULT one.
func (pm *PartitionManager) EnsurePartitions(ctx context.Context, now time.Time) error {
	db := pm.pool.Writer()
	for i := 0; i <= pm.cfg.Lookahead; i++ {
		lo, hi := pm.bounds(now.Add(time.Duration(i) * pm.cfg.Interval))
		name := partitionName(lo)
		// Names are generated here and validated by regex on the drop path; the
		// timestamp literals are formatted from time.Time, not user input.
		stmt := fmt.Sprintf(
			`create table if not exists %s partition of outbox for values from ('%s') to ('%s')`,
			name, lo.Format(time.RFC3339Nano), hi.Format(time.RFC3339Nano),
		)
		if _, err := db.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("outbox: ensure partition %s: %w", name, err)
		}
		pm.metrics.addCreated(ctx)
	}
	return nil
}

// DropExpired DETACH+DROPs every generated partition whose whole range is older
// than Retention — UNLESS it still holds an unpublished row, in which case it is
// skipped (and counted) so a lagging relay can never cause silent event loss.
// The DEFAULT partition is never dropped.
func (pm *PartitionManager) DropExpired(ctx context.Context, now time.Time) error {
	if pm.cfg.Retention <= 0 {
		return nil // retention disabled
	}
	cutoff := now.UTC().Add(-pm.cfg.Retention)
	names, err := pm.listPartitions(ctx)
	if err != nil {
		return err
	}
	db := pm.pool.Writer()
	var errs []error
	for _, name := range names {
		lo, ok := parsePartitionLow(name)
		if !ok {
			continue // DEFAULT or foreign naming — leave alone
		}
		hi := lo.Add(pm.cfg.Interval)
		if hi.After(cutoff) {
			continue // not yet fully expired
		}
		// Safety invariant: never drop a partition with unpublished rows.
		var hasUnpublished bool
		if err := db.QueryRow(
			ctx,
			fmt.Sprintf(`select exists(select 1 from %s where published_at is null)`, name),
		).Scan(&hasUnpublished); err != nil {
			errs = append(errs, fmt.Errorf("outbox: probe %s: %w", name, err))
			continue
		}
		if hasUnpublished {
			pm.metrics.addSkipped(ctx)
			if pm.onError != nil {
				pm.onError(fmt.Errorf("outbox: partition %s past retention but holds unpublished rows — skipping drop", name))
			}
			continue
		}
		if _, err := db.Exec(ctx, "alter table outbox detach partition "+name); err != nil {
			errs = append(errs, fmt.Errorf("outbox: detach %s: %w", name, err))
			continue
		}
		if _, err := db.Exec(ctx, "drop table "+name); err != nil {
			errs = append(errs, fmt.Errorf("outbox: drop %s: %w", name, err))
			continue
		}
		pm.metrics.addDropped(ctx)
	}
	return errors.Join(errs...)
}

// listPartitions returns the relnames of outbox's child partitions (including
// the DEFAULT partition; callers filter via parsePartitionLow).
func (pm *PartitionManager) listPartitions(ctx context.Context) ([]string, error) {
	rows, err := pm.pool.Reader().Query(ctx, `
		select c.relname
		from pg_inherits i
		join pg_class c on c.oid = i.inhrelid
		join pg_class p on p.oid = i.inhparent
		where p.relname = 'outbox'
		order by c.relname`)
	if err != nil {
		return nil, fmt.Errorf("outbox: list partitions: %w", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, fmt.Errorf("outbox: scan partition: %w", err)
		}
		names = append(names, n)
	}
	return names, rows.Err()
}

// parsePartitionLow recovers the lower bound encoded in a generated partition
// name. It returns ok=false for any name that is not one we generate (the
// DEFAULT partition, or a partition created by some other tool), so those are
// never dropped.
func parsePartitionLow(name string) (time.Time, bool) {
	if !partitionNameRe.MatchString(name) {
		return time.Time{}, false
	}
	t, err := time.ParseInLocation(partitionTimeLayout, name[len("outbox_p"):], time.UTC)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// observe records the current partition-count / bound gauges.
func (pm *PartitionManager) observe(ctx context.Context) error {
	names, err := pm.listPartitions(ctx)
	if err != nil {
		return err
	}
	var (
		ranged           int
		oldest, newest   time.Time
		haveOldestNewest bool
	)
	for _, n := range names {
		lo, ok := parsePartitionLow(n)
		if !ok {
			continue
		}
		ranged++
		if !haveOldestNewest || lo.Before(oldest) {
			oldest = lo
		}
		if !haveOldestNewest || lo.After(newest) {
			newest = lo
		}
		haveOldestNewest = true
	}
	pm.metrics.recordCount(ctx, int64(ranged))
	if haveOldestNewest {
		pm.metrics.recordBounds(ctx, oldest, newest.Add(pm.cfg.Interval))
	}
	return nil
}
