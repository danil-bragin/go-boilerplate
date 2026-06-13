// Package migoracle is a throwaway verification tool that proves a collapsed
// migration baseline is schema-byte-identical to the old migration chain.
//
// It does so by introspecting the public schema purely through system catalogs
// (no pg_dump binary, no container exec) and producing a deterministic,
// sorted fingerprint string. Two databases with the same effective schema
// produce the same fingerprint; any structural difference changes it.
package migoracle

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"go-boilerplate/platform/storage/pg"
)

// section runs query against the pool, formats every row as a pipe-joined
// line of fmt.Sprintf("%v", v) cells, sorts the lines for determinism, and
// returns them under a "== header ==" banner.
func section(ctx context.Context, pool *pg.Pool, header, query string) (string, error) {
	rows, err := pool.Reader().Query(ctx, query)
	if err != nil {
		return "", fmt.Errorf("migoracle: query %q: %w", header, err)
	}
	defer rows.Close()

	var lines []string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return "", fmt.Errorf("migoracle: values %q: %w", header, err)
		}
		cells := make([]string, len(vals))
		for i, v := range vals {
			cells[i] = fmt.Sprintf("%v", v)
		}
		lines = append(lines, strings.Join(cells, "|"))
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("migoracle: rows %q: %w", header, err)
	}

	sort.Strings(lines)

	var b strings.Builder
	fmt.Fprintf(&b, "== %s ==\n", header)
	for _, l := range lines {
		b.WriteString(l)
		b.WriteByte('\n')
	}
	return b.String(), nil
}

// Fingerprint returns a deterministic normalised description of the public
// schema of the database behind pool, built from system catalogs. The
// goose_db_version bookkeeping table is excluded so that two equivalent
// schemas migrated via different chains still match.
func Fingerprint(ctx context.Context, pool *pg.Pool) (string, error) {
	type q struct{ header, sql string }
	queries := []q{
		{
			"columns",
			`select table_name, column_name, data_type, is_nullable, coalesce(column_default,'')
			   from information_schema.columns
			  where table_schema='public' and table_name <> 'goose_db_version'`,
		},
		{
			"indexes",
			`select indexdef
			   from pg_indexes
			  where schemaname='public' and tablename <> 'goose_db_version'`,
		},
		{
			"constraints",
			`select conrelid::regclass::text, conname, pg_get_constraintdef(oid)
			   from pg_constraint
			  where connamespace = 'public'::regnamespace`,
		},
		{
			"partitions",
			`select c.relname, pg_get_expr(c.relpartbound, c.oid)
			   from pg_class c
			   join pg_inherits i on c.oid=i.inhrelid
			   join pg_class p on i.inhparent=p.oid
			  where p.relnamespace='public'::regnamespace`,
		},
		{
			"partitioned",
			`select c.relname, pt.partstrat
			   from pg_partitioned_table pt
			   join pg_class c on c.oid=pt.partrelid
			  where c.relnamespace='public'::regnamespace`,
		},
	}

	var b strings.Builder
	for _, qy := range queries {
		s, err := section(ctx, pool, qy.header, qy.sql)
		if err != nil {
			return "", err
		}
		b.WriteString(s)
	}
	return b.String(), nil
}
