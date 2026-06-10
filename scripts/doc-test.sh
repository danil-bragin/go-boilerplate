#!/usr/bin/env bash
# doc-test.sh — keep docs/adding-a-service.md honest.
#
# Extracts every fenced Go code block from the guide and verifies it against
# the real codebase:
#
#   ```go            → the block must be a complete file: it is written into
#                      a throwaway package directory inside the module and
#                      `go build` MUST succeed (real imports, real signatures).
#   ```go nocompile  → the block references files the guide has not created
#                      (e.g. the scaffolded service's own package), so it is
#                      only checked for valid Go syntax via `gofmt`.
#
# Each compiled block gets a stub sql/00001_init.sql so `//go:embed sql`
# examples build. Run via `just doc-test`; blocking in CI.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
doc="$root/docs/adding-a-service.md"
[[ -f "$doc" ]] || { echo "doc-test: $doc not found" >&2; exit 1; }

# Workdir must live INSIDE the module so go-boilerplate/... imports resolve.
work="$root/.doctest-tmp"
rm -rf "$work"
mkdir -p "$work"
cleanup() { rm -rf "$work"; }
trap cleanup EXIT

# Split the markdown into per-block files: block_<n>.go (compile) and
# block_<n>.go.nocompile (parse only).
awk -v dir="$work" '
	/^```go nocompile[[:space:]]*$/ { mode = "nocompile"; n++; file = dir "/block_" n ".go.nocompile"; next }
	/^```go[[:space:]]*$/           { mode = "compile";   n++; file = dir "/block_" n ".go";           next }
	/^```/                          { mode = ""; next }
	mode != ""                      { print > file }
' "$doc"

shopt -s nullglob
compile_blocks=("$work"/block_*.go)
parse_blocks=("$work"/block_*.go.nocompile)

if (( ${#compile_blocks[@]} == 0 && ${#parse_blocks[@]} == 0 )); then
	echo "doc-test: no \`\`\`go blocks found in $doc — the guide lost its examples?" >&2
	exit 1
fi

fail=0

# Parse-only blocks: gofmt exits non-zero on any syntax error. Fragments
# that are not complete files (no package clause) are wrapped in a throwaway
# package+func so statement-level snippets still get syntax-checked.
for f in "${parse_blocks[@]}"; do
	if ! head -5 "$f" | grep -q '^package '; then
		wrapped="$f.wrapped.go"
		{ echo "package docfragment"; echo "func _() {"; cat "$f"; echo "}"; } > "$wrapped"
		mv "$wrapped" "$f"
	fi
	if ! gofmt -e "$f" >/dev/null; then
		echo "doc-test: PARSE FAIL: $(basename "$f") (see block in $doc)" >&2
		fail=1
	fi
done

# Compile blocks: each in its own package dir (package names may repeat).
for f in "${compile_blocks[@]}"; do
	name="$(basename "$f" .go)"
	pkgdir="$work/$name"
	mkdir -p "$pkgdir/sql"
	echo "-- +goose Up
select 1;
-- +goose Down
select 1;" > "$pkgdir/sql/00001_init.sql"
	mv "$f" "$pkgdir/doc.go"
	if ! (cd "$root" && go build "./.doctest-tmp/$name/") ; then
		echo "doc-test: COMPILE FAIL: $name (see block in $doc)" >&2
		fail=1
	fi
done

if (( fail )); then
	echo "doc-test: docs/adding-a-service.md has drifted from the code" >&2
	exit 1
fi
echo "doc-test OK: ${#compile_blocks[@]} block(s) compiled, ${#parse_blocks[@]} parse-checked"
