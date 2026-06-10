#!/usr/bin/env bash
# rename-module.sh — rename the Go module path across the whole repository.
#
# Usage: scripts/rename-module.sh [--check] <new-module-path>
#        scripts/rename-module.sh github.com/acme/myapp
#        scripts/rename-module.sh --check github.com/acme/myapp   # dry run
#
# Rewrites:
#   - go.mod module directive
#   - every Go import of "go-boilerplate/..." (anchored on the opening quote)
#   - goimports -local prefix in lefthook.yml and justfile
#   - .golangci.yml gofumpt module-path + goimports local-prefixes
#   - justfile Docker image prefix (go-boilerplate/<svc> → <basename>/<svc>)
# then runs `go build ./...` to prove the rename is complete.
#
# --check prints the planned changes (file: occurrence count) and modifies
# nothing; it exits non-zero if there is nothing to rename.
set -euo pipefail

old="go-boilerplate"
check=0
new=""

for arg in "$@"; do
	case "$arg" in
	--check) check=1 ;;
	-*)
		echo "error: unknown flag $arg" >&2
		exit 2
		;;
	*) new="$arg" ;;
	esac
done

if [[ -z "$new" ]]; then
	echo "usage: $0 [--check] <new-module-path>   (e.g. github.com/acme/myapp)" >&2
	exit 2
fi
if [[ "$new" == "$old" ]]; then
	echo "error: new module path equals the current one ($old)" >&2
	exit 1
fi
if [[ "$new" =~ [[:space:]\"\'] ]]; then
	echo "error: module path must not contain whitespace or quotes" >&2
	exit 1
fi

root="$(cd "$(dirname "$0")/.." && pwd)"
newbase="$(basename "$new")"

total=0
plan() { # plan <file-rel> <count> <description>
	local n="$2"
	[[ "$n" -gt 0 ]] || return 0
	total=$((total + n))
	printf 'plan: %-40s %4d  %s\n' "$1" "$n" "$3"
}

# ── 1. go.mod module directive ────────────────────────────────────────────
gomod_n="$(grep -c "^module $old\$" "$root/go.mod" || true)"
plan "go.mod" "$gomod_n" "module $old → module $new"

# ── 2. Go imports (anchored: \"go-boilerplate/…\") ─────────────────────────
go_files=()
while IFS= read -r f; do go_files+=("$f"); done \
	< <(grep -rl "\"$old/" "$root" --include='*.go' --exclude-dir=.git 2>/dev/null || true)
import_n=0
for f in "${go_files[@]:-}"; do
	[[ -n "$f" ]] || continue
	n="$(grep -o "\"$old/" "$f" | wc -l | tr -d ' ')"
	import_n=$((import_n + n))
done
plan "${#go_files[@]} Go files" "$import_n" "\"$old/… → \"$new/…"

# ── 3. lefthook.yml goimports -local ──────────────────────────────────────
lefthook_n="$(grep -c -- "-local $old" "$root/lefthook.yml" || true)"
plan "lefthook.yml" "$lefthook_n" "goimports -local $old → -local $new"

# ── 4. .golangci.yml gofumpt module-path + goimports local-prefixes ──────
golangci_n=0
golangci_n=$((golangci_n + $(grep -c "module-path: $old\$" "$root/.golangci.yml" || true)))
golangci_n=$((golangci_n + $(grep -c "^[[:space:]]*- $old\$" "$root/.golangci.yml" || true)))
plan ".golangci.yml" "$golangci_n" "module-path / local-prefixes → $new"

# ── 5. justfile: goimports -local + docker image prefix ──────────────────
just_local_n="$(grep -c -- "-local $old" "$root/justfile" || true)"
just_image_n="$(grep -c -- "-t $old/" "$root/justfile" || true)"
plan "justfile" "$((just_local_n + just_image_n))" \
	"-local → $new; image prefix $old/ → $newbase/"

if [[ "$total" -eq 0 ]]; then
	echo "nothing to rename: no occurrences of '$old' found" >&2
	exit 1
fi

echo "total: $total change(s) → module '$new'"

if [[ "$check" -eq 1 ]]; then
	echo "(dry run — nothing modified)"
	exit 0
fi

# ── apply ─────────────────────────────────────────────────────────────────
perl -pi -e "s{^module \Q$old\E\$}{module $new}" "$root/go.mod"
for f in "${go_files[@]:-}"; do
	[[ -n "$f" ]] || continue
	perl -pi -e "s{\"\Q$old\E/}{\"$new/}g" "$f"
done
perl -pi -e "s{-local \Q$old\E(\s)}{-local $new\$1}g" "$root/lefthook.yml"
perl -pi -e "s{module-path: \Q$old\E\$}{module-path: $new}" "$root/.golangci.yml"
perl -pi -e "s{^(\s*)- \Q$old\E\$}{\$1- $new}" "$root/.golangci.yml"
perl -pi -e "s{-local \Q$old\E(\s)}{-local $new\$1}g; s{-t \Q$old\E/}{-t $newbase/}g" "$root/justfile"

echo "applied; verifying with go build ./... ..."
(cd "$root" && go build ./...)
echo "rename complete: $old → $new"
echo "note: grep for remaining cosmetic mentions: grep -rn '$old' --exclude-dir=.git ."
