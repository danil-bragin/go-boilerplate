#!/usr/bin/env bash
# test-scaffold.sh — CI smoke test for the scaffolding scripts.
#
#   1. new-service.sh footest → generated service must build and vet clean,
#      and the script must refuse to overwrite an existing service.
#   2. rename-module.sh --check → dry run must produce a non-empty plan that
#      covers go.mod, and must not modify anything.
#
# Run via `just test-scaffold` or directly in CI. Always cleans up footest.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
svc="footest"

if [[ -e "$root/examples/$svc" ]]; then
	echo "error: examples/$svc already exists — remove it before running the smoke test" >&2
	exit 1
fi
cleanup() { rm -rf "${root:?}/examples/$svc"; }
trap cleanup EXIT

echo "── new-service.sh $svc ──"
"$root/scripts/new-service.sh" "$svc"

echo "── go build + vet generated service ──"
(cd "$root" && go build "./examples/$svc/..." && go vet "./examples/$svc/...")

echo "── idempotency: second run must refuse ──"
if "$root/scripts/new-service.sh" "$svc" >/dev/null 2>&1; then
	echo "FAIL: new-service.sh overwrote an existing service" >&2
	exit 1
fi

echo "── rename-module.sh --check ──"
before="$(cksum "$root/go.mod")"
plan="$("$root/scripts/rename-module.sh" --check github.com/acme/scaffold-smoke)"
after="$(cksum "$root/go.mod")"
echo "$plan"
if [[ -z "$plan" ]]; then
	echo "FAIL: rename-module --check produced an empty plan" >&2
	exit 1
fi
if ! grep -q "go.mod" <<<"$plan"; then
	echo "FAIL: rename-module --check plan does not cover go.mod" >&2
	exit 1
fi
if [[ "$before" != "$after" ]]; then
	echo "FAIL: rename-module --check modified go.mod (dry run must not write)" >&2
	exit 1
fi

echo "scaffold smoke test OK"
