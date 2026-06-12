#!/usr/bin/env bash
# new-service.sh — scaffold a new example service from the payments template.
#
# Usage: scripts/new-service.sh <name>        (or: just new-service <name>)
#
# Copies examples/payments → examples/<name>, renames files/dirs, rewrites
# package and service names (payments→<name>, Payments→<Name>), and marks
# every shared-proto reference with a TODO so you replace the demo events
# with your own. The generated service BUILDS as-is (it still consumes the
# demo orders/v1 events) so you can iterate from green.
set -euo pipefail

usage() {
	echo "usage: $0 <name>   # lowercase letters/digits, starts with a letter (e.g. shipping)" >&2
	exit 2
}

name="${1:-}"
[[ -n "$name" ]] || usage
if ! [[ "$name" =~ ^[a-z][a-z0-9]*$ ]]; then
	echo "error: service name must match ^[a-z][a-z0-9]*\$ (got: $name)" >&2
	exit 1
fi
if [[ "$name" == "payments" ]]; then
	echo "error: '$name' is the template itself" >&2
	exit 1
fi

root="$(cd "$(dirname "$0")/.." && pwd)"
src="$root/examples/payments"
dst="$root/examples/$name"

if [[ -e "$dst" ]]; then
	echo "error: $dst already exists — refusing to overwrite" >&2
	exit 1
fi

# Capitalised form for exported identifiers (payments → Payments style).
cap_name="$(tr '[:lower:]' '[:upper:]' <<<"${name:0:1}")${name:1}"

# Cleanup on failure: a mid-run error (interrupted copy, failed rewrite)
# must not leave a half-scaffolded examples/<name> behind — it would both
# break the build and make the next run refuse with "already exists".
# Disarmed once the scaffold completes.
scaffold_done=0
cleanup() {
	if [[ "$scaffold_done" -eq 0 && -e "$dst" ]]; then
		echo "error: scaffold failed — removing partial $dst" >&2
		rm -rf "$dst"
	fi
}
trap cleanup EXIT

cp -R "$src" "$dst"

# Drop the template's cross-service contract tests: they pin the SHOWCASE
# services' wire compatibility against the central examples/e2e/contract
# fixtures (contract.PaymentsEventsTopic, …). A freshly scaffolded service
# has no such fixtures, so these tests would reference undefined symbols
# after the name rewrite. Delete them — the new service writes its own.
find "$dst" -name 'contract_test.go' -delete

# Rename files and directories whose names contain the template name
# (cmd/payments, payments.go, payments_test.go, payments.sql.go, ...).
# -depth so children are renamed before their parents.
find "$dst" -depth -name '*payments*' | while IFS= read -r path; do
	dir="$(dirname "$path")"
	base="$(basename "$path")"
	mv "$path" "$dir/${base//payments/$name}"
done

# Rewrite package names, import paths, topic names, consumer groups, table
# names. Only the plural service-name forms are touched — singular domain
# words ("payment", "Payment") stay; they are the demo domain you replace.
find "$dst" -type f \( -name '*.go' -o -name '*.sql' -o -name '*.yaml' \) -print0 |
	xargs -0 perl -pi -e "s/payments/$name/g; s/Payments/$cap_name/g"

# Flag every shared-proto reference: the new service must define its own
# events instead of reusing the demo orders/v1 protos.
grep -rl 'go-boilerplate/gen/proto/' "$dst" --include='*.go' | while IFS= read -r f; do
	perl -pi -e 's{^(\s*\w*\s*"go-boilerplate/gen/proto/.*)$}{$1 // TODO(new-service): replace demo orders/v1 events with proto/'"$name"'/v1 + `just gen`}' "$f"
done

scaffold_done=1

echo "Scaffolded examples/$name from the payments template."
echo
echo "Manual wiring checklist (the scaffold does NOT touch shared files):"
echo "  [ ] Protos: define proto/$name/v1 events, run 'just gen', replace the"
echo "      orders/v1 references (grep 'TODO(new-service)' in examples/$name)"
echo "  [ ] .github/workflows/ci.yml: add '$name' to the build-images matrix"
echo "      (jobs.build-images.strategy.matrix.service)"
echo "  [ ] docker-compose.yml: add a '$name' service block under the 'apps'"
echo "      profile (copy the payments block; new PG db + env vars)"
echo "  [ ] Env: PG_DSN database for '$name', KAFKA_BROKERS, topic names"
echo "  [ ] justfile: add '$name' to the (hardcoded) build-images recipe"
echo "  [ ] cmd/migrate/main.go: register ${name}.Migrations in the services map"
echo
echo "Verify: go build ./examples/$name/... && go vet ./examples/$name/..."
