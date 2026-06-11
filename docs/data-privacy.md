# Data privacy & the right to erasure

Patterns — not shipped code — for honoring deletion requests (GDPR art. 17,
CCPA) in this architecture: immutable-ish Kafka topics, an outbox that
re-publishes state, per-service Postgres, and a gateway read-model projection.
The tension is always the same: **the system is designed to remember; privacy
law requires it to forget.** The patterns below resolve that without breaking
effectively-once delivery or replayability.

## Where personal data lives

Inventory first — a deletion request must enumerate every copy:

| Location | Example here | Erasable in place? |
|---|---|---|
| Service-of-record tables | `orders.orders.customer_id` | Yes (UPDATE/DELETE) |
| Projections | gateway `order_views` rows | Yes (DELETE by `customer_id`) |
| Kafka topics | `orders.events`, retry tiers, DLTs | **No** — append-only; wait for retention, compaction tombstone, or crypto-shred |
| Outbox/inbox/audit tables | serialized event payloads, audit `actor` | Yes, but payloads need targeted scrubbing |
| Backups & DLQ exports | pg_dump archives, redrive dumps | No — only ages out; crypto-shredding is the practical answer |
| Logs/traces | should contain IDs only — keep PII out of logs by convention (`config.Secret`, no payload logging) | Prevent, don't clean |

## Pattern 1 — crypto-shredding (per-subject key)

Encrypt every PII field with a **data key unique to the subject** (customer),
kept in a small key table owned by one service:

- `subject_keys(subject_id, key_ciphertext, created_at, destroyed_at)` — data
  keys themselves wrapped by a KMS master key.
- Producers encrypt PII fields (name, address, email) with the subject's key
  before the event hits the outbox; non-PII fields (amounts, status, ids) stay
  plaintext so analytics and choreography keep working.
- Consumers/projections decrypt on read (or store the ciphertext as-is when
  they don't need the cleartext).

**Delete = destroy the key.** Once `subject_keys` drops the key (and the KMS
copy), every ciphertext copy — Kafka segments, backups, DLT exports, retry
tiers — becomes unreadable garbage simultaneously. No topic rewrite, no
backup surgery. This is the only pattern that covers *backups* without
restoring-and-scrubbing them.

Costs, stated honestly: key-service availability sits on the hot path of every
producer/consumer that touches PII; per-subject keys must be cached carefully
(a destroyed key must drop from caches within your erasure SLO); encrypted
fields can't be queried/indexed server-side. Apply it to genuinely personal
fields only — `customer_id` as an opaque UUID is usually a *pseudonym*, not
PII, and can stay plaintext as the join key.

## Pattern 2 — delete = forget (an event like any other)

Erasure is a domain fact, so model it as one: a `CustomerForgotten` event on
the customer-owning service's topic. Every consumer that materialized PII
subscribes and reacts:

- service-of-record: null/scrub PII columns (keep the row skeleton if
  referential integrity or financial-retention law requires it);
- projections: `DELETE FROM order_views WHERE customer_id = $1` — same
  inbox-dedup path as every other event, so it is effectively-once and
  replayable;
- key service: destroy the subject key (pattern 1) **after** a grace window —
  in-flight events still need decryption to be processed into their scrubbed
  form;
- audit: record that erasure happened (the *fact* of deletion is not PII).

Because forgetting is itself an event, a projection rebuilt from history
re-applies the forgetting at the end of replay — replay safety falls out for
free, *provided the forget event outlives the data events it scrubs* (see
retention below).

## Retention vs compaction (Kafka)

Two different tools, often confused:

- **Retention (`delete` cleanup policy)** bounds *time*: segments older than
  `retention.ms` vanish wholesale. If your event topics carry PII, set
  retention to the shortest window the business tolerates (e.g. 30 days) —
  then Kafka itself is never the long-term PII store, and erasure in Kafka
  reduces to "wait out the window" while patterns 1–2 handle the databases.
  Fits this repo's topology: topics are transport, Postgres is the
  system-of-record (ADR-0011: state-based, not event sourcing — you may
  truncate topics without losing state).
- **Compaction (`compact`)** bounds *keys*: the log keeps the latest record
  per key forever. PII in a compacted topic is erasable **only** by producing
  a tombstone (null value) for that key — and the actual disk reclaim waits
  for the cleaner; with `delete.retention.ms` and slow segment rolls, that's
  unbounded in the worst case, so compaction alone is *not* an erasure
  mechanism. Compacted topics keyed by `customer_id` + tombstone-on-forget
  work, but pair them with crypto-shredding when the erasure SLO is real.

Beware the **infinite-retention trap**: "keep everything for replay" plus PII
in payloads means every erasure request becomes a topic-rewrite project.
Decide per topic, in the schema review, whether a field is PII — and either
keep it out of events entirely (carry the pseudonymous id, look up PII at the
edge) or encrypt it per-subject from day one.

## Projection-row deletion

Read models are PII copies and must be wired into forgetting from the start:

- delete by subject, not by aggregate: `DELETE … WHERE customer_id = $1`
  needs the index to exist (the listing index here already covers it);
- the deletion goes through the normal projection consumer (inbox-deduped
  `CustomerForgotten` handler), never a manual psql session — manual deletes
  diverge from what a rebuild reproduces;
- rebuilds (`docs/operations.md`, projection-rebuild runbook) replay history:
  with bounded topic retention the PII-bearing events may already be gone —
  fine, the projection rebuilds from the surviving window plus the
  service-of-record state; with longer retention, the replayed forget event
  (or the destroyed key) re-erases;
- inbox/audit rows: cap their retention (cleanup workers) so scrubbed
  subjects don't linger in `payload` columns past the erasure SLO.

## Access requests (DSAR)

Erasure's sibling obligation (GDPR art. 15, CCPA "right to know") is showing
a subject what the system did with their identity. The audit trail is the
backbone of that answer, and the read path is SHIPPED, not just patterned:

- `audit.PgStore.Query(ctx, actor, since, limit)`
  (`platform/security/audit`) returns one actor's entries newest-first,
  backed by the `(actor, created_at)` index;
- the gateway exposes it as `GET /v1/audit?actor=<subject>&since=<rfc3339>&limit=<n>`
  — **admin-only** (the `admin` role; non-admins get 403), documented in
  `examples/gateway/openapi.yaml`. Each service owns its own `audit_log`
  table, so a full DSAR response aggregates this same query across services
  (the gateway endpoint demonstrates the pattern on its own database).

Two privacy caveats: the audit log is itself a PII location (the `actor`
column — see the inventory above), so its retention is bounded by the
cleanup worker (`audit.Cleanup`); and DSAR responses must not over-disclose —
entries reference subjects/ids, never raw payloads, which is why
`audit.Entry.Metadata` is a small string map and not the command body.

## Choosing

| Requirement | Pattern |
|---|---|
| Erasure must cover backups & Kafka history | Crypto-shredding (1) |
| Erasure of live DB rows + projections | Forget event (2) — simplest, start here |
| Kafka topics with PII payloads | Short retention; compaction+tombstones only with (1) on top |
| "We might event-source later" | Decide PII-in-events policy *now*; retrofitting (1) onto an existing log is the expensive path |

Start with (2) alone if events carry only pseudonymous ids — that is the
cheapest compliant posture this architecture supports, and the one its
defaults (state-based services, bounded-retention transport topics) are built
for.
