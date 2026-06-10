# Round 4 — Application Layer: layering, error codes, i18n, time

> Design approved by user 2026-06-10 (brainstorm decisions: layers EVERYWHERE uniformly;
> ambient-tx formalized; flat UPPER_SNAKE codes; full i18n+time demo).
> TDD mandatory; one commit per task; normal-English messages +
> "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"; `--no-verify`.
> Research grounding: RFC 9457 (code+params extensions legal), Google AIP-193 (every message
> variable MUST be in params), Stripe-style flat string codes, go-i18n v2 (alive; universal-translator
> dead — avoid), pgx TimestamptzCodec ScanLocation gotcha, "cmd never calls cmd" rule,
> repository interfaces consumer-side, ambient ctx-tx kept (inbox owns tx invariant).

Waves: **W1** platform foundations → **W2** services layering ∥ gateway edge → **W3** docs+registry-gen+review+gate.

## W1 — platform foundations (one lane)

| id | task |
|---|---|
| W1.1 | `platform/apperr`: `Error{Code string, Status int, Params map[string]any, Permanent bool, msg string, err error}`; constructors `New/Newf`, `Wrap(err, code,…)`, `WithParam(s)`; `errors.Is/As/Unwrap`; `Code(err) string` helper (unknown → "INTERNAL"); invariant enforced by vet-style test: msg template variables ⊆ Params keys (Google rule — store msg as template w/ {param} refs, render for logs). Registry: `Register(code, status, permanent bool)` at init from per-service const blocks (platform codes here: `INTERNAL`, `VALIDATION_FAILED`, `AUTH_UNAUTHENTICATED`, `AUTH_FORBIDDEN`, `NOT_FOUND` generics live with services instead where domain-specific). Duplicate-registration panic. `apperr.IsPermanent(err)`. Tests: table — wrap/is/as/code/params/permanent, registry dup panic, template-params invariant. |
| W1.2 | `httpx.Problem` extensions: `Code string`, `Params map[string]any`, `Instance string` (request path or request-id URN); `WriteProblem` sets them; `httpx.FromError(err) Problem` maps `apperr` (status/code/params) else 500 INTERNAL; RFC 9457 reference in godoc. Decode/validation errors → `VALIDATION_FAILED` + `errors` field-map kept + per-field params `[{field,rule,param}]`. Tests: shape, RFC 9457 member-name compliance, As-mapping, unknown error → INTERNAL w/o leaking detail. |
| W1.3 | `cqrs.Validation` rework: validator.ValidationErrors → `apperr.New(VALIDATION_FAILED, 400, Permanent=true)` with `Params{"fields":[{field,rule,param}]}`; keep `Validatable` escape. Behavior order unchanged. Tests: tag failure → typed error w/ field params; Validatable path. |
| W1.4 | Permanent→DLT short-circuit: `kafka.WithRetry` + servicekit fast-attempts wrap + `retry.Escalator` path: `apperr.IsPermanent(err)` → skip remaining attempts/tiers, produce straight to DLT with `x-error-code` header = apperr code. consume.Typed: decode/unknown-type errors already skip; handler permanent errors flow through. Integration test: validation-failing event → DLT after ONE attempt, no tier topics touched; transient error keeps old behavior. |
| W1.5 | `platform/i18n`: go-i18n v2 + x/text matcher. `Bundle` from embed.FS (TOML: `en.toml` base, `ru.toml`); `Middleware(bundle, supported...)` parses Accept-Language → ctx locale+localizer; `T(ctx, code, params)` → message or "" when missing (caller falls back to dev msg); message IDs = error codes + `validation.<rule>` keys. WriteProblem integration: localize Title/Detail when localizer in ctx (httpx gets optional hook — `httpx.SetLocalizer`-free design: gateway passes a `ProblemLocalizer` func via middleware-installed ctx value to keep httpx dependency-free of i18n; pick clean seam, document). Tests: negotiation (ru, en, q-weights, unsupported→en), missing key fallback, plural demo case. |
| W1.6 | `platform/clock`: `Clock` iface + `System` (Now() = time.Now().UTC()) + docs "inject only where business reads now; DB time = now() stays"; synctest usage example in package test. `platform/storage/pg`: pool AfterConnect registers `TimestamptzCodec{ScanLocation: time.UTC}` for BOTH pools; µs-truncation note in godoc. Tests: scan returns UTC location even with TZ=America/New_York session (set via conn exec `SET TIME ZONE`), pgtest integration; clock test w/ synctest. |

## W2-A — services layering (orders showcase + payments + notifications)

| id | task |
|---|---|
| W2A.1 | orders `internal/domain/order`: `Repository` iface (Insert, MarkPaymentOutcome, MarkTimeoutEmitted, ListUnpaidExpired, Get…) consumer-side; `pgRepository` impl over sqlc resolving DBTX via `pg.FromContext` (ambient formalized — godoc states the invariant); `Service` owning: status state-machine (table-driven transitions pending→created→{paid,payment_failed,payment_timeout}; invalid transition → `apperr ORDERS_INVALID_STATUS_TRANSITION` Permanent), Create (insert + outbox OrderCreated atomically — outbox enqueue stays in service via injected enqueuer iface), ApplyPaymentOutcome (first-outcome-wins + compensation warn — logic MOVED from transport/payments_consumer.go), EmitPaymentTimeout (CAS + outbox — moved from watcher). Clock injected (occurred_at). Codes const block `ORDERS_*` + registration. Unit tests: state machine table (no Docker), service w/ fake repo+enqueuer; integration: repo over pgtest; entry-point tests stay green. |
| W2A.2 | orders entry-points become thin adapters: CreateOrderHandler → service.Create; transport payments_consumer → service.ApplyPaymentOutcome (transport keeps ONLY decode+dispatch); unpaid_watcher → service.EmitPaymentTimeout (keeps loop+RunInTx boundary). "cmd never calls cmd" + layering rationale in package docs. All existing orders tests green; transport test asserts no business branching remains (review-level). |
| W2A.3 | payments `internal/domain/payment`: Repository iface + pgRepository; `Service.Process` (decision rule via injected Clock, persist, outbox event) — `paymentOutcome` moves in; ProcessPaymentHandler → adapter. Codes `PAYMENTS_*`. Unit (fake repo) + existing integration green. |
| W2A.4 | notifications: thin `internal/domain/notification.Service` (record/log) — demonstrates minimal uniform layer; adapter in transport. Codes if any. Tests: unit. |

## W2-B — gateway edge (validation, errors, i18n wiring, time)

| id | task |
|---|---|
| W2B.1 | gateway codes `GATEWAY_*` (`GATEWAY_ORDER_NOT_FOUND`, `GATEWAY_INVALID_CURSOR`, `GATEWAY_IDEMPOTENCY_BODY_MISMATCH`, `GATEWAY_ATTACHMENT_*`…); replace inline Problem constructions + `authError` type with apperr returns; `responseErrorHandler`/`requestErrorHandler` → `httpx.FromError` + localization; auth/authz platform codes wired. e2e/gateway tests updated to assert codes. |
| W2B.2 | edge validation: openapi constraints (amount_cents min 1 max?, currency `^[A-Z]{3}$` + ISO-4217 whitelist in code, customer_id maxLength 128) + regen; CreateOrder validates the command struct (same tags as orders command) BEFORE produce → 400 VALIDATION_FAILED w/ field params. Consumer keeps validation (defense in depth; now Permanent→DLT fast). Tests: 400 cases incl currency, e2e negative case. |
| W2B.3 | i18n wiring: bundle (en+ru catalogs for all registered codes + validation rules) embedded in gateway (or platform/i18n/catalog default + gateway overrides — pick: catalogs live WITH the service that owns the codes; platform ships en defaults for platform codes); middleware mounted; Accept-Language: ru → localized title/detail, en default; contract fields code/params asserted unchanged. Tests: integration ru/en problem responses. |
| W2B.4 | time in API: OrderView + list items get `created_at` (RFC3339 UTC Z) — openapi format date-time + projection/store plumbing (created_at already in orders_read? check migration — add if missing w/ backfill); `X-Timezone` header (IANA, tzdb-validated, else 400 `GATEWAY_INVALID_TIMEZONE`) → response adds `created_at_local` (display-only, documented); cursor untouched. SSE unaffected. Tests: Z-format assert, tz happy/invalid, DST-boundary format case. |

## W3 — registry gen, docs, gate

| id | task |
|---|---|
| W3.1 | `go generate` registry → `docs/errors.md` (code, status, permanent, params schema, en text) — script walks registered codes (export registry snapshot via small cmd/errgen or test-generated file; choose simplest reproducible); CI check: docs/errors.md in sync (generate + git diff). |
| W3.2 | docs: conventions.md §layering (uniform-layers decision + rationale "examples are templates: uniformity > YAGNI", cmd-never-calls-cmd, repo-iface-consumer-side, ambient-tx formalized + goroutine gotcha), §errors (registry process, additive-only, params rule), §time (UTC rules table, ScanLocation, X-Timezone convention, DST note for civil dates — future seam), §i18n (contract=code+params; server localization = courtesy). adding-a-service.md updated (new service checklist: codes block, domain pkg, catalogs). ARCHITECTURE.md rows. .env.example if new envs. |
| W3.3 | Final gate: lint 0; gofumpt; short lane green; full `-p 1` suite green; e2e (codes+i18n+created_at assertions); review pass over round-4 diff (adversarial, MUST-FIX fixed); plan archived; memory updated. |

Notes/risks (from code analysis): do NOT genericize repositories (sqlc already typed); projection state machine stays separate from orders' (different by design — placeholder upserts); idempotency replica-staleness logic in gateway must not be "simplified" into service; e2e problem-payload assertions will need updating (codes added) — update, don't loosen.
