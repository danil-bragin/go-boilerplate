# ADR 0005 — Kafka client: franz-go over confluent-kafka-go / segmentio/kafka-go

**Status:** Accepted  
**Date:** 2026-06-08

## Context

Three serious Go Kafka clients exist: `confluent-kafka-go` (CGO wrapper around librdkafka), `segmentio/kafka-go` (pure-Go, widely used), and `twmb/franz-go` (pure-Go, newer, actively maintained). `confluent-kafka-go` requires CGO, complicating cross-compilation and distroless container builds. `segmentio/kafka-go` is mature but lacks cooperative-sticky rebalancing and KIP-848 support; its EOS implementation has known gaps. `franz-go` is pure-Go, zero CGO, supports cooperative-sticky rebalancing by default, has a complete EOS implementation (`GroupTransactSession`), implements KIP-848 (next-gen consumer groups), and provides first-class OTel instrumentation via `kotel`.

## Decision

Use `twmb/franz-go` as the sole Kafka client. It is wrapped in `platform/kafka` behind producer and consumer-group abstractions that handle OTel tracing, retry-topics, DLT routing, and graceful shutdown.

## Consequences

- Pure-Go: distroless container images, no CGO cross-compilation issues, no librdkafka version pinning.
- Cooperative-sticky rebalancing reduces partition churn on rolling deployments.
- `GroupTransactSession` is available for the future Kafka EOS path (ADR-0004).
- franz-go is a single-maintainer project; this is an accepted risk mitigated by the wrapper abstraction — swapping the underlying client requires only changes to `platform/kafka`.
- `segmentio/kafka-go` and `confluent-kafka-go` are excluded as dependencies.
