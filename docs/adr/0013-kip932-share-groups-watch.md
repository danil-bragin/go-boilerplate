# ADR 0013 — Watch item: KIP-932 share groups vs. custom retry topics

**Status:** Accepted (watch item)  
**Date:** 2026-06-10

## Context

`platform/messaging/retry` exists because classic Kafka consumer groups couple delivery to partition order: one poison record blocks its partition, so non-blocking redrive requires escalation topics (`<topic>.retry.<idx>`), a second consumer, due-time headers, and the key-parking workaround for per-key ordering. That is ~all accidental complexity standing in for a queue semantic Kafka historically lacked.

**KIP-932 ("Queues for Kafka", share groups)** adds exactly that semantic to the broker: per-record acknowledgement, `ack`/`release`/`reject`, broker-tracked delivery counts with automatic redelivery, and per-record DLQ-style terminal handling — without partitions limiting consumer counts. Timeline: early access in Apache Kafka 4.0, preview in 4.1, with GA targeted around 4.2. franz-go support and Redpanda parity were not production-ready when this was written.

## Decision

Keep the custom retry-topic machinery, and **re-evaluate it for deletion when ALL of:**

1. The broker fleet runs a Kafka version with share groups **GA** (≥ 4.2, not preview), or Redpanda ships production share-group parity — whichever broker the deployment actually uses.
2. franz-go has stable share-group consumer support.
3. Broker-side delivery-count + redelivery semantics cover the policies used here (bounded attempts, terminal dead-lettering).

## Consequences

- Until then: index-named retry tiers + DLT + `cmd/redrive` remain the supported path.
- **What gets deleted when the trigger fires:** `platform/messaging/retry` (escalator, retry consumer, key parking), the `.retry.<idx>` topic provisioning in `servicekit.AddConsumerWithRetry`, and the retry-tier sections of the runbooks. The DLT + `cmd/redrive` flow likely survives in simplified form (share groups dead-letter per record; redrive tooling still needs to exist). The inbox stays regardless — share groups are at-least-once, not effectively-once.
- Consumers written against `kafka.HandlerFunc`/`consume.Typed` are unaffected by the swap; only the wiring in servicekit changes. Keep it that way.
