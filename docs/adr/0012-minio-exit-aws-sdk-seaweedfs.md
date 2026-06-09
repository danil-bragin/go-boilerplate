# ADR 0012 — Object storage: aws-sdk-go-v2 client + SeaweedFS for local dev (MinIO exit)

**Status:** Accepted  
**Date:** 2026-06-10

## Context

`platform/storage/blob` used `minio-go` as its S3 client and MinIO as the local/dev object store. MinIO's open-source story collapsed over 2025–2026: the community edition was progressively stripped (management UI removed mid-2025), development moved to the commercial AIStor product, and between December 2025 and February 2026 the open-source MinIO server entered maintenance mode with its repository effectively archived — no feature work, security-fix cadence unclear. Building a boilerplate's storage layer on a client SDK and dev server owned by a vendor that has abandoned its OSS edition is an unacceptable long-term risk.

The `ObjectStore` interface (`Put`/`Get`/`Delete`/`Exists`/`PresignGet`/`List`) was always plain S3 semantics; nothing in it is MinIO-specific.

## Decision

- **Client:** `github.com/aws/aws-sdk-go-v2` (`service/s3` + `s3.PresignClient`) replaces `minio-go`. It is the canonical, vendor-neutral implementation of the S3 API, maintained by AWS, and works against any S3-compatible store. Path-style addressing is a config option (`S3_USE_PATH_STYLE`, default `true`) for local stores; flexible-checksum calculation/validation is set to `WhenRequired` for third-party S3 compatibility.
- **Local/dev server:** SeaweedFS (`chrislusf/seaweedfs`, Apache-2.0, actively maintained) replaces MinIO in `docker-compose.yml` — a single all-in-one container (`server -s3`, S3 API on 8333) with static credentials from `deploy/seaweedfs/s3.json` and an `amazon/aws-cli` one-shot job creating the default bucket.
- **Tests:** blob contract tests and gateway attachment integration tests run against a SeaweedFS generic testcontainer; the `testcontainers-go/modules/minio` dependency is dropped.

The exported `ObjectStore` interface and `blob.Config` env var names (`S3_*`) are unchanged — zero caller changes outside config plumbing.

## Consequences

- No MinIO dependencies remain in `go.mod` (`minio-go`, `minio` testcontainer module removed).
- Production deployments point the same client at AWS S3 (empty `S3_ENDPOINT`, `S3_USE_PATH_STYLE=false`) or any S3-compatible store — no code changes.
- SeaweedFS is dev-infrastructure only; it is never imported by Go code, so swapping it later costs one compose service.
- aws-sdk-go-v2 is heavier than minio-go (multiple modules) but is the industry-default S3 client with guaranteed maintenance.
- Local S3 endpoint moves from `localhost:9000` to `localhost:8333`; dev credentials change from `minioadmin` to `seaweedadmin` (see `.env.example`).
