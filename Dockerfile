# syntax=docker/dockerfile:1.7
# Parametric multi-stage Dockerfile.
# Build a specific service with: docker build --build-arg SERVICE=gateway -t gateway:local .
# Services: gateway | orders | payments | notifications

ARG SERVICE=gateway

# ── builder ──────────────────────────────────────────────────────────────────
# Digest-pinned tag golang:1.26-alpine (tag in comment for readability). Bump
# the digest via the Renovate/Dependabot docker lane (.github/dependabot.yml).
FROM golang:1.26-alpine@sha256:7a3e50096189ad57c9f9f865e7e4aa8585ed1585248513dc5cda498e2f41812c AS builder

ARG SERVICE

RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /src

# Layer-cache go module downloads independently from source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /bin/app \
    ./examples/${SERVICE}/cmd/${SERVICE}

# Static healthcheck client for the distroless runtime (no shell/curl there).
# Probes http://127.0.0.1:<port>/livez where <port> comes from ADMIN_HTTP_ADDR
# (servicekit admin listener, default :9090). See cmd/probe.
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /bin/probe \
    ./cmd/probe

# ── runtime ──────────────────────────────────────────────────────────────────
# Digest-pinned tag gcr.io/distroless/static:nonroot (tag in comment).
# distroless/static:nonroot is a rolling tag — pinning the digest makes the
# runtime base reproducible; bump via the Renovate/Dependabot docker lane.
FROM gcr.io/distroless/static:nonroot@sha256:f7f8f729987ad0fdf6b05eeeae94b26e6a0f613bdf46feea7fc40f7bd72953e6

COPY --from=builder /bin/app /bin/app
COPY --from=builder /bin/probe /probe
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

USER nonroot

HEALTHCHECK --interval=10s --timeout=3s --retries=5 --start-period=15s CMD ["/probe"]

ENTRYPOINT ["/bin/app"]
