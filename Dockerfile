# syntax=docker/dockerfile:1.7
# Parametric multi-stage Dockerfile.
# Build a specific service with: docker build --build-arg SERVICE=gateway -t gateway:local .
# Services: gateway | orders | payments | notifications

ARG SERVICE=gateway

# ── builder ──────────────────────────────────────────────────────────────────
# Digest-pinned tag golang:1.26-alpine (tag in comment for readability). Bump
# the digest via the Renovate/Dependabot docker lane (.github/dependabot.yml).
FROM golang:1.26-alpine@sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2 AS builder

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
FROM gcr.io/distroless/static:nonroot@sha256:d29e660cc75a5b6b1334e03c5c81ccf9bc0884a002c6000dbf0fb96034814478

COPY --from=builder /bin/app /bin/app
COPY --from=builder /bin/probe /probe
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

USER nonroot

HEALTHCHECK --interval=10s --timeout=3s --retries=5 --start-period=15s CMD ["/probe"]

ENTRYPOINT ["/bin/app"]
