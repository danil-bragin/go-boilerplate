# syntax=docker/dockerfile:1.7
# Parametric multi-stage Dockerfile.
# Build a specific service with: docker build --build-arg SERVICE=gateway -t gateway:local .
# Services: gateway | orders | payments | notifications

ARG SERVICE=gateway

# ── builder ──────────────────────────────────────────────────────────────────
FROM golang:1.26-alpine AS builder

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

# ── runtime ──────────────────────────────────────────────────────────────────
FROM gcr.io/distroless/static:nonroot

COPY --from=builder /bin/app /bin/app
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

USER nonroot

ENTRYPOINT ["/bin/app"]
