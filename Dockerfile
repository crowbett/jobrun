# syntax=docker/dockerfile:1.7

# ---------- build ----------
FROM golang:1.27-alpine AS builder
WORKDIR /src

ARG VERSION=dev
ENV CGO_ENABLED=0 GOFLAGS=-buildvcs=false

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# gen/ and internal/postgres/sqlc are committed, so no buf/protoc/sqlc is needed
# inside the image build; goose migrations are embedded in the binary.
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/ ./cmd/jobrund ./cmd/jobworker ./cmd/jobrun

# ---------- runtime ----------
# distroless/static: no shell, no package manager, runs as uid 65532 (nonroot).
# NOTE: no HEALTHCHECK -- distroless has no shell. Use a gRPC probe against
# grpc.health.v1.Health/Check (k8s: `grpc: { port: 50052 }`) or grpc_health_probe.
FROM gcr.io/distroless/static-debian12:nonroot

# jobrund is the entrypoint; jobworker (remote worker Deployment in deploy/helm) and the
# jobrun CLI (kubectl exec ... /jobrun enqueue) ride along in the same image.
COPY --from=builder /out/jobrund /out/jobworker /out/jobrun /

# ADMIN_ADDR binds every interface here so the page is reachable through
# the container's published port; the admin surface has no auth, so
# ADMIN_ALLOW_REMOTE=true is the explicit opt-in jobrund requires for that.
# Publish 9110 only to loopback (-p 127.0.0.1:9110:9110) or not at all.
ENV GRPC_ADDR=:50052 ADMIN_ADDR=:9110 ADMIN_ALLOW_REMOTE=true LOG_FORMAT=json
EXPOSE 50052 9110
USER nonroot:nonroot

ENTRYPOINT ["/jobrund"]
