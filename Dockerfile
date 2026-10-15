# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM registry.access.redhat.com/hi/go:1-builder AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download
COPY . .
ENV CGO_ENABLED=0
ARG TARGETOS TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o kafka-source-connector ./main.go

FROM --platform=${TARGETOS}/${TARGETARCH} registry.access.redhat.com/hi/core-runtime:2-openssl-builder AS runtime
COPY --from=builder --chown=65532:0 /app/kafka-source-connector .
EXPOSE 8080
ENTRYPOINT ["./kafka-source-connector"]
