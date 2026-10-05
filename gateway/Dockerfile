# syntax=docker/dockerfile:1
FROM golang:1.26-bookworm AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -o /usr/local/bin/gateway ./cmd/gateway

# CGO_ENABLED=0 static binary needs nothing from the OS image, so distroless nonroot suffices.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /usr/local/bin/gateway /usr/local/bin/gateway

USER nonroot
ENTRYPOINT ["gateway"]
