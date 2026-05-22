FROM golang:1.25.5 AS builder

WORKDIR /src

COPY ofm-common /src/ofm-common
COPY ofm-order-service /src/ofm-order-service

WORKDIR /src/ofm-order-service

RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/order-service ./cmd/order-service

FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && groupadd --system app \
    && useradd --system --gid app --home-dir /app --shell /usr/sbin/nologin app \
    && chown app:app /app \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/order-service /app/order-service
COPY --from=builder /src/ofm-order-service/migration /app/migration

RUN chown -R app:app /app

USER app:app

CMD ["/app/order-service"]
