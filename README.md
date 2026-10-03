# OFM Order Service

## Purpose

The Order Service owns order data and order-specific read models: snapshots, requirements, checkout, delivery, attachments, revisions, disputes, and participant-facing reads. It does not own workflow orchestration or payment settlement. Status: active.

## Interfaces and data flow

The service exposes order gRPC and read operations used by the gateway and saga. The saga sends order commands; the service validates and persists them in PostgreSQL, writes outbox records, and publishes changes for CDC and projections. Redis is derived where configured and is never the authoritative write store.

## Configuration

.env.example groups are DB_*, MIGRATIONS_*, Redis, gRPC clients/listener, NATS/Kafka, and observability. Database values select order storage; Redis values select read models; broker values select command and projection flows.

## Local development

    cp .env.example .env
    just run
    go test ./...

## Build and operations

Dockerfile builds ofm/order-service:<tag>. Helm and ofm-infra provide deployment and dependencies. Diagnose order state transitions, outbox/CDC, Kafka lag, read-model freshness, and saga correlation IDs.

