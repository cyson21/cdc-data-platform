# Local CDC Infrastructure

This directory contains the local-first infrastructure skeleton for Project 05.

## Services

| Service | Purpose |
|---|---|
| `source-postgres` | ATS-style source database with logical replication enabled |
| `control-postgres` | Spring Boot control-plane database |
| `kafka` | Raw, canonical, retry, and DLQ event topics |
| `kafka-connect` | Debezium PostgreSQL connector runtime |
| `minio` | S3-compatible object storage for later Iceberg evidence |
| `iceberg-catalog-postgres` | JDBC Iceberg catalog metadata store for local Trino proof |
| `trino` | Local query engine candidate for later lakehouse checks |

## Local Kafka Connect Image

`kafka-connect` builds `infra/local/connect-postgres.Dockerfile` from `quay.io/debezium/connect:2.7`.
The local image removes non-PostgreSQL Debezium connector directories so the smoke runner does not scan the full Debezium bundle on small local Docker VMs.

## Verify Compose

```bash
docker compose -f infra/local/docker-compose.yml config -q
```

## Start Local Stack

```bash
docker compose -f infra/local/docker-compose.yml up -d
```

## Lakehouse Stack

`trino` mounts `infra/local/trino/catalog/iceberg.properties` into `/etc/trino/catalog`.
The `iceberg` catalog uses JDBC catalog metadata in `iceberg-catalog-postgres` and stores table data under `s3://cdc-lakehouse/warehouse` through MinIO.
Use `tools/runner/lakehouse-smoke trino-compose-iceberg-bootstrap` and `trino-compose-iceberg-append` for pull-never local proof after the required images are explicitly available.

## Create Topics

```bash
./infra/local/kafka-topics.sh
```

## Register Debezium Connector

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  --data @infra/local/debezium-postgres-connector.json \
  http://localhost:8083/connectors
```

## Guardrails

- This stack is local reproducibility evidence, not production-scale performance evidence.
- Small Docker VM settings can block Kafka and Kafka Connect from running together. Treat memory/CPU changes as local execution prerequisites, not platform performance evidence.
- Real AWS S3, Athena, and dbt-athena remain opt-in only.
- CDC proof runners must print Debezium source offset, source LSN, and event id.
