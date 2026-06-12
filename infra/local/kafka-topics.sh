#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-infra/local/docker-compose.yml}"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-cdc-data-platform}"
KAFKA_SERVICE="${KAFKA_SERVICE:-kafka}"
BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:29092}"

topics=(
  "cdc.raw.public.job_postings"
  "cdc.raw.public.applicants"
  "cdc.raw.public.evaluations"
  "cdc.raw.public.agent_tasks"
  "cdc.canonical.hiring"
  "cdc.retry.hiring"
  "cdc.dlq.hiring"
)

for topic in "${topics[@]}"; do
  docker compose --project-name "${COMPOSE_PROJECT_NAME}" -f "${COMPOSE_FILE}" exec -T "${KAFKA_SERVICE}" \
    kafka-topics \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions 3 \
    --replication-factor 1
done
