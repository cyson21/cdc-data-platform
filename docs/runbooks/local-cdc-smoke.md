# Local CDC Smoke Runbook

## Purpose

Verify that source PostgreSQL applicant changes are captured by Debezium and visible in the raw Kafka topic.

## Scope

This is local-first evidence. It does not prove production-scale performance and does not require real AWS S3, Athena, or dbt-athena.

The local Kafka Connect image is pruned to the Debezium PostgreSQL connector only. This keeps plugin scanning focused on the project source database and avoids treating Docker VM resource tuning as platform performance evidence.

## Preflight

```bash
docker compose -f infra/local/docker-compose.yml config -q
docker compose --project-name cdc-data-platform -f infra/local/docker-compose.yml -f infra/local/docker-compose.apache-kafka.yml config -q
CDC_SMOKE_DOCKER_TIMEOUT_SECONDS=3 CDC_SMOKE_EXTRA_COMPOSE_FILES=infra/local/docker-compose.apache-kafka.yml ./tools/runner/cdc-smoke docker-preflight
./tools/runner/cdc-smoke --help
python3 -m py_compile tools/runner/cdc-smoke
```

## Run

```bash
./tools/runner/cdc-smoke applicant-change-capture
```

For the current 1 CPU / 1 GB Colima VM, use the local Apache Kafka override instead of pulling new images or increasing VM memory:

```bash
CDC_SMOKE_EXTRA_COMPOSE_FILES=infra/local/docker-compose.apache-kafka.yml CDC_SMOKE_KAFKA_BIN_DIR=/opt/kafka/bin ./tools/runner/cdc-smoke applicant-change-capture
docker compose --project-name cdc-data-platform -f infra/local/docker-compose.yml -f infra/local/docker-compose.apache-kafka.yml down
```

The 2026-06-09 Apache Kafka override run produced raw applicant `c/u/d` events with Debezium `source.lsn`, `sequence`, `txId`, and source metadata event ids. Kafka Connect exited with `137` after event capture on the 1 CPU / 1 GB VM, so the Connect offsets endpoint can be recorded as unavailable while `eventSourceOffset` remains present in the raw event proof.

Before any runtime scenario, the runner checks Docker daemon responsiveness through `docker-preflight`. If Docker is unhealthy, runtime scenarios fail fast with a JSON `Docker preflight failed` message instead of entering Compose startup. A 2026-06-09 rerun returned `status=ok` with Docker server version `29.2.1`. Earlier unhealthy Docker state returned:

```text
status=timeout
reason=docker info timed out after 3 seconds
composeProjectName=cdc-data-platform
```

To observe Kafka Connect after raw event capture:

```bash
CDC_SMOKE_EXTRA_COMPOSE_FILES=infra/local/docker-compose.apache-kafka.yml CDC_SMOKE_KAFKA_BIN_DIR=/opt/kafka/bin CDC_SMOKE_STABILITY_SECONDS=20 ./tools/runner/cdc-smoke connect-stability
```

`connect-stability` is green only when the runner prints `connectorStatusSamples` for the requested duration. The current local proof uses the Apache Kafka low-memory override and applicant-focused connector scope. It printed connector/task `RUNNING` samples at elapsed seconds `[0,5,10,15,20]` plus operations `c/u/d`, source LSN, event source offset, and event id. Runtime connector setup waits for PostgreSQL replication slot `cdc_data_platform_slot` to become active before source mutation. The applicant raw-topic consumer streams output and terminates as soon as the target applicant has all required operations. Parser null payloads are ignored, offset API timeouts are recorded as unavailable evidence, timeout messages include observed applicant candidate events, and stability status probe failures include partial samples, `kafka-connect` service status, Docker inspect state, bounded log tail, and Docker preflight status. After raw CDC capture, failure context also includes operations, source LSN, event source offset, and event id. Do not treat runner registration or unit tests as long-running stability evidence.

To exercise Kafka outage recovery:

```bash
CDC_SMOKE_EXTRA_COMPOSE_FILES=infra/local/docker-compose.apache-kafka.yml CDC_SMOKE_KAFKA_BIN_DIR=/opt/kafka/bin ./tools/runner/cdc-smoke kafka-outage-recovery
docker compose --project-name cdc-data-platform -f infra/local/docker-compose.yml -f infra/local/docker-compose.apache-kafka.yml stop kafka-connect kafka source-postgres
```

`kafka-outage-recovery` is green only when the runner prints pre-outage `c`, outage-time source mutations `u/d`, post-recovery `u/d` raw events, Kafka status snapshots, source LSN, source offset, event id, `durableOffsetResumed=true`, and zero missing/duplicate counts. The current local proof satisfies those fields under the Apache Kafka low-memory override. It stops and starts only the Project 05 Kafka service. Do not treat runner registration or unit proof-shape tests as runtime recovery proof.

To exercise Kafka Connect restart recovery:

```bash
CDC_SMOKE_EXTRA_COMPOSE_FILES=infra/local/docker-compose.apache-kafka.yml CDC_SMOKE_KAFKA_BIN_DIR=/opt/kafka/bin ./tools/runner/cdc-smoke connector-restart-recovery
docker compose --project-name cdc-data-platform -f infra/local/docker-compose.yml -f infra/local/docker-compose.apache-kafka.yml stop kafka-connect kafka source-postgres
```

`connector-restart-recovery` is green only when the runner prints restart-before and restart-after operations with source LSN, source offset, event id, `durableOffsetResumed=true`, and zero missing/duplicate counts. The current local proof satisfies those fields under the Apache Kafka low-memory override.

To observe Data Quality SLA against local runtime CDC events:

```bash
CDC_SMOKE_EXTRA_COMPOSE_FILES=infra/local/docker-compose.apache-kafka.yml CDC_SMOKE_KAFKA_BIN_DIR=/opt/kafka/bin ./tools/runner/cdc-smoke quality-sla-runtime
docker compose --project-name cdc-data-platform -f infra/local/docker-compose.yml -f infra/local/docker-compose.apache-kafka.yml stop kafka-connect kafka source-postgres
```

`quality-sla-runtime` is green when the runner captures the target applicant `c/u/d` raw events and prints completeness, duplicate rate, freshness lag, event source offsets, source LSN range, and source metadata event ids. The runner writes the same machine-readable proof to `lakehouse/out/proofs/quality-sla-runtime.json`. The 2026-06-12 run printed `slaStatus=WARN`, `completenessRatio=1.0`, `duplicateRatio=0.0`, `missingEventCount=0`, `duplicateEventCount=0`, and warning `source-lsn-lag-unavailable` because Kafka Connect `/offsets` returned an empty offset list. This is still local runtime observation of raw Debezium event source offsets, not production-scale SLA evidence. Use `./tools/runner/control-plane-smoke quality-sla-runtime-record` to prove the same runtime-shaped evidence can be persisted through the control-plane SLA table/API path.

For Docker-free parser/idempotency fixture evidence:

```bash
./tools/runner/cdc-smoke schema-evolution-fixture
```

For local Docker Debezium schema evolution runtime evidence:

```bash
CDC_SMOKE_EXTRA_COMPOSE_FILES=infra/local/docker-compose.apache-kafka.yml CDC_SMOKE_KAFKA_BIN_DIR=/opt/kafka/bin ./tools/runner/cdc-smoke schema-evolution-runtime
docker compose --project-name cdc-data-platform -f infra/local/docker-compose.yml -f infra/local/docker-compose.apache-kafka.yml stop kafka-connect kafka source-postgres
```

## Expected Evidence

The runner prints JSON with:

```text
sourceTable=applicants
sourcePrimaryKey=<uuid>
operations=["c","u","d"]
sourceLsn=<non-empty values>
eventSourceOffset=<Debezium raw event source lsn/sequence/txId values>
eventId=<source metadata based ids>
rawTopic=cdc.raw.public.applicants
rawEventCount>=3
sourceOffset=<Kafka Connect offset response or explicit unavailable reason>
```

For `connect-stability`, the runner additionally prints:

```text
scenario=connect-stability
stabilitySecondsRequested=<duration>
stabilitySecondsObserved=<duration or last sample elapsed seconds>
connectorStatusSamples=[{"connectorState":"RUNNING","taskStates":["RUNNING"],...}]
```

For `connector-restart-recovery`, the runner additionally prints:

```text
scenario=connector-restart-recovery
evidenceLabel=local-docker-debezium-runtime
operationsBeforeRestart=["c"]
operationsAfterRestart=["u","d"]
recoveryAction=kafka-connect-force-recreate
connectorStatusBeforeRestart={"connectorState":"RUNNING","taskStates":["RUNNING"]}
connectorStatusAfterRestart={"connectorState":"RUNNING","taskStates":["RUNNING"]}
sourceOffsetBeforeRestart=<Kafka Connect offset response or explicit unavailable reason>
sourceOffsetAfterRestart=<Kafka Connect offset response or explicit unavailable reason>
durableOffsetResumed=true
missingEventCount=0
duplicateEventCount=0
```

For `kafka-outage-recovery`, the runner additionally prints:

```text
scenario=kafka-outage-recovery
evidenceLabel=local-docker-debezium-runtime
operationsBeforeOutage=["c"]
sourceMutationsDuringOutage=["u","d"]
operationsAfterRecovery=["u","d"]
recoveryAction=kafka-stop-start
connectorStatusBeforeOutage={"connectorState":"RUNNING","taskStates":["RUNNING"]}
connectorStatusAfterRecovery={"connectorState":"RUNNING","taskStates":["RUNNING"]}
kafkaStatusBeforeOutage=<compose service status>
kafkaStatusDuringOutage=<compose service status>
kafkaStatusAfterRecovery=<compose service status>
sourceOffsetBeforeOutage=<Kafka Connect offset response or explicit unavailable reason>
sourceOffsetAfterRecovery=<Kafka Connect offset response or explicit unavailable reason>
durableOffsetResumed=true
missingEventCount=0
duplicateEventCount=0
```

For `quality-sla-runtime`, the runner additionally prints:

```text
scenario=quality-sla-runtime
evidenceLabel=local-docker-debezium-runtime-sla
requiredOperations=["c","u","d"]
expectedEventCount=3
observedRuntimeEventCount=3
completenessRatio=1.0
duplicateRatio=0.0
freshnessLagMillis=<Debezium ts_ms to local observation lag>
sourceLsnFrom=<first target event LSN>
sourceLsnTo=<last target event LSN>
sourceConnectorOffsetLsn=<Kafka Connect offset LSN or null>
sourceLsnLag=<offset LSN minus sourceLsnTo or null>
slaStatus=PASSED|WARN|FAILED
violatedSloIds=[]
warningSloIds=[] or ["source-lsn-lag-unavailable"]
```

For `schema-evolution-fixture`, the runner prints JSON with:

```text
scenario=schema-evolution-fixture
evidenceLabel=fixture
sourceTable=applicants
operations=["c","u"]
sourceLsn=[88432240,88432241]
eventSourceOffset=[{"lsn":88432240},{"lsn":88432241}]
eventId=<source metadata based ids>
schemaVersions=["v1","v2_added_screening_score"]
sourceOffset={"file":"000000010000000000000058","pos":32240}
runtimeEventCount=0
```

For `schema-evolution-runtime`, the runner prints JSON with:

```text
scenario=schema-evolution-runtime
evidenceLabel=local-docker-debezium-runtime
sourceTable=applicants
operations=["c","u"]
sourceLsn=<runtime LSN values>
eventSourceOffset=<Debezium raw event source lsn/sequence/txId values>
eventId=<source metadata based ids>
schemaVersions=["v1_before_screening_score","v2_added_screening_score"]
schemaAddedField=screening_score
schemaAddedFieldObserved=true
schemaAddedFieldValue=87
runtimeEventCount=2
sourceOffset=<Kafka Connect offset response or explicit unavailable reason>
```

## Interpretation

- `operations` use Debezium operation codes: `c` insert, `u` update, `d` delete.
- `eventId` is derived from Debezium source metadata and primary key, not wall-clock time.
- Applicant runtime consumers stream the raw topic and stop after target applicant operations are observed; this avoids waiting for broad topic backlog exhaustion.
- Runtime connector setup waits for `pg_replication_slots.active=true` on `cdc_data_platform_slot` before source mutation.
- If target applicant operations are not observed, timeout output includes `observedApplicantEvents` to help distinguish "no raw data" from "raw data for other applicant ids".
- `canonicalEventCount` is expected to be `0` until the canonical control-plane slice is implemented.
- `kafka-outage-recovery` is not green proof unless Kafka comes back up and the outage-time source updates are observed after recovery.
- `connector-restart-recovery` is not green proof unless Kafka stays up through the forced Kafka Connect recreate and the post-restart `u/d` events are observed.
- `quality-sla-runtime` is local Debezium/Kafka runtime SLA observation over raw applicant CDC events. Pair it with control-plane `quality-sla-runtime-record` before claiming the runtime-shaped evidence is persisted; `project-audit` now checks the paired proof artifacts.
- `source-lsn-lag-unavailable` means Kafka Connect `/offsets` did not return a usable LSN; inspect `eventSourceOffset` for Debezium raw event source metadata.
- `schema-evolution-fixture` is not Debezium runtime capture proof; it is a deterministic fixture that exercises source metadata event id and schema-added payload shape.
- `schema-evolution-runtime` is local Docker Debezium runtime evidence. It proves nullable column addition was reflected in raw CDC data events, but it is still local reproducibility evidence and not production-scale proof.
