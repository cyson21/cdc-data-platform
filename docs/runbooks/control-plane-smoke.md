# Control Plane Smoke Runbook

## Purpose

Verify local Spring Boot control-plane ingest and recovery behavior that can run without Kafka Connect.

## Scope

`canonical-ingest` uses unit service/API tests to verify:

- raw Debezium envelope parsing
- source metadata event id generation
- source offset JSON preservation
- canonical ledger handoff and duplicate count result shape

`sink-failure-replay` uses Testcontainers PostgreSQL and real repository/service classes to verify:

- source metadata ledger duplicate prevention
- DLQ row replay request creation
- retry event scheduling
- publish-ready handoff to retry topic metadata

`kafka-outage-recovery` and `connector-restart-recovery` use Testcontainers PostgreSQL and real recovery repository/service classes to verify:

- recovery run idempotency from source LSN, source offset, and event id boundaries
- recovered event count, duplicate event count, and gap flag recording
- latest recovery state lookup by scenario and connector

These scenarios do not prove Kafka broker outage, Kafka Connect restart, Debezium runtime capture, or lakehouse sink delivery.

`kafka-backed-sink-failure-replay` uses Testcontainers PostgreSQL plus Spring Kafka embedded broker to verify:

- a ready retry event is sent to `cdc.retry.applicants`
- the embedded broker returns partition and offset metadata
- the retry row is marked `PUBLISHED` only after broker send completion

`pipeline-quality` uses unit service/API tests to verify:

- quality status calculation from source/raw/canonical counts
- duplicate and missing event status boundaries
- source LSN range and event id details preservation

`pipeline-quality-db` uses Testcontainers PostgreSQL and the real quality repository to verify:

- quality check insert into `pipeline_quality_checks`
- latest quality check lookup by check name and source table
- source LSN range and event id details persisted as JSONB

`quality-sla` uses unit service/API tests to verify:

- freshness lag threshold evaluation
- connector lag threshold evaluation
- completeness ratio threshold evaluation
- duplicate ratio threshold evaluation
- source LSN range and event id details preservation in SLA proof

`quality-sla-db` uses Testcontainers PostgreSQL and the real SLA repository/service/API path to verify:

- SLA evaluation insert into `pipeline_quality_sla_evaluations`
- latest SLA evaluation lookup by check name and source table
- ratio, threshold, lag, violated SLO id, source LSN range, and event id details persisted as JSONB

`quality-sla-runtime-record` uses Testcontainers PostgreSQL and the runtime SLA record API/service path to verify:

- runtime-shaped SLA observation insert into `pipeline_quality_sla_evaluations`
- warning SLO ids such as `source-lsn-lag-unavailable` are preserved
- runtime source LSN, event source offset, event id, and Kafka Connect offset-unavailable details are persisted as JSONB
- latest SLA evaluation id is returned from the same SLA table
- `lakehouse/out/proofs/quality-sla-runtime-record.json` is written for `project-audit` paired evidence checks; if `quality-sla-runtime.json` exists in the same proof dir, the runner reuses that runtime source metadata.

## Preflight

```bash
python3 -m py_compile tools/runner/control-plane-smoke
python3 tools/tests/test_control_plane_smoke.py
```

## Run

```bash
./tools/runner/control-plane-smoke canonical-ingest
./tools/runner/control-plane-smoke sink-failure-replay
./tools/runner/control-plane-smoke kafka-outage-recovery
./tools/runner/control-plane-smoke connector-restart-recovery
./tools/runner/control-plane-smoke kafka-backed-sink-failure-replay
./tools/runner/control-plane-smoke pipeline-quality
./tools/runner/control-plane-smoke pipeline-quality-db
./tools/runner/control-plane-smoke quality-sla
./tools/runner/control-plane-smoke quality-sla-db
./tools/runner/control-plane-smoke quality-sla-runtime-record
```

## Expected Evidence

The runner prints JSON with:

```text
scenario=canonical-ingest
evidenceLabel=unit-service-api
sourceTable=applicants
sourcePrimaryKey=19c14e85-9840-7a5a-bb16-3c7f70547d9f
sourceLsn=88432240
eventSourceOffset=<lsn/sequence/txId>
eventId=cdc_...
canonicalEventCount=1
duplicateEventCount=1
testResult=passed
```

DB-backed replay runner prints JSON with:

```text
scenario=sink-failure-replay
evidenceLabel=testcontainers-postgres
sourceTable=applicants
sourcePrimaryKey=19c14e85-9840-7a5a-bb16-3c7f70547d9f
sourceLsn=88432240
sourceOffset=<file/pos/lsn>
duplicateLedgerBlocked=true
replayStatus=REQUESTED
retryStatus=PUBLISHED
retryTopic=cdc.retry.applicants
testResult=passed
```

Recovery-state runners print JSON with:

```text
scenario=kafka-outage-recovery|connector-restart-recovery
evidenceLabel=testcontainers-postgres-recovery
recoveryScenario=KAFKA_OUTAGE|CONNECTOR_RESTART
sourceLsnFrom=<lsn>
sourceLsnTo=<lsn>
sourceOffsetFrom=<file/pos/lsn>
sourceOffsetTo=<file/pos/lsn>
eventId=cdc_...
recoveryStatus=RECOVERED
recoveredEventCount=<count>
duplicateEventCount=<count>
gapDetected=false
testResult=passed
```

Kafka-backed sink failure runner prints JSON with:

```text
scenario=kafka-backed-sink-failure-replay
evidenceLabel=embedded-kafka-broker
sourceLsn=88432400
sourceOffset=<file/pos/lsn>
retryTopic=cdc.retry.applicants
brokerPartition=<partition>
brokerOffset=<offset>
brokerAccepted=true
retryStatus=PUBLISHED
attemptCount=1
testResult=passed
```

Pipeline quality runner prints JSON with:

```text
scenario=pipeline-quality
evidenceLabel=unit-service-api
checkName=applicant-cdc-completeness
sourceTable=applicants
sourceRowCount=3
rawEventCount=3
canonicalEventCount=3
duplicateEventCount=0
missingEventCount=0
checkStatus=PASSED
sourceLsnFrom=88432240
sourceLsnTo=88432288
eventIds=[cdc_...]
testResult=passed
```

DB-backed pipeline quality runner prints JSON with:

```text
scenario=pipeline-quality-db
evidenceLabel=testcontainers-postgres-quality
checkName=applicant-cdc-completeness
sourceTable=applicants
checkStatus=PASSED
sourceLsnFrom=88432240
sourceLsnTo=88432288
latestQualityCheckId=<id>
testResult=passed
```

Data Quality SLA runner prints JSON with:

```text
scenario=quality-sla
evidenceLabel=unit-service-api-sla
checkName=applicant-cdc-sla
sourceTable=applicants
slaStatus=FAILED
completenessRatio=0.98
duplicateRatio=0.0196
freshnessLagMillis=90000
lagMillis=180000
violatedSloIds=[completeness, duplicate-rate, freshness, lag]
sourceLsnFrom=88432240
sourceLsnTo=88432320
eventIds=[cdc_...]
testResult=passed
```

DB-backed Data Quality SLA runner prints JSON with:

```text
scenario=quality-sla-db
evidenceLabel=testcontainers-postgres-sla
checkName=applicant-cdc-sla
sourceTable=applicants
slaStatus=FAILED
completenessRatio=0.98
duplicateRatio=0.0196
freshnessLagMillis=90000
lagMillis=180000
latestSlaEvaluationId=<id>
sourceLsnFrom=88432240
sourceLsnTo=88432320
eventIds=[cdc_...]
testResult=passed
```

DB-backed runtime Data Quality SLA record runner prints JSON with:

```text
scenario=quality-sla-runtime-record
evidenceLabel=testcontainers-postgres-runtime-sla
runtimeEvidenceLabel=local-docker-debezium-runtime-sla
checkName=applicant-cdc-runtime-sla
sourceTable=applicants
slaStatus=WARN
completenessRatio=1.0
duplicateRatio=0.0
freshnessLagMillis=1580
lagMillis=0
warningSloIds=[source-lsn-lag-unavailable]
sourceLsnFrom=26710752
sourceLsnTo=26711584
sourceConnectorOffsetLsn=null
sourceLsnLag=null
eventSourceOffset=<lsn/sequence/txId list>
eventIds=[cdc_...]
latestSlaEvaluationId=<id>
testResult=passed
```

## Interpretation

- `duplicateLedgerBlocked=true` proves the same source event did not create a second ledger row.
- `canonicalEventCount=1` in `canonical-ingest` proves the raw envelope was accepted for canonical ledger handoff by source metadata.
- `replayStatus=REQUESTED` proves the DLQ row was converted into an operator replay request.
- `retryStatus=PUBLISHED` proves the retry queue row moved to publish handoff state.
- Treat this as DB-backed control-plane evidence only until Kafka-backed sink failure replay is verified.
- `testcontainers-postgres-recovery` proves recovery state handling only. Treat runtime Kafka outage and Kafka Connect restart proof as pending until the CDC runtime stack is green.
- `embedded-kafka-broker` proves broker send acceptance for retry publish only. It is not Debezium runtime or Docker Compose Kafka outage proof.
- `pipeline-quality` proves unit service/API quality status behavior only.
- `pipeline-quality-db` proves DB-backed quality check persistence and latest lookup. It is not live CDC runtime quality proof.
- `quality-sla` proves Phase 2 unit service/API SLO evaluation behavior only. It is not live CDC runtime SLA observation.
- `quality-sla-db` proves DB-backed SLA persistence and latest lookup. It is not live CDC runtime SLA observation.
- `quality-sla-runtime-record` proves control-plane persistence of runtime-shaped SLA evidence. It does not run live Debezium/Kafka capture; pair it with `cdc-smoke quality-sla-runtime` when claiming local runtime observation. `project-audit` compares the two proof artifacts before keeping local-first completion green.
