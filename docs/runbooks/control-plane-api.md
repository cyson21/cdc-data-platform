# Control Plane API Runbook

## Purpose

Verify local Spring Boot control-plane API slices that do not require the full Kafka Connect runtime stack.

## Endpoints

| Method | Path | Evidence |
|---|---|---|
| `POST` | `/api/canonical-events/ingest` | raw Debezium envelope to source metadata ledger event |
| `GET` | `/api/connectors/{connectorName}/health` | latest connector status, task status, lag millis, offset summary |
| `POST` | `/api/replay-requests` | replay request id and source row metadata |
| `POST` | `/api/retry-events` | source metadata retry event scheduling |
| `POST` | `/api/retry-events/publish-ready` | ready retry event publish handoff with topic and payload metadata |
| `POST` | `/api/pipeline-quality/checks` | quality status from source/raw/canonical counts and source metadata details |
| `POST` | `/api/pipeline-quality/sla/evaluations` | freshness/completeness/duplicate/lag SLO evaluation |
| `POST` | `/api/pipeline-quality/sla/evaluations/records` | persisted SLA evaluation with ratios, thresholds, SLO ids, and source metadata |
| `POST` | `/api/pipeline-quality/sla/runtime-evaluations/records` | persisted runtime SLA observation evidence with warning SLO ids and source metadata |
| `GET` | `/api/pipeline-quality/sla/evaluations/latest` | latest persisted SLA evaluation by `checkName` and `sourceTable` |

## Verify

```bash
mvn -f backend/pom.xml -Dtest=CanonicalIngestControllerTest test
mvn -f backend/pom.xml -Dtest=ConnectorHealthHttpControllerTest test
mvn -f backend/pom.xml -Dtest=ReplayRequestControllerTest test
mvn -f backend/pom.xml -Dtest=RetryEventControllerTest test
mvn -f backend/pom.xml -Dtest=PipelineQualityControllerTest test
```

For repository-backed checks:

```bash
TESTCONTAINERS_DOCKERCONFIG_SOURCE=autoIgnoringUserProperties TESTCONTAINERS_RYUK_DISABLED=true DOCKER_HOST=unix:///Users/chanyang.son/.colima/default/docker.sock env 'api.version=1.44' mvn -f backend/pom.xml -Dtest='*ConnectorHealth*Test,*Replay*Test,*RetryEvent*Test' test
```

## Boundary

These tests prove control-plane API wiring and database behavior. The canonical ingest endpoint parses a raw Debezium envelope, derives source metadata event id/source offset, and hands it to the canonical ledger publisher. The pipeline quality endpoint records count-based quality status and preserves source metadata details such as LSN ranges and event ids. The SLA endpoints evaluate freshness, completeness, duplicate rate, and lag thresholds, store DB-backed SLA records, store runtime-shaped SLA observations with warning SLO ids, and return latest persisted SLA evidence by check name/source table. The retry publish endpoint returns retry topic and payload metadata and marks rows as `PUBLISHED`; it does not prove a Kafka broker accepted the message. Debezium runtime capture and Kafka recovery remain tied to `./tools/runner/cdc-smoke applicant-change-capture`.

For a DB-backed multi-step replay/retry smoke, use `docs/runbooks/control-plane-smoke.md`.
