# CDC Data Platform

CDC Data Platform은 PostgreSQL 변경 이벤트를 Debezium으로 캡처하고 Kafka, Spring Boot control plane, S3-compatible object storage, Iceberg/mart 검증으로 수렴시키는 local-first 데이터 플랫폼 백엔드 프로젝트입니다.

## 한눈에 보기

| 항목 | 내용 |
|---|---|
| 문제 | 변경이 잦은 source DB 이벤트를 downstream API와 분석 테이블까지 유실/중복 없이 전달해야 함 |
| 핵심 역량 | Java 21, Spring Boot, Debezium, Kafka, Kafka Connect, Testcontainers, MinIO/S3-compatible, Iceberg |
| 대표 증거 | `project-audit` local-first MVP gate, runtime CDC smoke, replay/DLQ, MinIO sink, Iceberg engine-backed proof |
| 실행 기준 | `./tools/runner/project-audit`로 local-first completion 상태 확인 |
| 범위 경계 | `optionalCloudProofReady=false`; AWS S3/Athena/dbt-athena 실제 실행은 선택형 cloud proof로 분리 |

## 왜 만들었나

Spring Boot API 개발을 넘어 CDC 기반 데이터 흐름, Kafka/Debezium 운영, 장애 복구, lakehouse 수렴, mart 검증을 독립 프로젝트로 보여주기 위해 만들었습니다. StockRush는 Kafka/Saga/Outbox가 강점이지만 Debezium/Lakehouse 축이 없고, Member Event Consistency는 Kafka를 MVP에서 제외하므로 Project 05를 별도 축으로 분리했습니다.

## 핵심 설계

```text
Source PostgreSQL
-> Debezium PostgreSQL Connector
-> Kafka raw CDC topics
-> Spring Boot CDC Control Plane
     - envelope normalization
     - source LSN/event id idempotency
     - retry/replay/DLQ
     - connector lag and quality metrics
-> canonical/retry/DLQ topics
-> Lakehouse writer
-> S3-compatible object storage + Iceberg tables
-> Athena/dbt compatible mart validation
```

완료 주장은 local-first proof에 한정합니다. MinIO, Apache Iceberg Java API engine-backed proof, local-compatible mart validation은 완료 범위에 포함하지만, AWS Athena/dbt-athena 실제 실행은 성공 artifact가 생기기 전까지 완료 주장에 넣지 않습니다.

## 구현 범위

| 영역 | 구현 내용 | 증거 |
|---|---|---|
| Source DB | ATS-style applicants/job/evaluation/task schema | Flyway schema checks |
| CDC ingest | Debezium envelope parser, source metadata event id, canonical event model | `control-plane-smoke canonical-ingest` |
| Idempotency | `cdc_event_ledger` insert-if-absent, duplicate replay suppression | control-plane smoke/tests |
| Health/Lag | connector health/lag API, Kafka Connect diagnostics | connector health/lag smoke |
| Recovery | replay/DLQ API shell, retry scheduling/publish, sink failure replay | DB-backed and Embedded Kafka smoke |
| Runtime CDC | applicant insert/update/delete capture, Kafka outage, connector restart, schema evolution | `cdc-smoke` runners |
| Quality SLA | freshness/completeness/duplicate/lag evaluation and persistence | quality SLA smoke/audit |
| Lakehouse | object sink, MinIO live endpoint, Iceberg metadata/engine proof, local mart validation | `lakehouse-smoke`, proof artifacts |
| Completion audit | local-first gate, optional cloud proof separation, completion report generation | `project-audit` |

## 대표 시나리오

| 시나리오 | 검증한 문제 | 결과/증거 |
|---|---|---|
| Applicant Change Capture | 지원자 `c/u/d` 변경이 CDC topic으로 도착하는지 | Debezium op, source LSN, event id 보존 |
| Canonical Ingest | raw envelope을 downstream용 event로 정규화하는지 | canonical ledger insert |
| Sink Failure Replay | sink 실패가 raw event 유실로 이어지지 않는지 | DLQ/replay/retry handoff |
| Kafka Outage Recovery | broker 중단 후 recovery state가 보존되는지 | DB-backed + runtime runner |
| Connector Restart Recovery | restart 후 source offset continuity가 유지되는지 | runtime proof |
| Data Quality SLA | completeness, duplicate, freshness, source lag를 평가하는지 | paired runtime/control-plane proof |
| Runtime-to-Lakehouse Replay | persisted runtime SLA proof를 lakehouse object로 replay하는지 | duplicate replay suppression |
| Backfill + CDC Merge | snapshot 이후 CDC를 current-state로 병합하는지 | source metadata idempotency key |
| Multi-table Lineage | applicants/job/evaluations/tasks가 mart lineage로 수렴하는지 | local mart lineage validation |

## 빠른 실행

local-first completion 상태 확인:

```bash
./tools/runner/project-audit
```

완료 보고서와 proof manifest까지 갱신:

```bash
./tools/runner/project-audit \
  --write-next-proof-manifest \
  --write-completion-report \
  --run-local-verification
```

대표 smoke:

```bash
python3 tools/tests/test_project_audit.py
python3 tools/tests/test_control_plane_smoke.py
python3 tools/tests/test_lakehouse_smoke.py
./tools/runner/control-plane-smoke canonical-ingest
./tools/runner/control-plane-smoke pipeline-quality
./tools/runner/lakehouse-smoke mart-result-validation
./tools/runner/lakehouse-smoke lakehouse-proof-audit
```

## 검증

| 구분 | 명령/증거 | 비고 |
|---|---|---|
| Project audit | `./tools/runner/project-audit` | `completionReady=true`, `completionScope=local-first-mvp` |
| Python runner tests | `python3 tools/tests/test_project_audit.py`, `python3 tools/tests/test_control_plane_smoke.py`, `python3 tools/tests/test_lakehouse_smoke.py` | Docker 없는 로컬 검증 |
| Control plane | `./tools/runner/control-plane-smoke ...` | canonical ingest, quality, replay/recovery |
| CDC runtime | `./tools/runner/cdc-smoke ...` | Docker/Kafka/Debezium 준비 필요 |
| Lakehouse | `./tools/runner/lakehouse-smoke ...` | local object, MinIO, Iceberg, mart validation |
| Backend Maven | `TESTCONTAINERS_DOCKERCONFIG_SOURCE=autoIgnoringUserProperties ... mvn -f backend/pom.xml test` | Testcontainers/Docker 필요 |
| Optional cloud | Athena/dbt-athena opt-in scenarios | 현재 `optionalCloudProofReady=false` |

전체 명령 목록과 긴 proof 설명은 [Local CDC Smoke](docs/runbooks/local-cdc-smoke.md), [Lakehouse Smoke](docs/runbooks/lakehouse-smoke.md), [Completion Report](lakehouse/out/project-audit/completion-report.md)를 기준으로 확인합니다.

## 프로젝트 구조

```text
backend/        Spring Boot control plane, Flyway schema, Testcontainers tests
infra/local/    PostgreSQL, Kafka, Kafka Connect, Debezium, MinIO/Trino compose
tools/runner/   cdc-smoke, control-plane-smoke, lakehouse-smoke, project-audit
tools/tests/    runner unit tests
lakehouse/      local object storage, proof artifacts, mart SQL/output
docs/           design, ADR, runbooks, project tracking
```

## 문서 읽는 순서

| 순서 | 문서 | 목적 |
|---|---|---|
| 1 | [Portfolio One-Pager](docs/portfolio/one-pager.md) | local-first 완료와 optional cloud 경계 |
| 2 | [Completion Report](lakehouse/out/project-audit/completion-report.md) | `project-audit` 생성 완료 보고서 |
| 5 | [Control Plane API Runbook](docs/runbooks/control-plane-api.md) | API와 smoke 경로 |
| 6 | [Local CDC Smoke](docs/runbooks/local-cdc-smoke.md) | Debezium/Kafka runtime 검증 |
| 7 | [Lakehouse Smoke](docs/runbooks/lakehouse-smoke.md) | object/Iceberg/mart 검증 |

## 범위 밖

- Project 04 AI Gateway 재개
- StockRush, Enterprise Policy RAG, Member Event Consistency 수정
- 처음부터 Kubernetes, 멀티 리전, 운영 규모 성능 주장
- 실제 AWS 인증이 필요한 S3/Athena/dbt-athena를 기본 검증으로 강제
- 채용 제품 UI 구현
- Trino compose path를 green proof 없이 완료 주장에 포함하는 것
