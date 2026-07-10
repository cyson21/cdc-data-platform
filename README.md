# CDC Data Platform

CDC Data Platform은 PostgreSQL 변경 이벤트를 Debezium으로 수집하고 Kafka, Spring Boot control plane, S3 호환 object storage, Iceberg·mart까지 연결하는 데이터 플랫폼 백엔드 프로젝트입니다. 모든 핵심 흐름은 로컬에서 재현할 수 있습니다.

## 프로젝트 요약

| 항목 | 내용 |
|---|---|
| 해결하려는 문제 | 원본 DB의 변경 이벤트를 후속 API와 분석 테이블까지 유실과 중복 없이 전달해야 함 |
| 주요 기술 | Java 21, Spring Boot, Debezium, Kafka, Kafka Connect, Testcontainers, MinIO/S3 호환 저장소, Iceberg |
| 주요 기능 | CDC 수집, 멱등 처리 기록, retry·DLQ, 데이터 품질 검사, lakehouse 저장 |
| 확인 방법 | `project-audit`, CDC 스모크 테스트, replay·DLQ, MinIO sink, Iceberg 실행 결과 |
| 빠른 실행 | `./tools/runner/project-audit`로 로컬 MVP 완료 상태 확인 |
| 제한 사항 | `optionalCloudProofReady=false`; AWS S3·Athena·dbt-athena 실행은 별도 선택 항목 |

## 주요 내용

| 주제 | 관련 문서와 코드 | 설명 |
|---|---|---|
| 로컬 완료 상태 | `./tools/runner/project-audit`, [완료 보고서](lakehouse/out/project-audit/completion-report.md) | 로컬에서 완료된 범위와 아직 실행하지 않은 AWS 항목을 구분 |
| 이벤트 정규화와 재처리 | [Control Plane Smoke](docs/runbooks/control-plane-smoke.md), [Local CDC Smoke](docs/runbooks/local-cdc-smoke.md) | Debezium envelope 정규화, retry, DLQ, replay 흐름 |
| Lakehouse 저장 | [Lakehouse Smoke](docs/runbooks/lakehouse-smoke.md) | object sink, Iceberg, mart 결과를 확인하는 방법 |

## 주요 코드와 테스트

| 구분 | 코드 또는 기능 | 테스트 또는 실행 방법 |
|---|---|---|
| Debezium envelope 정규화 | [CanonicalIngestServiceTest](backend/src/test/java/com/example/cdcplatform/event/CanonicalIngestServiceTest.java) | `./tools/runner/control-plane-smoke canonical-ingest` |
| retry, DLQ, replay | [ReplayRequestServiceTest](backend/src/test/java/com/example/cdcplatform/replay/ReplayRequestServiceTest.java) | `./tools/runner/control-plane-smoke pipeline-quality` |
| 품질 기준과 lakehouse object sink | [PipelineQualityServiceTest](backend/src/test/java/com/example/cdcplatform/quality/PipelineQualityServiceTest.java), [LocalLakehouseObjectSinkTest](backend/src/test/java/com/example/cdcplatform/lakehouse/LocalLakehouseObjectSinkTest.java) | `./tools/runner/lakehouse-smoke lakehouse-proof-audit` |

## 실행 환경

| 구분 | 준비 사항 | 확인할 내용 |
|---|---|---|
| 기본 로컬 | Python 실행 도구와 로컬 파일 시스템 | project audit, control plane, lakehouse 스모크 테스트 |
| CDC 실행 환경 | Docker, PostgreSQL, Kafka, Kafka Connect, Debezium | 원본 DB 변경, connector 재시작, Kafka 중단 시나리오 |
| AWS 연동 | AWS 인증과 별도 비용 | S3·Athena·dbt-athena 실행이며 현재 완료 범위에 포함하지 않음 |

## 구현 결과

| 구현 내용 | 결과 | 확인 방법 |
|---|---|---|
| 로컬 완료 점검 | `completionReady=true`, `completionScope=local-first-mvp` | `./tools/runner/project-audit` |
| CDC 정규화 | Debezium envelope을 source metadata 기반 canonical event로 변환 | `control-plane-smoke canonical-ingest` |
| 재처리와 복구 | retry, DLQ, replay, 중복 억제 흐름 제공 | control plane 스모크 테스트, 실행 도구 |
| Lakehouse 저장 | local object sink, MinIO, Iceberg metadata·engine, mart 확인을 분리 | `lakehouse-smoke`, 실행 결과 파일 |

## 프로젝트 배경

Spring Boot API 개발을 넘어 CDC 데이터 흐름, Kafka·Debezium 운영, 장애 복구, lakehouse 저장, mart 확인을 한 프로젝트에서 다루기 위해 만들었습니다. StockRush에는 Debezium과 lakehouse가 없고, Member Event Consistency는 Kafka를 MVP에서 제외하므로 별도 프로젝트로 분리했습니다.

## 주요 설계

| 설계 | 선택 이유 | 구현과 테스트 |
|---|---|---|
| Source metadata 기반 event id | Debezium replay와 connector 재시작 때 중복 반영을 줄이기 위함 | `cdc_event_ledger`, 중복 이벤트 스모크 테스트 |
| Retry와 DLQ 분리 | sink 장애가 raw event 유실이나 전체 pipeline 중단으로 번지지 않게 하기 위함 | replay·DLQ 스모크 테스트 |
| 데이터 품질 기준 | freshness, completeness, duplicate, lag를 완료 판단에 사용하기 위함 | pipeline quality 스모크 테스트 |
| 로컬 lakehouse | 클라우드 인증 없이 object·Iceberg·mart 흐름을 확인하기 위함 | lakehouse 스모크 테스트, project audit |

## 아키텍처

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
-> Athena/dbt compatible mart
```

완료 범위는 로컬 실행 결과로 한정합니다. MinIO, Apache Iceberg Java API, 로컬 mart 확인은 포함하지만 AWS Athena와 dbt-athena는 실제 실행 결과가 생기기 전까지 완료 항목에 넣지 않습니다.

## 구현 범위

| 영역 | 구현 내용 | 확인 방법 |
|---|---|---|
| Source DB | 지원자·공고·평가·작업 schema | Flyway schema 검사 |
| CDC ingest | Debezium envelope parser, source metadata event id, canonical event model | `control-plane-smoke canonical-ingest` |
| 멱등 처리 | `cdc_event_ledger` insert-if-absent, 중복 replay 억제 | control plane 스모크 테스트 |
| 상태와 지연 | connector 상태·지연 API, Kafka Connect 진단 | connector 상태·지연 스모크 테스트 |
| 복구 | replay·DLQ API, retry 예약·발행, sink 실패 replay | DB 기반 테스트와 Embedded Kafka 스모크 테스트 |
| CDC 실행 | 지원자 insert·update·delete 수집, Kafka 중단, connector 재시작, schema 변경 | `cdc-smoke` 실행 도구 |
| 데이터 품질 | freshness·completeness·duplicate·lag 평가와 저장 | 데이터 품질 스모크 테스트와 점검 |
| Lakehouse | object sink, MinIO endpoint, Iceberg metadata·engine, 로컬 mart 확인 | `lakehouse-smoke`, 실행 결과 파일 |
| 완료 점검 | 로컬 완료 범위, AWS 선택 항목, 완료 보고서 생성 | `project-audit` |

## 대표 시나리오

| 시나리오 | 확인 내용 | 결과 |
|---|---|---|
| Applicant Change Capture | 지원자 `c/u/d` 변경이 CDC topic으로 도착하는지 | Debezium op, source LSN, event id 보존 |
| Canonical Ingest | raw envelope을 downstream용 event로 정규화하는지 | canonical ledger insert |
| Sink Failure Replay | sink 실패가 raw event 유실로 이어지지 않는지 | DLQ/replay/retry handoff |
| Kafka Outage Recovery | broker 중단 후 recovery state가 보존되는지 | DB-backed + runtime runner |
| Connector Restart Recovery | 재시작 후 source offset이 이어지는지 | 실행 결과 |
| Data Quality SLA | completeness, duplicate, freshness, source lag를 평가하는지 | CDC 실행 결과와 control plane 결과를 함께 확인 |
| Runtime-to-Lakehouse Replay | 저장된 품질 검사 결과를 lakehouse object로 replay하는지 | 중복 replay 억제 |
| Backfill + CDC Merge | snapshot 이후 CDC를 current-state로 병합하는지 | source metadata idempotency key |
| Multi-table Lineage | applicants·job·evaluations·tasks가 mart lineage로 이어지는지 | 로컬 mart lineage 확인 |

## 부하 및 동시성 테스트

| 테스트 항목 | 방법 | 결과 | 확인 방법 |
|---|---|---|---|
| duplicate replay | 같은 source event 재처리 | ledger 기반 중복 반영 억제 | control-plane smoke |
| connector 재시작 | Debezium·Kafka 실행 환경 재시작 | offset 연속성과 복구 상태 확인 | `cdc-smoke` 실행 결과 |
| Kafka outage | broker 장애 후 pipeline 복구 | raw/canonical/retry 흐름 보존 | DB-backed + runtime runner |
| mart lineage | multi-table event를 분석 테이블로 수렴 | applicants/job/evaluations/tasks lineage 검증 | lakehouse smoke |

## 기술 선택과 문제 해결

| 주제 | 고민한 점 | 적용 내용 | 확인 방법과 남은 과제 |
|---|---|---|---|
| Debezium CDC 분리 | 애플리케이션 outbox와 달리 원본 DB의 변경 전체를 수집해야 함 | Debezium PostgreSQL Connector | CDC 실행 스모크 테스트 |
| 로컬 실행 우선 | 클라우드 인증이 기본 테스트를 막으면 반복 실행하기 어려움 | MinIO, Iceberg, 로컬 mart를 우선 사용 | `project-audit`, lakehouse 스모크 테스트 |
| AWS 실행 분리 | S3·Athena·dbt-athena 성공 결과 없이는 완료로 볼 수 없음 | `optionalCloudProofReady=false` 유지 | AWS 연동은 선택형 후속 작업 |
| 메모리 제한 | Trino와 Kafka 로컬 실행은 메모리를 많이 사용함 | 로컬 점검, CDC 실행, lakehouse, AWS 실행을 구분 | 실행하지 못한 항목은 별도 기록 |

## 빠른 실행

로컬 완료 상태 확인:

```bash
./tools/runner/project-audit
```

완료 보고서와 실행 결과 목록까지 갱신:

```bash
./tools/runner/project-audit \
  --write-next-proof-manifest \
  --write-completion-report \
  --run-local-verification
```

주요 스모크 테스트:

```bash
python3 tools/tests/test_project_audit.py
python3 tools/tests/test_control_plane_smoke.py
python3 tools/tests/test_lakehouse_smoke.py
./tools/runner/control-plane-smoke canonical-ingest
./tools/runner/control-plane-smoke pipeline-quality
./tools/runner/lakehouse-smoke mart-result-validation
./tools/runner/lakehouse-smoke lakehouse-proof-audit
```

## 테스트

| 구분 | 명령 또는 결과 | 설명 |
|---|---|---|
| 프로젝트 점검 | `./tools/runner/project-audit` | `completionReady=true`, `completionScope=local-first-mvp` |
| Python 실행 도구 테스트 | `python3 tools/tests/test_project_audit.py`, `python3 tools/tests/test_control_plane_smoke.py`, `python3 tools/tests/test_lakehouse_smoke.py` | Docker가 필요 없는 로컬 테스트 |
| Control plane | `./tools/runner/control-plane-smoke ...` | canonical ingest, 품질, replay·복구 |
| CDC 실행 | `./tools/runner/cdc-smoke ...` | Docker, Kafka, Debezium 준비 필요 |
| Lakehouse | `./tools/runner/lakehouse-smoke ...` | local object, MinIO, Iceberg, mart 확인 |
| 백엔드 Maven | `TESTCONTAINERS_DOCKERCONFIG_SOURCE=autoIgnoringUserProperties ... mvn -f backend/pom.xml test` | Testcontainers와 Docker 필요 |
| AWS 선택 항목 | Athena·dbt-athena 선택 시나리오 | 현재 `optionalCloudProofReady=false` |

전체 명령과 자세한 실행 결과는 [Local CDC Smoke](docs/runbooks/local-cdc-smoke.md), [Lakehouse Smoke](docs/runbooks/lakehouse-smoke.md), [완료 보고서](lakehouse/out/project-audit/completion-report.md)에서 확인할 수 있습니다.

## 운영/배포

| 항목 | 내용 | 확인 방법 |
|---|---|---|
| 로컬 점검 | 완료 상태와 보고서 생성 | `project-audit` |
| 실행 환경 | PostgreSQL, Kafka, Kafka Connect, Debezium, MinIO·Trino Compose | `infra/local/` |
| Lakehouse | object sink, MinIO, Iceberg, 로컬 mart | `lakehouse-smoke`, 실행 결과 파일 |
| AWS 선택 항목 | AWS S3·Athena·dbt-athena | 선택 시나리오, 현재 미완료 |

## 담당 범위

개인 프로젝트이며, 직접 구현하고 테스트한 범위는 다음과 같습니다.

| 분야 | 구현 내용 | 확인 방법 |
|---|---|---|
| CDC control plane | envelope 정규화, 멱등 처리, retry·DLQ | control plane 스모크 테스트 |
| 데이터 품질 | freshness, completeness, duplicate, lag 평가 | 데이터 품질 실행 결과 |
| Lakehouse 저장 | local object, Iceberg, mart 확인 | lakehouse 실행 결과 파일 |

## 프로젝트 구조

```text
backend/        Spring Boot control plane, Flyway schema, Testcontainers 테스트
infra/local/    PostgreSQL, Kafka, Kafka Connect, Debezium, MinIO·Trino Compose
tools/runner/   cdc-smoke, control-plane-smoke, lakehouse-smoke, project-audit
tools/tests/    실행 도구 단위 테스트
lakehouse/      로컬 object storage, 실행 결과 파일, mart SQL·결과
docs/           설계, ADR, 실행 안내, 프로젝트 진행 문서
```

## 참고 문서

| 순서 | 문서 | 내용 |
|---|---|---|
| 1 | [Portfolio One-Pager](docs/portfolio/one-pager.md) | 로컬 완료 범위와 AWS 선택 항목 |
| 2 | [완료 보고서](lakehouse/out/project-audit/completion-report.md) | `project-audit`가 생성한 완료 보고서 |
| 3 | [Control Plane API Runbook](docs/runbooks/control-plane-api.md) | API와 스모크 테스트 |
| 4 | [Local CDC Smoke](docs/runbooks/local-cdc-smoke.md) | Debezium·Kafka 실행 테스트 |
| 5 | [Lakehouse Smoke](docs/runbooks/lakehouse-smoke.md) | object·Iceberg·mart 확인 |

## 제한 사항

- Project 04 AI Gateway 재개
- StockRush, Enterprise Policy RAG, Member Event Consistency 수정
- Kubernetes, 멀티 리전, 운영 규모 성능 테스트
- AWS 인증이 필요한 S3·Athena·dbt-athena를 기본 테스트로 요구하지 않음
- 채용 제품 UI 구현
- Trino Compose 실행이 성공하지 않았다면 완료 항목에 포함하지 않음
