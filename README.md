# CDC Data Platform

PostgreSQL의 변경 이벤트를 Debezium으로 수집하고, source metadata를 기준으로 정규화·중복 억제·복구 상태를 관리하는 Java 21 기반 데이터 플랫폼 구현 프로젝트입니다. 로컬 CDC 실행 환경, Spring Boot control plane, lakehouse 저장 경로는 각각 독립적으로 검증합니다.

## 포트폴리오 링크

- [웹 사례](https://cyson21.github.io/projects/cdc-data-platform/) · [전체 포트폴리오 PDF](https://github.com/cyson21/portfolio-hub/releases/download/latest/portfolio-complete.pdf) · [최신 이력서](https://github.com/cyson21/portfolio-hub/releases/download/latest/resume.pdf)

## 한눈에 보기

| 항목 | 내용 |
|---|---|
| 문제 | 원본 DB 변경을 추적하면서 재전달·재시작에 따른 중복과 sink 실패를 관찰하고 복구할 수 있어야 함 |
| 핵심 구현 | Debezium envelope 정규화, source metadata 기반 event id, ledger 중복 억제, retry·DLQ·replay, 품질 지표, 로컬 object sink |
| 기술 | Java 21, Spring Boot 3.5, PostgreSQL, Kafka, Debezium, Testcontainers, Python 실행 도구 |
| 검증 방식 | JVM 단위 테스트, PostgreSQL Testcontainers 통합 테스트, Docker CDC 스모크, 로컬 파일 기반 lakehouse 스모크를 독립 실행 |
| 미검증 | AWS S3·Athena·dbt-athena, Trino Compose 성공 경로, 운영 규모 부하·고가용성 |

## 아키텍처와 검증 경계

```text
[A. Docker CDC runtime]
Source PostgreSQL
  -> Debezium PostgreSQL Connector
  -> Kafka raw CDC topic

[B. Spring Boot control plane]
Debezium envelope fixture -> CanonicalIngestService -> event id -> cdc_event_ledger
Replay request fixture    -> ReplayRequestService -> replay records
Retry event fixture       -> RetryEventService -> retry records
Quality input             -> PipelineQualityService -> quality records

[C. Local lakehouse experiments]
Canonical event fixture  -> LocalLakehouseObjectSink -> JSONL object
Independent runner input -> MinIO-compatible path / Iceberg fixtures / mart validation
```

- A는 [Docker Compose](infra/local/docker-compose.yml), [Debezium connector 설정](infra/local/debezium-postgres-connector.json), [CDC 실행 도구](tools/runner/cdc-smoke)로 검증합니다.
- B는 Spring 서비스·저장소 코드와 JVM/Testcontainers 테스트로 검증합니다. 현재 A의 Kafka topic을 B가 상시 소비하는 런타임 연결은 없습니다.
- C는 로컬 object sink와 lakehouse 실행 도구로 독립 검증합니다. Trino와 AWS 실행 성공을 전제로 하지 않습니다.

## 핵심 설계 판단

| 결정 | 구현 이유 | 코드 | 테스트 |
|---|---|---|---|
| source metadata 기반 event id | connector 재시작과 동일 변경 재전달 때 같은 이벤트를 식별하기 위해 connector·schema·table·PK·LSN·operation을 입력으로 사용 | [CdcEventId.java](backend/src/main/java/com/example/cdcplatform/event/CdcEventId.java), [CanonicalIngestService.java](backend/src/main/java/com/example/cdcplatform/event/CanonicalIngestService.java) | [CdcEventIdTest.java](backend/src/test/java/com/example/cdcplatform/event/CdcEventIdTest.java), [CanonicalIngestServiceTest.java](backend/src/test/java/com/example/cdcplatform/event/CanonicalIngestServiceTest.java) |
| ledger insert-if-absent로 중복 억제 | canonical 처리 결과를 event id 기준으로 한 번만 기록해 같은 이벤트의 중복 반영을 막음 | [CanonicalEventPublisher.java](backend/src/main/java/com/example/cdcplatform/event/CanonicalEventPublisher.java), [CdcEventLedgerRepository.java](backend/src/main/java/com/example/cdcplatform/ledger/CdcEventLedgerRepository.java) | [CdcEventLedgerRepositoryTest.java](backend/src/test/java/com/example/cdcplatform/ledger/CdcEventLedgerRepositoryTest.java) |
| replay와 retry 상태 분리 | 실패 원인과 재처리 요청을 DB에 남기고, 발행 재시도 상태를 별도로 추적하기 위함 | [ReplayRequestService.java](backend/src/main/java/com/example/cdcplatform/replay/ReplayRequestService.java), [RetryEventService.java](backend/src/main/java/com/example/cdcplatform/retry/RetryEventService.java), [KafkaRetryEventPublisher.java](backend/src/main/java/com/example/cdcplatform/retry/KafkaRetryEventPublisher.java) | [ReplayRequestServiceTest.java](backend/src/test/java/com/example/cdcplatform/replay/ReplayRequestServiceTest.java), [SinkFailureReplayFlowTest.java](backend/src/test/java/com/example/cdcplatform/resilience/SinkFailureReplayFlowTest.java), [KafkaRetryEventPublisherTest.java](backend/src/test/java/com/example/cdcplatform/resilience/KafkaRetryEventPublisherTest.java) |
| 품질 상태를 별도 레코드로 관리 | freshness·completeness·duplicate·source lag를 동일한 판단 기준으로 기록하기 위함 | [PipelineQualityService.java](backend/src/main/java/com/example/cdcplatform/quality/PipelineQualityService.java) | [PipelineQualityServiceTest.java](backend/src/test/java/com/example/cdcplatform/quality/PipelineQualityServiceTest.java) |
| lakehouse 저장을 교체 가능한 경계로 분리 | 클라우드 인증 없이 canonical event의 직렬화와 object key 규칙을 검증하기 위함 | [LocalLakehouseObjectSink.java](backend/src/main/java/com/example/cdcplatform/lakehouse/LocalLakehouseObjectSink.java) | [LocalLakehouseObjectSinkTest.java](backend/src/test/java/com/example/cdcplatform/lakehouse/LocalLakehouseObjectSinkTest.java) |

## 검증 시나리오

| 시나리오 | 관찰 대상 | 실행 근거 | 증명하지 않는 범위 |
|---|---|---|---|
| Applicant change capture | 지원자 `create/update/delete` 변경의 Debezium operation·LSN·offset 보존 | [cdc-smoke](tools/runner/cdc-smoke) `applicant-change-capture`, [실행 안내](docs/runbooks/local-cdc-smoke.md) | Spring control plane 또는 lakehouse 자동 전달 |
| Canonical ingest | raw envelope을 canonical event로 변환하고 동일 event id를 중복으로 기록하지 않음 | [CanonicalIngestServiceTest.java](backend/src/test/java/com/example/cdcplatform/event/CanonicalIngestServiceTest.java), `control-plane-smoke canonical-ingest` | Kafka broker와 Debezium runtime 소비 |
| Sink failure replay | PostgreSQL에 DLQ·replay·retry 상태를 기록하고 동일 ledger insert를 차단 | [SinkFailureReplayFlowTest.java](backend/src/test/java/com/example/cdcplatform/resilience/SinkFailureReplayFlowTest.java), `control-plane-smoke sink-failure-replay` | 실제 Kafka broker의 전송 보장 |
| Pipeline quality | completeness·duplicate·freshness·source lag 입력에 따른 상태 판정 | [PipelineQualityServiceTest.java](backend/src/test/java/com/example/cdcplatform/quality/PipelineQualityServiceTest.java), `control-plane-smoke pipeline-quality` | 운영 SLA 또는 장기 추세 |
| Local object sink | canonical event를 결정된 key의 JSONL object로 저장 | [LocalLakehouseObjectSinkTest.java](backend/src/test/java/com/example/cdcplatform/lakehouse/LocalLakehouseObjectSinkTest.java), `lakehouse-smoke object-storage-sink` | Trino query, AWS S3, Athena 실행 |

## 재현 방법

### 1. Docker 없이 실행하는 JVM·도구 테스트

준비 사항: Java 21, Maven 3.x, Python 3.

```bash
mvn -f backend/pom.xml \
  -Dtest=CanonicalIngestServiceTest,CanonicalIngestControllerTest,PipelineQualityServiceTest,LocalLakehouseObjectSinkTest \
  test

python3 tools/tests/test_lakehouse_smoke.py
```

### 2. Docker가 필요한 PostgreSQL 통합 테스트

준비 사항: 실행 가능한 Docker daemon. Colima 전용 socket이나 API 버전은 일반 실행의 필수값이 아닙니다.

```bash
TESTCONTAINERS_DOCKERCONFIG_SOURCE=autoIgnoringUserProperties \
  mvn -f backend/pom.xml \
  -Dtest=CdcEventLedgerRepositoryTest,SinkFailureReplayFlowTest \
  test

python3 tools/tests/test_control_plane_smoke.py
```

`test_control_plane_smoke.py`는 여러 Maven·Testcontainers·Embedded Kafka 시나리오를 묶은 확장 검사이므로 위의 두 PostgreSQL 테스트보다 오래 걸릴 수 있습니다. 실행 도구가 적용하는 Testcontainers 환경 설정도 이 프로젝트의 로컬 검증 기준에 포함됩니다.

Colima에서 실행 도구의 기본 socket 경로가 현재 사용자 홈과 다르면 다음처럼 명시합니다.

```bash
DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock" \
  python3 tools/tests/test_control_plane_smoke.py
```

### 3. PostgreSQL·Kafka·Debezium 로컬 CDC 실행

준비 사항: Docker Compose, 로컬 포트와 메모리 여유. 실행 전 상세 전제와 정리 절차는 [Local CDC Smoke](docs/runbooks/local-cdc-smoke.md)를 확인합니다.

```bash
(
  set -e
  trap 'docker compose --project-name cdc-data-platform -f infra/local/docker-compose.yml down' EXIT
  ./tools/runner/cdc-smoke docker-preflight
  ./tools/runner/cdc-smoke applicant-change-capture
)
```

### 4. 로컬 파일 기반 lakehouse 저장 검증

다음 명령은 로컬 object와 mart fixture를 검증하며 Trino나 AWS 성공을 뜻하지 않습니다.

```bash
./tools/runner/lakehouse-smoke object-storage-sink
./tools/runner/lakehouse-smoke mart-result-validation
```

생성 결과는 `lakehouse/out/` 아래의 로컬 작업 파일이며 각 실행에서 다시 만듭니다.

## 담당 범위

개인 프로젝트로 다음 범위를 직접 설계·구현·검증했습니다.

- Debezium envelope 모델과 canonical event 변환
- source metadata event id와 PostgreSQL ledger 중복 억제
- replay·DLQ·retry API 및 상태 저장
- connector 상태와 pipeline quality 판단 로직
- 로컬 object sink와 lakehouse 검증 도구
- Docker CDC, control plane, lakehouse 단계별 실행 안내와 테스트

## 제한 사항

- Docker CDC runtime, Spring control plane, lakehouse 저장 경로는 독립 검증 단계이며 단일 종단 시스템으로 연결되어 있지 않습니다.
- AWS S3·Athena·dbt-athena와 Trino Compose 기반 Iceberg 실행 결과는 없습니다.
- 처리량, 지연 시간, 장시간 안정성, 고가용성을 측정하지 않았으며 로컬 장애 주입은 일반적인 무손실 보장을 의미하지 않습니다.
- `lakehouse/out/`의 실행 결과는 로컬 작업 파일이며 Git에서 추적하지 않습니다.

## 관련 문서

| 문서 | 내용 |
|---|---|
| [Control Plane API](docs/runbooks/control-plane-api.md) | canonical ingest, replay, retry, quality API 실행 방법 |
| [Control Plane Smoke](docs/runbooks/control-plane-smoke.md) | JVM·DB 기반 시나리오와 각 검증 경계 |
| [Local CDC Smoke](docs/runbooks/local-cdc-smoke.md) | PostgreSQL·Kafka·Debezium 실행 및 정리 절차 |
| [Lakehouse Smoke](docs/runbooks/lakehouse-smoke.md) | 로컬 object·MinIO·Iceberg·mart 시나리오 구분 |
| [Local Infrastructure](infra/local/README.md) | Docker Compose 서비스와 포트 구성 |
