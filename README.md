# CDC Data Platform

[![CI](https://github.com/cyson21/cdc-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/cyson21/cdc-data-platform/actions/workflows/ci.yml)

CDC runtime, control plane, lakehouse 구성요소를 독립적인 범위로 구현한 Java 21 프로토타입입니다. DB 변경 중복을 막고 적재 실패 원인과 재처리 상태를 원천 위치와 함께 추적하는 범위를 검증했습니다.

개인 프로젝트로 Debezium·Kafka 변경 수집, 표준 이벤트 변환, 처리 장부와 실패 재처리 API를 직접 설계·구현했습니다. 변경 수집, 처리 제어와 로컬 적재는 각각 구현했으며 하나의 상시 처리 경로로 연결하지는 않았습니다.

[웹 사례](https://cyson21.github.io/projects/cdc-data-platform/) · [전체 포트폴리오 PDF](https://github.com/cyson21/portfolio-hub/releases/download/latest/portfolio-complete.pdf) · [최신 이력서](https://github.com/cyson21/portfolio-hub/releases/download/latest/resume.pdf)

## 문제

Debezium 이벤트는 수집기 재시작이나 재전달로 중복될 수 있고, 적재 실패 뒤 재처리 이력이 사라질 수 있습니다. 원천 위치를 반영한 이벤트 ID와 처리 장부로 중복을 막고 실패 원인과 재시도 상태를 남겨야 합니다.

## 설계

| 구현 범위 | 흐름 | 확인 범위 |
|---|---|---|
| 변경 수집 | PostgreSQL -> Debezium -> Kafka 원천 토픽 | 생성·수정·삭제 종류, LSN, 오프셋 보존 |
| 처리 제어 | 고정 입력 -> 표준 이벤트 변환 -> 중복 방지·재처리 | Spring 서비스와 PostgreSQL 상태 |
| 로컬 적재 | 표준 이벤트 -> 로컬 객체 저장 -> 조회용 데이터 | JSONL 저장 키·직렬화와 로컬 결과 |

- 이벤트 ID는 수집기, 스키마, 테이블, 기본 키, LSN과 변경 종류를 조합합니다.
- 처리 장부는 이벤트 ID를 한 번만 기록해 같은 변경의 중복 반영을 막습니다.
- 재처리 요청, 재발행과 실패 원인을 별도 상태로 저장해 복구 요청과 전달 시도를 구분합니다.

## 실패 조건

| 조건 | 보호 규칙 |
|---|---|
| 같은 원천 변경 재전달 | 동일 이벤트 ID로 판정하고 처리 장부를 한 번만 기록해야 함 |
| 적재 실패 | 원인과 실패·재처리·재시도 상태를 추적할 수 있어야 함 |
| 최신성·완전성 저하 | 같은 품질 기준으로 상태와 근거 값을 기록해야 함 |
| 로컬 적재 재실행 | 정해진 저장 키와 JSONL 구조를 유지해야 함 |
| 수집기 재시작 | 변경 종류, LSN, Kafka 오프셋을 잃지 않고 관찰할 수 있어야 함 |

## 검증 결과

| 검증 | 확인 결과 |
|---|---|
| 원천 변경 캡처 | Docker CDC 실행 환경에서 지원자 생성·수정·삭제의 변경 종류, LSN과 오프셋을 확인 |
| 표준 이벤트 처리 | 고정 입력을 표준 이벤트로 변환하고 같은 이벤트 ID의 중복 기록을 차단 |
| 적재 실패 재처리 | PostgreSQL에 실패·재처리·재시도 상태를 남기고 기존 이벤트의 중복 반영을 차단 |
| 데이터 품질 | 완전성, 중복, 최신성과 원천 지연 값을 바탕으로 상태를 판정 |
| 로컬 객체 저장 | 표준 이벤트를 정해진 키의 JSONL 객체로 저장하고 조회용 고정 입력을 검증 |

변경 수집 토픽을 처리 제어가 상시 소비하거나 처리 결과가 로컬 적재로 자동 전달되는 경로는 현재 없습니다. 각 결과는 해당 구현 범위 안에서만 해석합니다.

## 대표 코드와 테스트

- 코드: [CanonicalIngestService](backend/src/main/java/com/example/cdcplatform/event/CanonicalIngestService.java) - 입력 정규화, 이벤트 ID 생성과 처리 장부의 중복 방지를 조정합니다.
- 테스트: [CanonicalIngestServiceTest](backend/src/test/java/com/example/cdcplatform/event/CanonicalIngestServiceTest.java) - create/update/delete 변환과 동일 이벤트 재전달을 검증합니다.

## 실행

### 로컬 회귀 테스트 (non-cloud)

Docker 없이 제어 API·서비스 단위·로컬 객체 저장 경로의 빠른 회귀를 실행합니다. `pom.xml`과 CI는 Java 21을 사용하며, 아래 명령은 Debezium·Kafka·PostgreSQL Testcontainers와 전체 Compose 스택을 실행하지 않습니다. 클래스 목록의 정본은 [`tools/ci_coverage_scopes.py`](tools/ci_coverage_scopes.py)입니다.

```bash
mvn -B -ntp -f backend/pom.xml clean test \
  -Dtest=CdcDataPlatformApplicationTest,CanonicalIngestControllerTest,CanonicalIngestServiceTest,CdcEventIdTest,DebeziumEnvelopeTest,ConnectorHealthControllerTest,ConnectorHealthHttpControllerTest,ConnectorHealthServiceTest,LocalLakehouseObjectSinkTest,PipelineQualityControllerTest,PipelineQualityServiceTest,ReplayRequestControllerTest,RetryEventControllerTest

python3 -m unittest \
  tools.tests.test_portfolio_evidence \
  tools.tests.test_cdc_smoke_fixture \
  tools.tests.test_lakehouse_smoke \
  tools.tests.test_ci_coverage_scopes
```

`tools.tests.test_project_audit`는 gitignore된 `TODO.md`와 로컬 proof 산출물에 의존하므로 CI 기본 경로에 포함하지 않습니다.

CI는 위 non-cloud 검증과 PostgreSQL Testcontainers 검증을 별도 job으로 분리합니다. AWS Athena/dbt-athena는 opt-in 환경 변수 없이는 실행되지 않습니다.

### Testcontainers 회귀 테스트

PostgreSQL Testcontainers가 필요한 JDBC·Flyway·복구 경로만 별도로 실행합니다. Docker daemon이 필요합니다.

```bash
mvn -B -ntp -f backend/pom.xml clean test \
  -Dtest=ConnectorHealthRepositoryTest,CdcEventLedgerRepositoryTest,PipelineQualityCheckRepositoryTest,PipelineQualitySlaRepositoryTest,ReplayRequestServiceTest,KafkaRetryEventPublisherTest,RecoveryRunServiceTest,SinkFailureReplayFlowTest,RetryEventServiceTest,SchemaMigrationTest
```

### 독립 실행 환경 테스트

PostgreSQL·Kafka·Debezium 실험은 독립 실행하고 종료 시 자원을 정리합니다. CI 기본 경로에 포함하지 않습니다.

```bash
(
  set -e
  trap 'docker compose --project-name cdc-data-platform -f infra/local/docker-compose.yml down' EXIT
  ./tools/runner/cdc-smoke docker-preflight
  ./tools/runner/cdc-smoke applicant-change-capture
)
```

```bash
./tools/runner/lakehouse-smoke object-storage-sink
./tools/runner/lakehouse-smoke mart-result-validation
```

전제와 정리 절차는 [Local CDC Smoke](docs/runbooks/local-cdc-smoke.md)와 [Lakehouse Smoke](docs/runbooks/lakehouse-smoke.md)를 따릅니다.

## 제한 사항

- 변경 수집, 처리 제어와 로컬 적재는 독립 프로토타입이며 연결되지 않은 구간의 전달 보장을 주장하지 않습니다.
- AWS S3·Athena·dbt-athena와 Trino Compose 기반 Iceberg 성공 결과는 포함하지 않습니다.
- 재시도 상태 모델은 실제 Kafka 브로커의 종단 전달 보장을 증명하지 않습니다.
- 처리량, 장시간 안정성, 스키마 변경 대응, 고가용성과 운영 복구 시간은 측정하지 않았습니다.
- `lakehouse/out/`은 실행 때 다시 만드는 로컬 작업 결과이며 영속 데이터 제품이 아닙니다.
