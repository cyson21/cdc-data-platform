# CDC Data Platform

[![CI](https://github.com/cyson21/cdc-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/cyson21/cdc-data-platform/actions/workflows/ci.yml)

Debezium으로 받은 DB 변경 이벤트를 중복 없이 쌓고, 적재에 실패하면 원인과 어디서부터 다시 돌릴지를 추적하는 Java 21 프로토타입입니다. 설계부터 구현, 테스트까지 혼자 진행한 개인 프로젝트입니다.

변경 수집, 처리 관리, 로컬 적재는 각각 만들었고, 아직 하나의 파이프라인으로 잇지는 않았습니다.

[포트폴리오](https://cyson21.github.io/projects/cdc-data-platform/) · [이력서](https://github.com/cyson21/portfolio-hub/releases/download/latest/resume.pdf)

## 풀려던 문제

Debezium은 재시작되거나 같은 변경을 다시 보내면서 이벤트가 중복될 수 있습니다. 적재에 실패했을 때 기록이 남지 않으면 무엇을 다시 돌려야 할지도 모릅니다. 그래서 원본 위치(LSN, 오프셋)로 이벤트 ID를 만들고, 처리 이력 테이블로 중복을 막고, 실패 원인과 재시도 상태를 남기게 했습니다.

## 구조

| 부분 | 흐름 | 확인한 것 |
|---|---|---|
| 변경 수집 | PostgreSQL -> Debezium -> Kafka 원본 토픽 | INSERT, UPDATE, DELETE 종류와 LSN, 오프셋이 그대로 남는지 |
| 처리 관리 | 고정 입력 -> 표준 이벤트 변환 -> 중복 방지, 재처리 | Spring 서비스와 PostgreSQL 상태 |
| 로컬 적재 | 표준 이벤트 -> 로컬 파일 저장 -> 조회용 데이터 | JSONL 저장 키와 직렬화, 로컬 결과 |

- 이벤트 ID는 수집기, 스키마, 테이블, 기본 키, LSN, 변경 종류를 합쳐서 만듭니다.
- 처리 이력 테이블에 이벤트 ID를 한 번만 기록해서, 같은 변경이 두 번 반영되지 않게 합니다.
- 재처리 요청, 재발행, 실패 원인을 각각 다른 상태로 저장해서, 복구를 요청한 것과 실제로 다시 보낸 것을 구분합니다.

## 실패 상황별 결과

| 상황 | 결과 |
|---|---|
| 같은 변경이 다시 들어옴 | 같은 이벤트 ID로 알아보고 처리 이력은 한 번만 남습니다 |
| 적재 실패 | 원인과 실패, 재처리, 재시도 상태를 따라갈 수 있습니다 |
| 데이터가 늦거나 빠짐 | 정해 둔 품질 기준으로 상태와 측정값을 기록합니다 |
| 로컬 적재를 다시 실행 | 같은 저장 키와 JSONL 구조를 유지합니다 |
| 수집기 재시작 | 변경 종류, LSN, Kafka 오프셋을 잃지 않습니다 |

## 확인한 방법

| 검증 | 확인한 내용 |
|---|---|
| 변경 수집 | Docker 환경에서 지원자 데이터 INSERT, UPDATE, DELETE의 종류, LSN, 오프셋 확인 |
| 표준 이벤트 처리 | 고정 입력을 표준 이벤트로 바꾸고, 같은 이벤트 ID가 두 번 기록되지 않는지 확인 |
| 적재 실패 재처리 | PostgreSQL에 실패, 재처리, 재시도 상태가 남고 기존 이벤트가 두 번 반영되지 않는지 확인 |
| 데이터 품질 | 누락, 중복, 최신성, 원본 지연 값으로 상태를 판정 |
| 로컬 파일 저장 | 표준 이벤트가 정해진 키의 JSONL 파일로 저장되는지, 조회용 데이터가 맞는지 확인 |

지금은 변경 수집 토픽을 처리 관리 쪽이 계속 소비하거나, 처리 결과가 로컬 적재로 자동으로 넘어가지는 않습니다. 위 결과는 각 부분 안에서만 확인한 것입니다.

## 대표 코드와 테스트

- 코드: [CanonicalIngestService](backend/src/main/java/com/example/cdcplatform/event/CanonicalIngestService.java) - 입력을 표준 이벤트로 바꾸고, 이벤트 ID를 만들고, 처리 이력으로 중복을 막습니다.
- 테스트: [CanonicalIngestServiceTest](backend/src/test/java/com/example/cdcplatform/event/CanonicalIngestServiceTest.java) - create, update, delete 변환과 같은 이벤트가 다시 왔을 때를 확인합니다.

## 실행

### 로컬 회귀 테스트 (non-cloud)

Docker 없이 관리 API, 서비스, 로컬 파일 저장을 빠르게 테스트합니다. `pom.xml`과 CI는 Java 21을 사용하며, 아래 명령은 Debezium·Kafka·PostgreSQL Testcontainers와 전체 Compose 스택을 실행하지 않습니다. 클래스 목록의 정본은 [`tools/ci_coverage_scopes.py`](tools/ci_coverage_scopes.py)입니다.

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

PostgreSQL, Kafka, Debezium 실험은 따로 띄우고 끝나면 정리합니다. CI에서는 돌리지 않습니다.

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

## 해 보지 않은 것

- 변경 수집, 처리 관리, 로컬 적재는 따로 만든 프로토타입이라, 연결되지 않은 구간의 전달은 보장하지 않습니다.
- AWS S3, Athena, dbt-athena와 Trino 기반 Iceberg는 성공적으로 돌려 본 결과가 없습니다.
- 재시도 상태 관리가 실제 Kafka에서 끝까지 전달되는 걸 보장하지는 않습니다.
- 처리량, 장시간 안정성, 스키마 변경 대응, 고가용성, 복구 시간은 재지 않았습니다.
- `lakehouse/out/`은 실행할 때마다 새로 만드는 로컬 결과물입니다.
