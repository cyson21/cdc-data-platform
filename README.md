# CDC Data Platform

PostgreSQL 변경 캡처, canonical event 제어, lakehouse 저장을 각각 재현하는 Java 21 기반 데이터 플랫폼 프로토타입입니다. 세 실험은 독립 검증되며 현재 단일 end-to-end pipeline으로 연결되어 있지 않습니다. [웹 사례](https://cyson21.github.io/projects/cdc-data-platform/)

## 문제

CDC 이벤트는 connector 재시작과 재전달로 중복될 수 있고 sink 실패 뒤 재처리 이력이 사라질 수 있습니다. source 위치를 보존한 event id, 중복 ledger, retry·DLQ·replay 상태가 필요하며 각 단계가 실제로 연결된 범위를 과장하지 않아야 합니다.

## 설계

| 독립 실험 | 흐름 | 검증 경계 |
|---|---|---|
| A. CDC runtime | PostgreSQL -> Debezium -> Kafka raw topic | create/update/delete operation, LSN, offset 캡처 |
| B. Control plane | envelope fixture -> canonical ingest -> ledger/retry/replay/quality | Spring 서비스와 PostgreSQL 상태 |
| C. Lakehouse | canonical fixture -> local object sink -> mart fixture | JSONL key·직렬화와 로컬 결과 |

- event id는 connector, schema, table, primary key, LSN, operation을 조합합니다.
- ledger는 event id `insert-if-absent`로 동일 변경의 중복 반영을 막습니다.
- replay request, retry 발행, DLQ 원인을 별도 상태로 저장해 복구 요청과 전달 시도를 구분합니다.

## 실패 조건

| 조건 | 보호 규칙 |
|---|---|
| 같은 source change 재전달 | 동일 event id로 판정하고 ledger를 한 번만 기록해야 함 |
| sink 처리 실패 | 원인, DLQ, replay request, retry 상태를 추적할 수 있어야 함 |
| freshness·completeness 저하 | 동일한 quality rule로 상태와 근거 값을 기록해야 함 |
| object sink 재실행 | 결정된 object key와 JSONL 구조를 재현해야 함 |
| connector 재시작 | operation, LSN, Kafka offset을 잃지 않고 관찰할 수 있어야 함 |

## 검증 결과

| 검증 | 확인 결과 |
|---|---|
| Applicant change capture | Docker CDC runtime에서 지원자 create/update/delete의 Debezium operation·LSN·offset을 확인 |
| Canonical ingest | fixture를 canonical event로 변환하고 동일 event id의 ledger 중복 insert를 차단 |
| Sink failure replay | PostgreSQL에 DLQ·replay·retry 상태를 남기고 기존 ledger 중복 반영을 차단 |
| Pipeline quality | completeness, duplicate, freshness, source lag 입력으로 상태를 판정 |
| Local object sink | canonical event를 결정된 key의 JSONL object로 저장하고 mart fixture를 검증 |

A의 Kafka topic을 B가 상시 소비하거나 B의 결과가 C로 자동 전달되는 경로는 현재 없습니다. 각 결과는 해당 실험 경계 안에서만 해석합니다.

## 대표 코드와 테스트

- 코드: [CanonicalIngestService](backend/src/main/java/com/example/cdcplatform/event/CanonicalIngestService.java) - envelope 정규화, event id 생성, ledger 중복 억제를 조정합니다.
- 테스트: [CanonicalIngestServiceTest](backend/src/test/java/com/example/cdcplatform/event/CanonicalIngestServiceTest.java) - create/update/delete 변환과 동일 이벤트 재전달을 검증합니다.

## 실행

Docker 없이 control plane 핵심 회귀를 실행합니다.

```bash
mvn -f backend/pom.xml \
  -Dtest=CanonicalIngestServiceTest,CanonicalIngestControllerTest,PipelineQualityServiceTest,LocalLakehouseObjectSinkTest \
  test
```

PostgreSQL·Kafka·Debezium 실험은 독립 실행하고 종료 시 자원을 정리합니다.

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

- CDC runtime, control plane, lakehouse는 독립 프로토타입이며 연결되지 않은 구간의 전달 보장을 주장하지 않습니다.
- AWS S3·Athena·dbt-athena와 Trino Compose 기반 Iceberg 성공 결과는 포함하지 않습니다.
- retry 상태 모델은 실제 Kafka broker의 end-to-end delivery guarantee를 증명하지 않습니다.
- 처리량, 장시간 안정성, schema evolution, 고가용성, 운영 복구 시간은 측정하지 않았습니다.
- `lakehouse/out/`은 실행 때 다시 만드는 로컬 작업 결과이며 영속 데이터 제품이 아닙니다.
