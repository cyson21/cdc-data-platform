# CDC Data Platform

PostgreSQL 변경 이벤트를 Debezium으로 캡처하고 Kafka를 거쳐 downstream API와 S3/Iceberg 분석 테이블로 수렴시키는 고가용성 데이터 플랫폼 백엔드 프로젝트입니다.

이 프로젝트는 ATS/역량검사형 도메인을 차용합니다. 지원자, 공고, 평가, AI agent task처럼 변경이 잦은 데이터를 source DB에 기록하고, 변경 이벤트가 운영 API와 분석 테이블까지 안정적으로 전파되는지 검증합니다.

## Completion Gate (local-first)

- local-first MVP 기준 completion은 `project-audit` 보고서의 `completionReady=true`, `completionScope=local-first-mvp`로 판단합니다.
- `optionalCloudProofReady=false`: AWS 인증 기반 S3/Athena/dbt-athena 증명은 선택형(opt-in) 상태이며, 현재는 미완료입니다.
- Local-first 증빙으로 `MinIO`, Apache Iceberg Java API engine-backed proof, local-compatible mart validation은 주장할 수 있지만, AWS Athena/dbt-athena 실행 자체는 미완료 항목으로 분리합니다.

## 상태

기획 완료 후 local-first MVP 구현과 검증을 마쳤습니다. Project 04 `AI Gateway`는 보류하고 Project 05를 우선 진행합니다. 이 폴더는 parent hub에서 ignore되는 `repos/*` 아래에 있으며, 독립 Git repository로 초기화했습니다.

현재 구현 증거:

- Spring Boot backend skeleton과 Flyway source/control schema 검증 완료
- Debezium envelope parser, source metadata 기반 event id, canonical event model 구현 완료
- raw Debezium envelope -> canonical ingest API 검증 완료
- `cdc_event_ledger` insert-if-absent idempotency 검증 완료
- connector health/lag API 검증 완료
- replay/DLQ API shell 검증 완료
- retry scheduling/publish API shell 검증 완료
- pipeline quality check API/smoke 검증 완료
- DB-backed pipeline quality check smoke 검증 완료
- Phase 2 Data Quality SLA service/API/smoke 검증 완료
- Phase 2 Data Quality SLA DB persistence/latest lookup smoke 검증 완료
- Phase 2 Data Quality SLA local CDC runtime observation smoke 검증 완료
- Phase 2 Data Quality SLA runtime observation control-plane persistence/API smoke 검증 완료
- Phase 2 Data Quality SLA paired runtime proof artifact audit 검증 완료
- Phase 2 Data Quality SLA control-plane -> lakehouse local handoff proof 검증 완료
- Phase 2 Data Quality SLA runtime -> lakehouse replay proof와 audit gate 검증 완료
- Backfill + CDC merge local proof 검증 완료
- Multi-table CDC lineage local proof 검증 완료
- Cross-table mart lineage local validation 검증 완료
- Debezium/Kafka runtime applicant change capture smoke 검증 완료
- Kafka Connect stability smoke runner 작성 완료
- Kafka Connect long-running stability runtime proof 확보
- runtime applicant target event streaming consumer hardening 완료
- Debezium replication slot active readiness wait 완료
- Debezium null payload/tombstone parser guard 완료
- Kafka Connect offset timeout unavailable evidence 처리 완료
- Kafka Connect stability observation failure diagnostics 완료
- Kafka Connect container inspect/log tail diagnostics 완료
- connect-stability raw-capture failure metadata diagnostics 완료
- Kafka outage recovery runtime runner 작성 완료
- Kafka outage recovery runtime proof 확보
- Kafka Connect restart recovery runtime runner 작성 완료
- Kafka Connect restart recovery runtime proof 확보
- Docker runtime preflight guard 검증 완료
- DB-backed sink failure retry/replay smoke 검증 완료
- DB-backed Kafka outage and connector restart recovery-state smoke 검증 완료
- Embedded Kafka-backed retry publish broker acceptance smoke 검증 완료
- local S3-compatible object path layout runner 검증 완료
- backend canonical event -> local S3-compatible object sink 검증 완료
- S3-compatible signed PUT fixture 검증 완료
- MinIO/S3-compatible live endpoint object sink runner 검증 완료
- live MinIO-backed S3-compatible sink proof 확보
- lakehouse runtime preflight runner 검증 완료
- local MinIO compose object sink runner 검증 완료
- local Iceberg metadata bootstrap/append fixture 검증 완료
- engine-backed Iceberg bootstrap/append opt-in runner 검증 완료
- local Apache Iceberg Java API engine-backed bootstrap/append proof 확보
- local Trino/Iceberg compose catalog와 pull-never runner 검증 완료
- schema evolution fixture runner 검증 완료
- Debezium runtime schema evolution smoke 검증 완료
- local-compatible lakehouse JSONL output과 Athena/dbt-style mart SQL 초안 검증 완료
- local mart result validation runner 검증 완료
- project completion audit local-first MVP gate green
- generated local-first completion report artifact 작성 완료

Optional cloud proof:

- actual AWS S3/Athena/dbt-athena execution remains opt-in and unclaimed

## 목표

```text
Source PostgreSQL 변경
-> Debezium CDC
-> Kafka raw CDC topic
-> Spring Boot CDC Control Plane
-> canonical/retry/DLQ topic
-> Lakehouse writer
-> S3-compatible object storage + Iceberg table
-> Athena/dbt compatible mart
```

## MVP 기능

| 영역 | 기능 | 깨지면 안 되는 기준 |
|---|---|---|
| Source DB | ATS-style source schema | insert/update/delete가 CDC로 관측된다 |
| CDC | Debezium PostgreSQL connector | connector 재시작 후에도 source offset이 이어진다 |
| Kafka | raw/canonical/retry/DLQ topic | topic별 책임이 섞이지 않는다 |
| Control Plane | Spring Boot normalization/replay/health | source LSN/event id 기준 중복 처리가 된다 |
| Resilience | retry, circuit breaker, async sink worker | sink 실패가 raw event 유실로 이어지지 않는다 |
| Lakehouse | S3-compatible storage + Iceberg | curated event가 snapshot 테이블로 수렴한다 |
| Analytics | Athena/dbt compatible mart | 변경 이벤트 기반 집계가 재현 가능하다 |
| Observability | lag, DLQ, replay, quality metrics | 장애와 복구 결과가 runner/report에 남는다 |

## 비교 모드

```text
DIRECT_DB_POLLING       source DB를 주기적으로 읽는 기준선
CDC_RAW                 Debezium raw event만 관측
CDC_CANONICAL           정규화 + idempotency 적용
CDC_LAKEHOUSE_GUARDED   정규화 + retry/replay + Iceberg 수렴
```

## 기술 스택

- Java 21, Spring Boot 3, Maven
- PostgreSQL, Debezium PostgreSQL Connector, Kafka, Kafka Connect
- MinIO 또는 S3-compatible object storage
- Apache Iceberg, Trino 또는 Spark SQL local query
- dbt compatible mart runner, Athena/dbt-athena는 opt-in
- Docker Compose, Testcontainers

## 문서

- 실행 전환 runbook: `docs/runbooks/planning-to-implementation.md`
- Control Plane API runbook: `docs/runbooks/control-plane-api.md`
- Local CDC smoke: `docs/runbooks/local-cdc-smoke.md`
- Local lakehouse smoke: `docs/runbooks/lakehouse-smoke.md`
- 포트폴리오 1페이지 요약: `docs/portfolio/one-pager.md`

## 증명 범위 분리

- **Optional cloud proof (미완료):** AWS S3, Athena, dbt-athena 실행은 현재 선택형 상태이며, 성공 proof artifact가 없으면 실제 운영형 클라우드 증빙으로 주장하지 않습니다.

## 검증 명령

```bash
docker compose -f infra/local/docker-compose.yml config -q
docker compose --project-name cdc-data-platform -f infra/local/docker-compose.yml -f infra/local/docker-compose.apache-kafka.yml config -q
CDC_SMOKE_DOCKER_TIMEOUT_SECONDS=3 CDC_SMOKE_EXTRA_COMPOSE_FILES=infra/local/docker-compose.apache-kafka.yml ./tools/runner/cdc-smoke docker-preflight
python3 -m py_compile tools/runner/cdc-smoke
python3 -m py_compile tools/runner/control-plane-smoke
python3 -m py_compile tools/runner/lakehouse-smoke
python3 tools/tests/test_control_plane_smoke.py
python3 tools/tests/test_cdc_smoke_fixture.py
python3 tools/tests/test_lakehouse_smoke.py
./tools/runner/control-plane-smoke canonical-ingest
./tools/runner/control-plane-smoke pipeline-quality
./tools/runner/control-plane-smoke pipeline-quality-db
./tools/runner/control-plane-smoke quality-sla
./tools/runner/control-plane-smoke quality-sla-db
./tools/runner/control-plane-smoke quality-sla-runtime-record
./tools/runner/control-plane-smoke sink-failure-replay
./tools/runner/control-plane-smoke kafka-outage-recovery
./tools/runner/control-plane-smoke connector-restart-recovery
./tools/runner/control-plane-smoke kafka-backed-sink-failure-replay
CDC_SMOKE_EXTRA_COMPOSE_FILES=infra/local/docker-compose.apache-kafka.yml CDC_SMOKE_KAFKA_BIN_DIR=/opt/kafka/bin ./tools/runner/cdc-smoke applicant-change-capture
CDC_SMOKE_EXTRA_COMPOSE_FILES=infra/local/docker-compose.apache-kafka.yml CDC_SMOKE_KAFKA_BIN_DIR=/opt/kafka/bin CDC_SMOKE_STABILITY_SECONDS=20 ./tools/runner/cdc-smoke connect-stability
CDC_SMOKE_EXTRA_COMPOSE_FILES=infra/local/docker-compose.apache-kafka.yml CDC_SMOKE_KAFKA_BIN_DIR=/opt/kafka/bin ./tools/runner/cdc-smoke kafka-outage-recovery
CDC_SMOKE_EXTRA_COMPOSE_FILES=infra/local/docker-compose.apache-kafka.yml CDC_SMOKE_KAFKA_BIN_DIR=/opt/kafka/bin ./tools/runner/cdc-smoke connector-restart-recovery
CDC_SMOKE_EXTRA_COMPOSE_FILES=infra/local/docker-compose.apache-kafka.yml CDC_SMOKE_KAFKA_BIN_DIR=/opt/kafka/bin ./tools/runner/cdc-smoke quality-sla-runtime
./tools/runner/cdc-smoke schema-evolution-fixture
CDC_SMOKE_EXTRA_COMPOSE_FILES=infra/local/docker-compose.apache-kafka.yml CDC_SMOKE_KAFKA_BIN_DIR=/opt/kafka/bin ./tools/runner/cdc-smoke schema-evolution-runtime
./tools/runner/lakehouse-smoke iceberg-convergence
./tools/runner/lakehouse-smoke mart-result-validation
./tools/runner/lakehouse-smoke mart-lineage-validation
./tools/runner/lakehouse-smoke object-storage-sink
./tools/runner/lakehouse-smoke control-plane-runtime-handoff
./tools/runner/lakehouse-smoke runtime-lakehouse-replay
./tools/runner/lakehouse-smoke backfill-cdc-merge
./tools/runner/lakehouse-smoke multi-table-cdc-lineage
./tools/runner/lakehouse-smoke canonical-object-sink
./tools/runner/lakehouse-smoke s3-compatible-put-fixture
./tools/runner/lakehouse-smoke lakehouse-runtime-preflight
./tools/runner/lakehouse-smoke minio-compose-object-sink
CDC_LAKEHOUSE_S3_ENDPOINT=http://127.0.0.1:9000 CDC_LAKEHOUSE_S3_ACCESS_KEY=minioadmin CDC_LAKEHOUSE_S3_SECRET_KEY=minioadmin ./tools/runner/lakehouse-smoke minio-object-sink
./tools/runner/lakehouse-smoke iceberg-metadata-bootstrap
./tools/runner/lakehouse-smoke iceberg-append-fixture
CDC_LAKEHOUSE_ICEBERG_ENGINE_CMD=./tools/runner/iceberg-java-engine ./tools/runner/lakehouse-smoke iceberg-engine-bootstrap
CDC_LAKEHOUSE_ICEBERG_ENGINE_CMD=./tools/runner/iceberg-java-engine ./tools/runner/lakehouse-smoke iceberg-engine-append
CDC_LAKEHOUSE_ICEBERG_ENGINE_CMD='spark-sql --conf spark.sql.catalog.cdc=org.apache.iceberg.spark.SparkCatalog' ./tools/runner/lakehouse-smoke iceberg-engine-bootstrap
CDC_LAKEHOUSE_ICEBERG_ENGINE_CMD='spark-sql --conf spark.sql.catalog.cdc=org.apache.iceberg.spark.SparkCatalog' ./tools/runner/lakehouse-smoke iceberg-engine-append
./tools/runner/lakehouse-smoke trino-compose-iceberg-bootstrap
./tools/runner/lakehouse-smoke trino-compose-iceberg-append
./tools/runner/lakehouse-smoke lakehouse-proof-audit
./tools/runner/project-audit --write-next-proof-manifest --write-completion-report --run-local-verification
TESTCONTAINERS_DOCKERCONFIG_SOURCE=autoIgnoringUserProperties TESTCONTAINERS_RYUK_DISABLED=true DOCKER_HOST=unix:///Users/chanyang.son/.colima/default/docker.sock env 'api.version=1.44' mvn -f backend/pom.xml test
```

`control-plane-smoke pipeline-quality`는 source row count, raw/canonical event count, duplicate/missing event count와 source LSN/event id details를 기반으로 quality status를 계산하는 unit service/API evidence입니다. `pipeline-quality-db`는 같은 quality check의 insert/latest lookup을 Testcontainers PostgreSQL로 검증하는 DB-backed evidence입니다. `control-plane-smoke quality-sla`는 Phase 2 Data Quality SLA slice로 freshness lag, connector lag, completeness ratio, duplicate ratio를 평가하고 source LSN range와 event ids를 proof에 남깁니다. `quality-sla-db`는 같은 SLA 평가 결과를 `pipeline_quality_sla_evaluations`에 저장하고 최신 평가를 조회하는 Testcontainers PostgreSQL evidence입니다. `cdc-smoke quality-sla-runtime`은 local Debezium/Kafka runtime에서 applicant `c/u/d` raw events를 관측해 completeness, duplicate rate, freshness lag를 계산하고 `eventSourceOffset`, source LSN, source metadata 기반 event id를 출력하며 `lakehouse/out/proofs/quality-sla-runtime.json`을 씁니다. 현재 runtime proof는 Kafka Connect `/offsets` 응답이 빈 배열이라 `source-lsn-lag-unavailable` warning을 남기며, raw Debezium event source offset은 proof에 포함됩니다. `control-plane-smoke quality-sla-runtime-record`는 같은 runtime-shaped evidence를 `pipeline_quality_sla_evaluations`에 저장하고 `lakehouse/out/proofs/quality-sla-runtime-record.json`을 쓰는 Testcontainers PostgreSQL proof입니다. 이 runner는 같은 proof dir에 runtime artifact가 있으면 그 source LSN/event id/source offset을 그대로 사용하고, live Debezium/Kafka capture를 다시 실행하지 않습니다. `project-audit`는 두 artifact를 paired evidence로 비교해 `phase2RuntimeSlaProofAudit`에 노출합니다. `lakehouse-smoke control-plane-runtime-handoff`는 persisted runtime SLA proof를 local S3-compatible JSONL object `control-plane/pipeline_quality_sla/event_date=2026-06-12/part-0001.jsonl`로 쓰고 source LSN/event id/event source offset을 보존합니다. `lakehouse-smoke runtime-lakehouse-replay`는 그 handoff object를 두 번 replay 입력으로 처리하고 source metadata 기반 `sha256:` key로 duplicate replay를 억제해 `control-plane/pipeline_quality_sla_replay/` JSONL object를 씁니다. `project-audit`는 이 replay proof를 `phase2RuntimeLakehouseReplayAudit`에 노출합니다. `lakehouse-smoke mart-result-validation`은 curated CDC fixture event로 dbt/Athena-compatible mart SQL의 로컬 결과 의미를 검증하고 `mart_hiring_funnel_daily`, `mart_agent_task_reliability` row를 출력합니다. 실제 Athena 또는 dbt-athena 실행 증거는 아닙니다. `lakehouse-smoke minio-compose-object-sink`와 `CDC_LAKEHOUSE_ICEBERG_ENGINE_CMD=./tools/runner/iceberg-java-engine` 기반 `iceberg-engine-*` proof는 local-first MVP lakehouse gate를 충족합니다. Actual AWS S3/Athena/dbt-athena execution remains optional cloud proof and is not claimed until an explicitly configured opt-in run writes a successful proof artifact. Docker/local 수치는 reproducible local evidence이며 운영 규모 성능 증거가 아닙니다.

`lakehouse-smoke backfill-cdc-merge`는 `snapshotHighWatermarkLsn=26710000`의 historical applicant snapshot에 이후 CDC `u/d/c` event를 병합하고, source metadata 기반 `sha256:` key로 중복 CDC event를 억제합니다. Proof는 `lakehouse/out/proofs/backfill-cdc-merge.json`에 기록되며 merged current-state JSONL과 applied CDC JSONL 모두 source LSN, event source offset, event id를 보존합니다. 이는 local-compatible Backfill + CDC merge 증거이며 live MinIO/S3, Iceberg engine, Athena, dbt-athena 실행 증거는 아닙니다.

`lakehouse-smoke multi-table-cdc-lineage`는 `job_postings`, `applicants`, `evaluations`, `agent_tasks` CDC event를 applicant-centered hiring pipeline lineage row로 수렴시키고, source metadata 기반 `sha256:` key로 중복 CDC event를 억제합니다. Proof는 `lakehouse/out/proofs/multi-table-cdc-lineage.json`에 기록되며 lineage JSONL과 applied CDC JSONL 모두 source LSN, event source offset, event id를 보존합니다. 이는 local-compatible multi-table lineage 증거이며 live MinIO/S3, Iceberg engine, Athena, dbt-athena 실행 증거는 아닙니다.

`lakehouse-smoke mart-lineage-validation`은 multi-table CDC lineage proof를 입력 의미로 사용해 `mart_hiring_pipeline_lineage`의 dbt/Athena-compatible SQL ref와 local mart row를 검증합니다. Proof는 `lakehouse/out/proofs/mart-lineage-validation.json`에 기록되며 source LSN, event source offset, event id를 lineage mart 결과와 함께 보존합니다. 이는 local semantic validation이며 실제 Athena/dbt-athena 실행 증거는 아닙니다.

`project-audit --write-completion-report`는 현재 audit JSON과 proof artifacts를 기반으로 `lakehouse/out/project-audit/completion-report.md`를 생성합니다. 이 보고서는 `completionReady`, `completionScope`, `optionalCloudProofReady`, source LSN, event source offset, event id, local-first proof highlights, optional AWS Athena/dbt-athena next action을 함께 표시합니다.

## Control Plane API

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/connectors/{connectorName}/health` | latest connector/task status, lag, offset summary 조회 |
| `POST` | `/api/canonical-events/ingest` | raw Debezium envelope를 source metadata 기반 canonical ledger event로 기록 |
| `POST` | `/api/replay-requests` | DLQ event replay 요청 생성 |
| `POST` | `/api/retry-events` | source metadata 기반 retry event 예약 |
| `POST` | `/api/retry-events/publish-ready` | ready retry event를 publish 대상 상태로 전환하고 topic/payload 반환 |
| `POST` | `/api/pipeline-quality/checks` | source/raw/canonical count와 source metadata details 기반 quality check 기록 |
| `POST` | `/api/pipeline-quality/sla/evaluations` | freshness/completeness/duplicate/lag SLO 평가 |
| `POST` | `/api/pipeline-quality/sla/evaluations/records` | SLA 평가 결과 저장 |
| `POST` | `/api/pipeline-quality/sla/runtime-evaluations/records` | runtime SLA 관측 결과와 warning SLO ids 저장 |
| `GET` | `/api/pipeline-quality/sla/evaluations/latest` | check name/source table 기준 최신 SLA 평가 조회 |

## 범위 제외

- Project 04 AI Gateway 재개
- StockRush, Enterprise Policy RAG, Member Event Consistency 수정
- 처음부터 Kubernetes, 멀티 리전, 운영 규모 성능 주장
- 실제 AWS 인증이 필요한 S3/Athena/dbt-athena를 기본 검증으로 강제
- 채용 제품 UI 구현

`completionReady`는 local-first MVP와 분리되어 있으며, `optionalCloudProofReady=false` 상태에서 AWS Athena/dbt-athena는 완료 주장 범위에서 제외합니다.

## Planning Complete Definition

Project 05 기획 완료 기준은 다음 문서가 서로 모순 없이 존재하는 것이다.

| 기준 | 증거 |
|---|---|
| 포지션 요구와 프로젝트 방향 매핑 | `docs/portfolio/one-pager.md`, hub selection report |
