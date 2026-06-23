# CDC Data Platform One-Pager

## 한 줄 요약

PostgreSQL 변경 이벤트를 Debezium으로 캡처하고 Kafka, Spring Boot control plane, S3/Iceberg 분석 테이블로 안정적으로 수렴시키는 CDC 기반 고가용성 데이터 플랫폼 프로젝트입니다.

## 완료 상태(요약)

- Local-first MVP는 `project-audit` 기준 `completionReady=true`, `completionScope=local-first-mvp`로 정리되어 있습니다.
- `optionalCloudProofReady=false`입니다. 실제 AWS S3/Athena/dbt-athena 증명은 선택형이며, 성공 artifact가 없으므로 완료 주장으로 사용하지 않습니다.
- 증거 분리: `MinIO`/`Iceberg`/local lakehouse 런타임 증거는 완료 범위에 포함, AWS Athena/dbt-athena는 별도 클라우드 증명으로 분리합니다.

## 왜 이 프로젝트인가

마이다스아이티 포지션은 Spring Boot API 개발을 넘어 CDC 기반 데이터 파이프라인, Kafka downstream 연계, 장애 복구, S3/Iceberg/Athena/dbt 분석 환경을 요구합니다. 기존 StockRush는 Kafka/Saga/Outbox를 보여주지만 Debezium CDC와 Lakehouse가 없고, Member Event Consistency는 Kafka를 MVP에서 제외합니다. Project 05는 이 요구를 독립 축으로 직접 증명합니다.

## 문제

ATS/역량검사 서비스에서는 지원자, 공고, 평가, AI agent task 상태가 계속 바뀝니다. 이 변경이 운영 API, 분석 환경, 자동화 agent에 서로 다른 속도로 반영되면 데이터 지연, 중복 처리, 누락, 잘못된 분석 결과가 생깁니다.

## 설계

```text
Source PostgreSQL
  -> Debezium PostgreSQL Connector
  -> Kafka raw CDC topics
  -> Spring Boot CDC Control Plane
  -> canonical/retry/DLQ topics
  -> S3-compatible storage + Iceberg tables
  -> Athena/dbt compatible marts
```

## 핵심 제어장치

| 영역 | 제어장치 | 증명할 내용 |
|---|---|---|
| CDC | Debezium source offset, LSN, event id | 변경 이벤트가 유실 없이 관측됨 |
| Kafka | raw/canonical/retry/DLQ topic 분리 | downstream 책임이 섞이지 않음 |
| Idempotency | source metadata 기반 ledger | 중복 이벤트가 재반영되지 않음 |
| Recovery | retry, replay, DLQ, circuit breaker | sink 실패와 broker 중단 후 복구 |
| Lakehouse | Iceberg snapshot, mart query | 분석 테이블로 최종 수렴 |

## Proof Boundary

- **완료 주장 범위(Local-first):** MinIO live object sink, local Iceberg bootstrap/append, local lakehouse replay, local quality/runtime proof artifacts.
- **미완료 범위(Optional cloud):** AWS S3/Athena/dbt-athena 실제 실행 증거는 별도 opt-in 실행 후에만 포함합니다.

## MVP 증거

| 시나리오 | 현재 증거 | 상태 |
|---|---|---|
| Applicant Change Capture | `cdc-smoke applicant-change-capture` Apache Kafka override로 raw topic `c/u/d`, `eventSourceOffset`, source LSN, event id 출력 | Local runtime |
| Kafka Connect Stability | `cdc-smoke connect-stability` Apache Kafka override로 connector/task `RUNNING` samples, `c/u/d`, source LSN, eventSourceOffset, event id 출력 | Local runtime |
| Canonical Ingest and Idempotency | `control-plane-smoke canonical-ingest`, `CdcEventIdTest`, `DebeziumEnvelopeTest`, `CdcEventLedgerRepositoryTest` | Complete |
| Kafka Outage Recovery | `control-plane-smoke kafka-outage-recovery` source LSN/offset/event id recovery-state 검증 | DB-backed |
| Connector Restart Recovery | `control-plane-smoke connector-restart-recovery` source offset continuity recovery-state 검증 | DB-backed |
| Sink Failure Replay | `control-plane-smoke sink-failure-replay`로 ledger duplicate block, DLQ replay request, retry publish handoff 검증 | DB-backed |
| Kafka-backed Retry Publish | `control-plane-smoke kafka-backed-sink-failure-replay` broker partition/offset acceptance 검증 | Embedded Kafka |
| Pipeline Quality | `control-plane-smoke pipeline-quality`, `pipeline-quality-db` count/status와 DB persistence 검증 | Unit + DB-backed |
| Data Quality SLA | `control-plane-smoke quality-sla`, `quality-sla-db`, `quality-sla-runtime-record`, `cdc-smoke quality-sla-runtime`, `lakehouse-smoke control-plane-runtime-handoff`, `runtime-lakehouse-replay`, `project-audit` paired proof artifact check | Phase 2 unit/API + DB-backed + local runtime + local handoff + local replay |
| Backfill + CDC Merge | `lakehouse-smoke backfill-cdc-merge` historical snapshot + later CDC `u/d/c`, duplicate suppression, delete handling, source metadata 보존 | Local proof |
| Multi-table CDC Lineage | `lakehouse-smoke multi-table-cdc-lineage` `job_postings/applicants/evaluations/agent_tasks` lineage join, duplicate suppression, source metadata 보존 | Local proof |
| Mart Lineage Validation | `lakehouse-smoke mart-lineage-validation` cross-table lineage mart SQL ref와 local mart row 검증 | Local semantic proof |
| Completion Report | `project-audit --write-completion-report` local-first readiness, optional cloud state, source LSN/event source offset/event id 요약 | Generated audit report |
| Iceberg Convergence | `lakehouse-smoke iceberg-convergence` local-compatible JSONL/mart output | Local-compatible |
| S3-compatible Object Layout | `lakehouse-smoke object-storage-sink` local bucket/key layout output | Local-compatible |
| Canonical Object Sink | `lakehouse-smoke canonical-object-sink` source offset/LSN/event id 보존 object output | Local backend |
| S3-compatible PUT | `lakehouse-smoke s3-compatible-put-fixture` signed PUT request와 CDC metadata 보존 검증 | Fixture |
| Live MinIO Object Sink | `lakehouse-smoke minio-compose-object-sink` HTTP `PUT` 200, event id, source LSN, eventSourceOffset proof artifact 기록 | Local MinIO |
| Engine-backed Iceberg | `CDC_LAKEHOUSE_ICEBERG_ENGINE_CMD=./tools/runner/iceberg-java-engine` bootstrap/append metadata, snapshot, Parquet data file proof | Local Iceberg engine |
| Iceberg Metadata | `lakehouse-smoke iceberg-metadata-bootstrap`, `iceberg-append-fixture` local table metadata output | Metadata fixture |
| Schema Evolution | `cdc-smoke schema-evolution-fixture`, `schema-evolution-runtime` source offset/LSN/event id and added field output | Fixture + local runtime |

## 현재 검증된 내용

- Event id는 source connector/schema/table/primary key/LSN/operation에서 생성되며 wall-clock time에 의존하지 않습니다.
- Canonical ingest API는 raw Debezium envelope에서 source offset, source LSN, event id를 보존해 canonical ledger handoff 결과를 반환합니다.
- Ledger repository는 동일 source event 중복 insert를 기존 이벤트로 감지합니다.
- Connector health API는 최신 connector/task status, lag, offset summary를 반환합니다.
- Replay API shell은 DLQ row를 `REPLAY_REQUESTED`로 전환하고 replay request row를 생성합니다. Kafka-backed sink failure replay smoke는 아직 작성 전입니다.
- Retry API shell은 source metadata 기반 retry event를 한 번만 예약하고 ready event를 `PUBLISHED` 상태로 전환해 retry topic/payload metadata를 반환합니다.
- Pipeline quality API는 source/raw/canonical count, duplicate/missing count, source LSN range, event id details로 quality status를 계산하고, DB smoke는 `pipeline_quality_checks` insert/latest lookup을 검증합니다. 이는 live CDC runtime quality proof가 아닙니다.
- Data Quality SLA slice는 freshness lag, connector lag, completeness ratio, duplicate ratio를 평가하고 위반 SLO id와 source LSN/event id를 출력합니다. DB-backed smoke는 `pipeline_quality_sla_evaluations` insert/latest lookup까지 검증합니다. `cdc-smoke quality-sla-runtime`은 local Debezium/Kafka raw applicant `c/u/d` event에서 completeness, duplicate rate, freshness lag, event source offset, source LSN, source metadata event id를 출력합니다. `control-plane-smoke quality-sla-runtime-record`는 이 runtime-shaped evidence와 `source-lsn-lag-unavailable` warning을 control-plane DB/API 경로에 저장하는 증거입니다. `lakehouse-smoke control-plane-runtime-handoff`는 persisted runtime SLA proof를 local S3-compatible JSONL object로 쓰고 source LSN/event source offset/event ids를 보존합니다. `lakehouse-smoke runtime-lakehouse-replay`는 handoff object를 replay 입력으로 두 번 처리하고 source metadata 기반 `sha256:` key로 duplicate replay를 억제합니다. `project-audit`는 paired SLA artifact와 replay artifact의 source LSN/event id/offset LSN이 맞는지 확인합니다.
- Backfill + CDC merge proof는 `snapshotHighWatermarkLsn=26710000` historical applicant snapshot에 이후 CDC `u/d/c` event를 병합하고, source metadata 기반 `sha256:` key로 중복 CDC event를 억제하며, delete event로 제거된 row를 current-state output에서 제외합니다. Proof는 source LSN, event source offset, event id, merged current-state JSONL, applied CDC JSONL을 남깁니다.
- Multi-table CDC lineage proof는 `job_postings`, `applicants`, `evaluations`, `agent_tasks` CDC event를 applicant-centered hiring pipeline lineage row로 수렴시키고, source metadata 기반 `sha256:` key로 중복 CDC event를 억제합니다. Proof는 source LSN, event source offset, event id, source table별 event count, lineage JSONL, applied CDC JSONL을 남깁니다.
- Mart lineage validation은 `mart_hiring_pipeline_lineage` SQL이 `curated_hiring_pipeline_lineage`를 참조하는지 검증하고, multi-table lineage proof shape에서 total applicants, complete lineage applicants, passed evaluations, completed agent tasks, lineage completion rate를 local mart row로 materialize합니다. 이는 실제 Athena/dbt-athena 실행 증거가 아닙니다.
- Sink failure replay DB smoke는 같은 source event의 ledger 중복 차단, DLQ replay request, retry publish handoff를 PostgreSQL 기반으로 한 번에 검증합니다. 이는 Kafka broker acceptance 증거가 아닙니다.
- Kafka-backed retry publish smoke는 Embedded Kafka broker가 retry topic record를 수락하고 partition/offset을 반환한 뒤 DB 상태를 `PUBLISHED`로 바꾸는 흐름을 검증합니다. 이는 Debezium runtime capture 증거가 아닙니다.
- Applicant change capture smoke는 Debezium/Kafka Connect가 source PostgreSQL insert/update/delete를 raw applicant topic으로 전파하고, 각 raw event에서 `source.lsn`, `sequence`, `txId`, source metadata event id를 출력하는 local Docker runtime 증거입니다. 현재 1 CPU / 1 GB Docker VM에서는 proof 출력 후 Kafka Connect가 137로 종료될 수 있으므로 장시간 안정성 증거는 아닙니다.
- Kafka Connect stability runner는 applicant-focused Apache Kafka override에서 connector/task `RUNNING` samples, operations `c/u/d`, source LSN, event source offset, event id를 출력하는 local runtime proof를 확보했습니다.
- Recovery-state smoke는 Kafka outage와 connector restart 이후 기록되어야 할 source LSN range, source offset range, event id, duplicate count, gap flag를 PostgreSQL 기반으로 검증합니다. 이는 실제 broker outage 또는 Kafka Connect restart 증거가 아닙니다.
- Schema evolution fixture는 schema-added payload에서도 source offset, source LSN, event id가 유지되는 출력 증거를 제공합니다. Runtime smoke는 nullable column 추가 후 raw Debezium event에 `screening_score=87`, source LSN, source metadata event id가 포함됨을 local Docker로 검증합니다.
- Lakehouse runner는 local JSONL, S3-compatible bucket/key layout, mart-shaped output을 생성합니다. Live MinIO object sink와 Apache Iceberg Java API engine-backed bootstrap/append proof까지 확보했으며, actual AWS Athena/dbt-athena proof는 opt-in으로 남겨둡니다.
- Canonical object sink smoke는 backend local filesystem sink가 canonical event의 source offset, source LSN, source metadata event id를 JSONL row와 S3-compatible object key에 보존함을 검증합니다. 이는 live MinIO/S3 proof가 아닙니다.
- S3-compatible PUT fixture는 fake local endpoint로 signed PUT 요청을 보내고 source offset, source LSN, source metadata event id가 request body에 보존됨을 검증합니다. 이는 live MinIO/S3 proof가 아닙니다.
- Iceberg metadata fixture는 format v2 metadata JSON과 append snapshot metadata를 생성합니다. 이는 Iceberg engine query 증거가 아닙니다.
- Engine-backed Iceberg proof는 local Apache Iceberg Java API engine command가 HadoopCatalog table metadata와 Parquet data file을 생성하고 append snapshot을 기록한 증거입니다. Trino compose path는 현재 1GB Colima VM에서 OOM으로 green proof가 아닙니다.

## 범위 절제

- 실제 AWS S3/Athena/dbt-athena는 opt-in입니다.
- 기본 증거는 MinIO, Apache Iceberg Java API engine-backed proof, local-compatible mart validation 기반 local-first로 만듭니다. Trino compose path는 별도 환경이 준비되어 green proof가 생기기 전까지 완료 주장에 넣지 않습니다.
- 운영 규모 성능 수치는 주장하지 않고, 장애 복구와 데이터 수렴 증거에 집중합니다.
- Local-first MVP completion은 `project-audit`의 `completionReady=true`, `phase2RuntimeSlaProofAudit.completionReady=true`, `phase2RuntimeLakehouseReplayAudit.completionReady=true`로 판단하고, optional cloud proof는 `optionalCloudProofReady=false`일 수 있음을 별도로 표시합니다.
