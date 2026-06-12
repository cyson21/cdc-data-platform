# Runner Tools

## `cdc-smoke`

Runs local CDC smoke scenarios through Docker Compose.

```bash
./tools/runner/cdc-smoke --help
./tools/runner/cdc-smoke applicant-change-capture
./tools/runner/cdc-smoke kafka-outage-recovery
./tools/runner/cdc-smoke connector-restart-recovery
./tools/runner/cdc-smoke quality-sla-runtime
./tools/runner/cdc-smoke schema-evolution-runtime
```

The `applicant-change-capture` scenario starts the local source PostgreSQL, Kafka, and Kafka Connect services, registers the Debezium connector, mutates one applicant row, consumes `cdc.raw.public.applicants`, and prints CDC proof fields.

Required proof fields:

- `sourceTable`
- `sourcePrimaryKey`
- `operations`
- `sourceLsn`
- `eventId`
- `rawTopic`
- `rawEventCount`
- `canonicalEventCount`
- `sourceOffset`

`canonicalEventCount` remains `0` in this runtime runner until the Docker stack is wired to the Spring control plane. Use `control-plane-smoke canonical-ingest` for backend canonical ingest evidence.

The `schema-evolution-runtime` scenario adds `screening_score` to `applicants`, updates one applicant, and prints raw Debezium `c/u` event evidence with source LSN, source offset, event id, and the added field value. It is local Docker runtime evidence, not long-running stability proof.

The `kafka-outage-recovery` scenario inserts one applicant before Kafka outage, consumes the `c` event, stops only the Project 05 Kafka service, mutates the same applicant while Kafka is stopped, starts Kafka again, and prints `u/d` event evidence with source LSN, source offset, event id, Kafka service status snapshots, `durableOffsetResumed`, and missing/duplicate counts. A green run requires Kafka to stay up after recovery.

The `connector-restart-recovery` scenario inserts one applicant before Kafka Connect restart, consumes the `c` event, force recreates only the Kafka Connect service, mutates the same applicant after restart, and prints `u/d` event evidence with source LSN, source offset, event id, connector status snapshots, `durableOffsetResumed`, and missing/duplicate counts. A green run requires Kafka to stay up through the restart.

The `quality-sla-runtime` scenario captures one applicant `c/u/d` sequence through local Debezium/Kafka, then prints runtime SLA evidence: completeness ratio, duplicate ratio, freshness lag from Debezium `ts_ms`, source LSN range, event source offsets, and source metadata event ids. If Kafka Connect `/offsets` returns no LSN, the proof records `source-lsn-lag-unavailable` instead of claiming connector offset lag. It writes `lakehouse/out/proofs/quality-sla-runtime.json` by default; use `--proof-dir` to redirect artifacts.

## `control-plane-smoke`

Runs DB-backed control-plane smoke scenarios through Maven and Testcontainers PostgreSQL.

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

The canonical ingest scenario prints source LSN, source offset, event id, canonical event count, and duplicate event count evidence from raw Debezium envelope parsing and backend API/service wiring.

The recovery scenarios print source LSN, source offset, event id, recovered count, duplicate count, and gap flag evidence. They do not prove Kafka broker outage or Kafka Connect restart runtime behavior.

The Kafka-backed sink failure scenario prints Embedded Kafka broker partition/offset evidence for retry topic publish acceptance. It does not prove Debezium runtime capture.

The pipeline quality scenario prints count-based quality status plus source LSN range and event id evidence. The `pipeline-quality-db` scenario verifies DB persistence and latest lookup with Testcontainers PostgreSQL. The `quality-sla` scenario prints Phase 2 unit/API evidence for freshness, completeness, duplicate rate, and lag threshold evaluation. The `quality-sla-db` scenario verifies SLA persistence/latest lookup with Testcontainers PostgreSQL. The `quality-sla-runtime-record` scenario verifies that runtime-shaped SLA evidence, including unavailable source LSN lag warnings and event source offsets, can be persisted through the control-plane DB/API path. It writes `lakehouse/out/proofs/quality-sla-runtime-record.json` by default and reuses `quality-sla-runtime.json` from the same proof dir when present. Use `cdc-smoke quality-sla-runtime` for live local Debezium/Kafka runtime SLA observation.

## `lakehouse-smoke`

Runs local-compatible lakehouse writer scenarios.

```bash
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
./tools/runner/lakehouse-smoke minio-object-sink
./tools/runner/lakehouse-smoke iceberg-metadata-bootstrap
./tools/runner/lakehouse-smoke iceberg-append-fixture
CDC_LAKEHOUSE_ICEBERG_ENGINE_CMD=./tools/runner/iceberg-java-engine ./tools/runner/lakehouse-smoke iceberg-engine-bootstrap
CDC_LAKEHOUSE_ICEBERG_ENGINE_CMD=./tools/runner/iceberg-java-engine ./tools/runner/lakehouse-smoke iceberg-engine-append
CDC_LAKEHOUSE_ICEBERG_ENGINE_CMD='spark-sql --conf spark.sql.catalog.cdc=org.apache.iceberg.spark.SparkCatalog' ./tools/runner/lakehouse-smoke iceberg-engine-bootstrap
CDC_LAKEHOUSE_ICEBERG_ENGINE_CMD='spark-sql --conf spark.sql.catalog.cdc=org.apache.iceberg.spark.SparkCatalog' ./tools/runner/lakehouse-smoke iceberg-engine-append
./tools/runner/lakehouse-smoke trino-compose-iceberg-bootstrap
./tools/runner/lakehouse-smoke trino-compose-iceberg-append
./tools/runner/lakehouse-smoke athena-mart-query-plan
CDC_LAKEHOUSE_ATHENA_OPT_IN=true CDC_LAKEHOUSE_ATHENA_DATABASE=cdc_mart CDC_LAKEHOUSE_ATHENA_OUTPUT_LOCATION=s3://<bucket>/athena-results/ ./tools/runner/lakehouse-smoke athena-mart-execution
CDC_LAKEHOUSE_DBT_ATHENA_OPT_IN=true ./tools/runner/lakehouse-smoke dbt-athena-run
./tools/runner/lakehouse-smoke lakehouse-proof-audit
```

The mart result validation scenario materializes deterministic mart rows from curated CDC fixture events and validates that the dbt-style SQL references `curated_hiring_events`; it is not Athena/dbt-athena execution. The mart lineage validation scenario materializes `mart_hiring_pipeline_lineage` rows from the multi-table CDC lineage proof shape and validates that the dbt-style SQL references `curated_hiring_pipeline_lineage`; it is not Athena/dbt-athena execution. The control-plane runtime handoff scenario reads `quality-sla-runtime-record.json`, writes one local S3-compatible JSONL object under `control-plane/pipeline_quality_sla/`, and preserves source LSN, event source offsets, event ids, SLA status, and warning ids. The runtime lakehouse replay scenario reads that handoff proof, processes the same object twice, writes one replay JSONL row under `control-plane/pipeline_quality_sla_replay/`, and suppresses duplicate replay with a source metadata based `sha256:` key. The backfill CDC merge scenario merges a historical applicant snapshot with later CDC `u/d/c` events, suppresses duplicate CDC events with a source metadata based `sha256:` key, writes current-state and applied-CDC JSONL objects under `backfill/`, and writes `lakehouse/out/proofs/backfill-cdc-merge.json`. The multi-table CDC lineage scenario joins `job_postings`, `applicants`, `evaluations`, and `agent_tasks` CDC events into applicant-centered lineage rows, suppresses duplicate CDC events with a source metadata based `sha256:` key, writes lineage and applied-CDC JSONL objects under `lineage/`, and writes `lakehouse/out/proofs/multi-table-cdc-lineage.json`. The canonical object sink scenario runs the backend local filesystem sink and prints source LSN, source offset, event id, local object path, and S3-compatible URI evidence. The signed PUT fixture sends a SigV4-style PUT to a local fake S3-compatible endpoint and verifies the request body preserves CDC source metadata. The runtime preflight scenario reports local image, command, and opt-in environment readiness. The MinIO compose scenario starts only the Project 05 `minio` service with `--pull never` after confirming the image already exists, then sends signed bucket/object PUT requests and writes proof metadata under `lakehouse/out/proofs/`. The MinIO object sink scenario sends signed bucket/object PUT requests to a configured opt-in S3-compatible endpoint and writes proof metadata under `lakehouse/out/proofs/`. The Iceberg engine scenarios pass generated bootstrap/append SQL to a configured opt-in engine command and write proof metadata under `lakehouse/out/proofs/`; `tools/runner/iceberg-java-engine` is the local Apache Iceberg Java API command that writes a HadoopCatalog table without Docker. The Trino compose scenarios start `iceberg-catalog-postgres`, `minio`, and `trino` with `--pull never`, then run Trino CREATE/INSERT SQL against the `iceberg` catalog and write proof metadata under `lakehouse/out/proofs/`. The Athena query-plan scenario renders mart SQL without calling AWS, while the Athena/dbt execution scenarios require explicit opt-in environment variables before invoking `aws` or `dbt` and write proof metadata only after successful execution. The proof audit scenario reports local-first MVP proof requirements separately from optional cloud proof, accepts recorded MinIO proof only when HTTP status and source metadata are present, accepts recorded engine proof only when `engineExitCode=0` and required table/append fields are present, and exposes optional Athena/dbt proof as unclaimed until an opt-in proof artifact exists. Fake engine tests prove CLI plumbing and SQL generation only; real Iceberg/Athena/dbt proof requires local engine commands or explicitly configured AWS/dbt-athena targets.

## `project-audit`

Runs the conservative Project 05 completion gate.

```bash
./tools/runner/project-audit
./tools/runner/project-audit --write-next-proof-manifest
./tools/runner/project-audit --write-completion-report
./tools/runner/project-audit --run-local-verification
```

