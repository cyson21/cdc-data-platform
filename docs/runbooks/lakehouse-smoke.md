# Local Lakehouse Smoke Runbook

## Purpose

Generate local-compatible curated event output and mart-shaped evidence for the CDC Data Platform MVP.

## Scope

This runner writes JSONL, local S3-compatible object layout, control-plane runtime SLA handoff output, local runtime replay output, backfill CDC merge output, multi-table CDC lineage output, local mart lineage validation output, signed PUT fixture evidence, lakehouse runtime preflight evidence, opt-in S3-compatible endpoint evidence, local MinIO compose endpoint evidence, local Iceberg metadata fixture evidence, opt-in Iceberg engine SQL evidence, Trino compose Iceberg SQL evidence, Athena SQL query-plan evidence, dbt-athena CLI plumbing evidence, lakehouse proof audit output, and mart JSON outputs. Local metadata fixtures are not engine-validated Iceberg tables, and Athena/dbt-athena execution remains opt-in. Live MinIO/S3 proof requires a local MinIO image or a configured S3-compatible endpoint. Engine-backed Iceberg proof requires `CDC_LAKEHOUSE_ICEBERG_ENGINE_CMD` or the local MinIO/PostgreSQL/Trino images needed by the Trino compose runners.

## Preflight

```bash
python3 -m py_compile tools/runner/lakehouse-smoke
python3 tools/tests/test_lakehouse_smoke.py
```

## Run

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
CDC_LAKEHOUSE_S3_ENDPOINT=http://127.0.0.1:9000 CDC_LAKEHOUSE_S3_ACCESS_KEY=minioadmin CDC_LAKEHOUSE_S3_SECRET_KEY=minioadmin ./tools/runner/lakehouse-smoke minio-object-sink
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

## Expected Evidence

The runner prints JSON with:

```text
scenario=iceberg-convergence
evidenceLabel=local-compatible
localObjectPath=<local JSONL path>
snapshotVersionId=local-snapshot-...
curatedEventCount=3
martRowCounts.mart_hiring_funnel_daily>=1
martRowCounts.mart_agent_task_reliability>=1
```

For `mart-result-validation`, the runner prints JSON with:

```text
scenario=mart-result-validation
evidenceLabel=dbt-athena-compatible-local-mart
curatedEventCount=3
rowCounts.mart_hiring_funnel_daily=1
rowCounts.mart_agent_task_reliability=1
martResults.mart_hiring_funnel_daily[0].appliedEvents=1
martResults.mart_hiring_funnel_daily[0].screeningEvents=1
martResults.mart_agent_task_reliability[0].completionRate=1.0
sqlRefsValidated=true
```

For `mart-lineage-validation`, the runner uses the local multi-table CDC lineage proof shape and prints JSON with:

```text
scenario=mart-lineage-validation
evidenceLabel=dbt-athena-compatible-local-lineage-mart
sourceProofScenario=multi-table-cdc-lineage
lineageRecordCount=2
rowCounts.mart_hiring_pipeline_lineage=1
martResults.mart_hiring_pipeline_lineage[0].totalApplicants=2
martResults.mart_hiring_pipeline_lineage[0].completeLineageApplicants=1
martResults.mart_hiring_pipeline_lineage[0].lineageCompletionRate=0.5
sourceLsn=[26720000,26720100,26720200,26720300,26720400,26720500]
eventSourceOffset=<lsn/sequence/txId list>
eventIds=[cdc_lineage_job_posting_created,...]
sqlRefsValidated=true
```

This is local semantic validation for dbt/Athena-compatible lineage mart SQL. It is not Athena or dbt-athena runtime proof.

For `object-storage-sink`, the runner prints JSON with:

```text
scenario=object-storage-sink
evidenceLabel=local-s3-compatible
bucket=cdc-lakehouse
s3CompatibleUri=s3://cdc-lakehouse/curated/hiring_events/event_date=2026-06-09/part-0001.jsonl
objectEventCount=3
```

For `control-plane-runtime-handoff`, the runner reads `lakehouse/out/proofs/quality-sla-runtime-record.json` and prints JSON with:

```text
scenario=control-plane-runtime-handoff
evidenceLabel=local-control-plane-runtime-lakehouse-handoff
sourceProofScenario=quality-sla-runtime-record
s3CompatibleUri=s3://cdc-lakehouse/control-plane/pipeline_quality_sla/event_date=2026-06-12/part-0001.jsonl
objectRecordCount=1
sourceLsn=[...]
eventSourceOffset=<lsn/sequence/txId list>
eventIds=[cdc_...]
warningSloIds=[source-lsn-lag-unavailable]
```

This is local JSONL handoff evidence for persisted control-plane runtime SLA data. It is not live MinIO/S3, Iceberg engine, Athena, or dbt-athena proof.

For `runtime-lakehouse-replay`, the runner reads `lakehouse/out/proofs/control-plane-runtime-handoff.json`, replays the referenced handoff object twice, and prints JSON with:

```text
scenario=runtime-lakehouse-replay
evidenceLabel=local-runtime-lakehouse-replay
sourceProofScenario=control-plane-runtime-handoff
s3CompatibleUri=s3://cdc-lakehouse/control-plane/pipeline_quality_sla_replay/source_table=applicants/source_lsn_to=26711584/part-0001.jsonl
replayAttemptCount=2
inputRecordCount=2
replayedRecordCount=1
suppressedDuplicateCount=1
idempotencyKeys=[sha256:...]
sourceLsn=[...]
eventSourceOffset=<lsn/sequence/txId list>
eventIds=[cdc_...]
```

This is local replay proof for a persisted control-plane runtime SLA handoff object. Duplicate suppression is based on source metadata, not wall-clock time. It is not live MinIO/S3, Iceberg engine, Athena, or dbt-athena proof.

For `backfill-cdc-merge`, the runner merges a historical snapshot with later CDC events and prints JSON with:

```text
scenario=backfill-cdc-merge
evidenceLabel=local-backfill-cdc-merge
snapshotHighWatermarkLsn=26710000
snapshotRecordCount=2
cdcEventCount=4
appliedCdcEventCount=3
suppressedDuplicateCount=1
deletedRecordCount=1
mergedRecordCount=2
mergedS3CompatibleUri=s3://cdc-lakehouse/backfill/applicant_current_state/snapshot_date=2026-06-12/part-0001.jsonl
appliedCdcS3CompatibleUri=s3://cdc-lakehouse/backfill/applied_cdc_events/snapshot_date=2026-06-12/part-0001.jsonl
sourceLsn=[26710752,26711320,26711584]
eventSourceOffset=<lsn/sequence/txId list>
eventIds=[cdc_merge_update_applicant_001,cdc_merge_delete_applicant_002,cdc_merge_create_applicant_003]
mergeIdempotencyKeys=[sha256:...]
```

This is local Backfill + CDC merge proof. Duplicate suppression is based on source metadata, not wall-clock time. It is not live MinIO/S3, Iceberg engine, Athena, or dbt-athena proof.

For `multi-table-cdc-lineage`, the runner joins four source tables into applicant-centered lineage rows and prints JSON with:

```text
scenario=multi-table-cdc-lineage
evidenceLabel=local-multi-table-cdc-lineage
sourceTables=[agent_tasks,applicants,evaluations,job_postings]
rawCdcEventCount=7
appliedCdcEventCount=6
suppressedDuplicateCount=1
lineageRecordCount=2
lineageCompleteRecordCount=1
lineageS3CompatibleUri=s3://cdc-lakehouse/lineage/hiring_pipeline/event_date=2026-06-12/part-0001.jsonl
appliedCdcS3CompatibleUri=s3://cdc-lakehouse/lineage/applied_cdc_events/event_date=2026-06-12/part-0001.jsonl
sourceLsn=[26720000,26720100,26720200,26720300,26720400,26720500]
eventSourceOffset=<lsn/sequence/txId list>
eventIds=[cdc_lineage_job_posting_created,...]
lineageIdempotencyKeys=[sha256:...]
```

This is local multi-table CDC lineage proof. Duplicate suppression is based on source metadata, not wall-clock time. It is not live MinIO/S3, Iceberg engine, Athena, or dbt-athena proof.

For `canonical-object-sink`, the runner prints JSON with:

```text
scenario=canonical-object-sink
evidenceLabel=local-s3-compatible-backend
bucket=cdc-lakehouse
s3CompatibleUri=s3://cdc-lakehouse/curated/hiring_events/event_date=2026-06-09/part-0001.jsonl
eventId=cdc_...
sourceLsn=88432240
eventSourceOffset.lsn=88432240
eventSourceOffset.txId=773
objectEventCount=1
```

For `s3-compatible-put-fixture`, the runner prints JSON with:

```text
scenario=s3-compatible-put-fixture
evidenceLabel=s3-compatible-signed-put-fixture
httpMethod=PUT
httpStatus=200
bucket=cdc-lakehouse
s3CompatibleUri=s3://cdc-lakehouse/curated/hiring_events/event_date=2026-06-09/part-0001.jsonl
authorizationHeader=AWS4-HMAC-SHA256 ...
eventId=cdc_...
sourceLsn=88432240
eventSourceOffset.lsn=88432240
putRequestCount=1
```

For `minio-object-sink`, the runner prints JSON with:

```text
scenario=minio-object-sink
evidenceLabel=s3-compatible-live-endpoint
endpoint=<configured endpoint>
httpMethod=PUT
httpStatus=200
bucket=cdc-lakehouse
s3CompatibleUri=s3://cdc-lakehouse/curated/hiring_events/event_date=2026-06-09/part-0001.jsonl
authorizationHeader=AWS4-HMAC-SHA256 ...
eventId=cdc_...
sourceLsn=88432240
eventSourceOffset.lsn=88432240
putRequestCount=1 or 2
```

For `lakehouse-runtime-preflight`, the runner prints JSON with:

```text
scenario=lakehouse-runtime-preflight
evidenceLabel=lakehouse-runtime-preflight
docker.daemonResponsive=<true|false>
localImages.minioServer=<true|false>
commands.sparkSql=<path-or-null>
configuration.s3EndpointConfigured=<true|false>
configuration.icebergEngineCommandConfigured=<true|false>
readiness.minioObjectSinkReady=<true|false>
readiness.icebergEngineReady=<true|false>
readiness.athenaDbtReady=<true|false>
blockers=[...]
```

For `minio-compose-object-sink`, the runner prints JSON with:

```text
scenario=minio-compose-object-sink
evidenceLabel=local-minio-live-endpoint
httpMethod=PUT
httpStatus=200
bucket=cdc-lakehouse
s3CompatibleUri=s3://cdc-lakehouse/curated/hiring_events/event_date=2026-06-09/part-0001.jsonl
eventId=cdc_...
sourceLsn=88432240
eventSourceOffset.lsn=88432240
compose.pullPolicy=never
compose.service=minio
proofArtifact=lakehouse/out/proofs/minio-compose-object-sink.json
```

For `iceberg-metadata-bootstrap`, the runner prints JSON with:

```text
scenario=iceberg-metadata-bootstrap
evidenceLabel=local-iceberg-metadata
formatVersion=2
tableName=hiring_events
metadataFile=<versioned metadata json>
snapshotCount=0
```

For `iceberg-append-fixture`, the runner prints JSON with:

```text
scenario=iceberg-append-fixture
evidenceLabel=local-iceberg-metadata
metadataFile=<versioned metadata json>
dataFile=<local JSONL data file>
snapshotCount=1
appendedRecordCount=3
```

For `iceberg-engine-bootstrap`, the runner prints JSON with:

```text
scenario=iceberg-engine-bootstrap
evidenceLabel=iceberg-engine-opt-in
tableIdentifier=cdc.hiring_events
sqlFile=<generated CREATE TABLE SQL file>
engineCommand=<configured command split into argv>
engineExitCode=0
```

For `iceberg-engine-append`, the runner prints JSON with:

```text
scenario=iceberg-engine-append
evidenceLabel=iceberg-engine-opt-in
tableIdentifier=cdc.hiring_events
sqlFile=<generated INSERT SQL file>
dataFile=<local JSONL data file>
appendedRecordCount=3
engineCommand=<configured command split into argv>
engineExitCode=0
```

For `trino-compose-iceberg-bootstrap`, the runner prints JSON with:

```text
scenario=trino-compose-iceberg-bootstrap
evidenceLabel=trino-iceberg-compose-opt-in
tableIdentifier=iceberg.cdc.hiring_events
warehouseLocation=s3://cdc-lakehouse/warehouse/cdc
sqlFile=<generated Trino CREATE SCHEMA/TABLE SQL file>
engineExitCode=0
compose.pullPolicy=never
compose.services=[iceberg-catalog-postgres,minio,trino]
```

For `trino-compose-iceberg-append`, the runner prints JSON with:

```text
scenario=trino-compose-iceberg-append
evidenceLabel=trino-iceberg-compose-opt-in
tableIdentifier=iceberg.cdc.hiring_events
sqlFile=<generated Trino INSERT SQL file>
dataFile=<local JSONL data file used to build INSERT values>
appendedRecordCount=3
engineExitCode=0
compose.pullPolicy=never
```

For `athena-mart-query-plan`, the runner prints JSON with:

```text
scenario=athena-mart-query-plan
evidenceLabel=athena-dbt-opt-in-plan
curatedTableDdlFile=<generated Athena DDL file>
athenaQueryFiles=[<rendered mart SQL files>]
dbtProjectDir=lakehouse/dbt
profilesTemplate=lakehouse/dbt/profiles.yml.template
```

For `athena-mart-execution`, the runner prints JSON with:

```text
scenario=athena-mart-execution
evidenceLabel=athena-cli-opt-in
athenaDatabase=<configured database>
athenaWorkgroup=<configured workgroup>
athenaOutputLocation=<configured S3 result location>
queryExecutions[*].queryExecutionId=<Athena query id>
queryExecutions[*].state=SUCCEEDED
proofArtifact=lakehouse/out/proofs/athena-mart-execution.json
```

For `dbt-athena-run`, the runner prints JSON with:

```text
scenario=dbt-athena-run
evidenceLabel=dbt-athena-cli-opt-in
dbtProjectDir=lakehouse/dbt
profilesDir=<generated profiles dir>
target=athena
dbtExitCode=0
proofArtifact=lakehouse/out/proofs/dbt-athena-run.json
```

For `lakehouse-proof-audit`, the runner prints JSON with:

```text
scenario=lakehouse-proof-audit
evidenceLabel=lakehouse-proof-audit
auditMode=actual-live-proof-required
completionReady=<true when local-first MVP proof is satisfied>
completionScope=local-first-mvp
localMvpReady=<true|false>
optionalCloudProofReady=<true|false>
requirements[*].id=<proof requirement id>
requirements[*].status=<satisfied-local-evidence|satisfied-live-proof|ready-for-live-run|missing-live-proof|...>
requirements[*].evidenceArtifact=<proof json path when satisfied from recorded proof>
blockingUnmetRequirements=[...]
optionalUnmetRequirements=[...]
unmetRequirements=[...]
```

## Interpretation

- `localObjectPath` points to local JSONL curated events.
- `snapshotVersionId` is a snapshot-like local marker, not an Iceberg table snapshot id.
- `mart-result-validation` materializes deterministic mart rows from the curated CDC fixture and validates the mart SQL files reference `curated_hiring_events`. It is local semantic evidence, not Athena or dbt-athena runtime execution.
- `s3CompatibleUri` mirrors the bucket/key layout expected from MinIO or S3, but it is not a live object-storage write proof.
- `canonical-object-sink` runs the backend local filesystem sink and preserves source offset, source LSN, and source metadata event id in the written JSONL row. It is not live MinIO/S3 proof.
- `s3-compatible-put-fixture` sends a signed PUT to a local fake S3-compatible endpoint and preserves source metadata in the uploaded JSONL body. It is not live MinIO/S3 proof.
- `lakehouse-runtime-preflight` reads local Docker image availability, PATH command availability, and opt-in environment variables. It does not write MinIO/S3 objects, execute Iceberg SQL, or run Athena/dbt-athena.
- `minio-compose-object-sink` checks for the local MinIO image before running Compose, then starts only the Project 05 `minio` service with `--pull never`. It writes proof metadata under `lakehouse/out/proofs/` and fails without pulling when the image is absent.
- `minio-object-sink` sends signed bucket/object PUT requests to the configured S3-compatible endpoint and writes proof metadata under `lakehouse/out/proofs/`. Use local MinIO for local proof; actual AWS remains opt-in.
- `local-iceberg-metadata` output mirrors the Iceberg table metadata shape and versioned metadata-file layout, but it is not validated by an Iceberg engine and does not create Avro manifest files.
- `iceberg-engine-bootstrap` and `iceberg-engine-append` pass generated SQL to the configured opt-in engine command and write proof metadata under `lakehouse/out/proofs/`. `tools/runner/iceberg-java-engine` is the local Apache Iceberg Java API engine command; it writes a HadoopCatalog table under `lakehouse/out/iceberg-java-engine/warehouse` without Docker. Fake engine tests prove CLI plumbing and SQL generation only; real proof requires an Iceberg engine command and `engineExitCode=0` output from both scenarios.
- `trino-compose-iceberg-bootstrap` and `trino-compose-iceberg-append` use the local Compose `iceberg-catalog-postgres`, `minio`, and `trino` services with `--pull never` and write proof metadata under `lakehouse/out/proofs/` after successful SQL execution. They fail without pulling when required images are absent.
- `athena-mart-query-plan` renders Athena-compatible SQL from the dbt-style mart SQL without calling AWS. The generated DDL contains a placeholder location unless `CDC_LAKEHOUSE_ATHENA_CURATED_LOCATION` is configured.
- `athena-mart-execution` calls `aws athena start-query-execution` and polls `get-query-execution` only when `CDC_LAKEHOUSE_ATHENA_OPT_IN=true`, database, and result S3 location are configured. A successful run writes proof metadata under `lakehouse/out/proofs/`.
- `dbt-athena-run` copies `lakehouse/dbt/profiles.yml.template` into an output profiles directory and runs `dbt run` only when `CDC_LAKEHOUSE_DBT_ATHENA_OPT_IN=true`. A successful run writes proof metadata under `lakehouse/out/proofs/`.
- `lakehouse-proof-audit` reports local-first MVP proof requirements, optional cloud proof requirements, and readiness state. It accepts recorded MinIO proof artifacts only when HTTP status, event id, source LSN, and event source offset are present. It accepts recorded engine proof artifacts only when `engineExitCode=0`, a table identifier is present, and append proof includes a positive appended row count. It sets `completionReady=true` for the local-first MVP when local curated/mart evidence, MinIO/S3-compatible proof, and engine-backed Iceberg proof are satisfied. It keeps `optionalCloudProofReady=false` until an explicit Athena/dbt-athena proof artifact exists.
- Mart SQL files under `lakehouse/marts` use dbt-style `ref(...)` syntax and Athena-compatible aggregate shapes for later opt-in integration.
