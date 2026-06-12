#!/usr/bin/env python3
import json
import os
import runpy
import subprocess
import sys
import tempfile
import threading
import unittest
import http.server
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import unquote


ROOT = Path(__file__).resolve().parents[2]


class LakehouseSmokeTest(unittest.TestCase):

    def test_iceberg_convergence_writes_curated_events_and_mart_counts(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "iceberg-convergence",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )

            proof = json.loads(completed.stdout)

            self.assertEqual("iceberg-convergence", proof["scenario"])
            self.assertEqual("local-compatible", proof["evidenceLabel"])
            self.assertTrue(proof["snapshotVersionId"].startswith("local-snapshot-"))
            self.assertEqual(3, proof["curatedEventCount"])
            self.assertGreaterEqual(proof["martRowCounts"]["mart_hiring_funnel_daily"], 1)
            self.assertGreaterEqual(proof["martRowCounts"]["mart_agent_task_reliability"], 1)

            object_path = Path(proof["localObjectPath"])
            self.assertTrue(object_path.exists())
            self.assertEqual(3, len(object_path.read_text().splitlines()))

    def test_mart_result_validation_scenario_is_registered(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                "--help",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertIn("mart-result-validation", completed.stdout)

    def test_mart_result_validation_materializes_expected_rows_and_sql_refs(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "mart-result-validation",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )

            proof = json.loads(completed.stdout)

            self.assertEqual("mart-result-validation", proof["scenario"])
            self.assertEqual("dbt-athena-compatible-local-mart", proof["evidenceLabel"])
            self.assertEqual(3, proof["curatedEventCount"])
            self.assertEqual(1, proof["rowCounts"]["mart_hiring_funnel_daily"])
            self.assertEqual(1, proof["rowCounts"]["mart_agent_task_reliability"])
            self.assertTrue(proof["sqlRefsValidated"])
            self.assertIn("mart_hiring_funnel_daily.sql", proof["martSqlFiles"][0])
            self.assertIn("mart_agent_task_reliability.sql", proof["martSqlFiles"][1])

            funnel_row = proof["martResults"]["mart_hiring_funnel_daily"][0]
            self.assertEqual("2026-06-09", funnel_row["eventDate"])
            self.assertEqual(1, funnel_row["appliedEvents"])
            self.assertEqual(1, funnel_row["screeningEvents"])
            self.assertEqual(2, funnel_row["totalApplicantEvents"])

            reliability_row = proof["martResults"]["mart_agent_task_reliability"][0]
            self.assertEqual("2026-06-09", reliability_row["eventDate"])
            self.assertEqual(1, reliability_row["completedTaskEvents"])
            self.assertEqual(1, reliability_row["totalTaskEvents"])
            self.assertEqual(1.0, reliability_row["completionRate"])

            for path in proof["martResultFiles"].values():
                self.assertTrue(Path(path).exists())

    def test_mart_lineage_validation_scenario_is_registered(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                "--help",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertIn("mart-lineage-validation", completed.stdout)

    def test_mart_lineage_validation_materializes_expected_rows_and_sql_refs(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "mart-lineage-validation",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )

            proof = json.loads(completed.stdout)
            result_file = Path(proof["martResultFiles"]["mart_hiring_pipeline_lineage"])
            self.assertTrue(result_file.exists())
            written_rows = json.loads(result_file.read_text())

        self.assertEqual("mart-lineage-validation", proof["scenario"])
        self.assertEqual("dbt-athena-compatible-local-lineage-mart", proof["evidenceLabel"])
        self.assertEqual("multi-table-cdc-lineage", proof["sourceProofScenario"])
        self.assertEqual([
            "agent_tasks",
            "applicants",
            "evaluations",
            "job_postings",
        ], proof["sourceTables"])
        self.assertEqual(2, proof["lineageRecordCount"])
        self.assertEqual(1, proof["rowCounts"]["mart_hiring_pipeline_lineage"])
        self.assertEqual(
            [26720000, 26720100, 26720200, 26720300, 26720400, 26720500],
            proof["sourceLsn"],
        )
        self.assertEqual([
            "cdc_lineage_job_posting_created",
            "cdc_lineage_applicant_010_created",
            "cdc_lineage_applicant_010_screening",
            "cdc_lineage_evaluation_010_created",
            "cdc_lineage_agent_task_010_completed",
            "cdc_lineage_applicant_011_created",
        ], proof["eventIds"])
        self.assertTrue(proof["sqlRefsValidated"])
        self.assertIn("mart_hiring_pipeline_lineage.sql", proof["martSqlFile"])

        row = proof["martResults"]["mart_hiring_pipeline_lineage"][0]
        self.assertEqual("2026-06-12", row["eventDate"])
        self.assertEqual("job-001", row["jobPostingId"])
        self.assertEqual("Backend Platform Engineer", row["jobTitle"])
        self.assertEqual(2, row["totalApplicants"])
        self.assertEqual(1, row["completeLineageApplicants"])
        self.assertEqual(1, row["applicantOnlyCount"])
        self.assertEqual(1, row["passedEvaluations"])
        self.assertEqual(1, row["completedAgentTasks"])
        self.assertEqual(0.5, row["lineageCompletionRate"])
        self.assertEqual(row, written_rows[0])

    def test_object_storage_sink_writes_s3_compatible_layout(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "object-storage-sink",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )

            proof = json.loads(completed.stdout)

            self.assertEqual("object-storage-sink", proof["scenario"])
            self.assertEqual("local-s3-compatible", proof["evidenceLabel"])
            self.assertEqual("s3://cdc-lakehouse/curated/hiring_events/event_date=2026-06-09/part-0001.jsonl", proof["s3CompatibleUri"])
            self.assertEqual("cdc-lakehouse", proof["bucket"])
            self.assertEqual(3, proof["objectEventCount"])

            object_path = Path(proof["localObjectPath"])
            self.assertTrue(object_path.exists())
            self.assertIn("object-storage/cdc-lakehouse/curated/hiring_events/event_date=2026-06-09", str(object_path))
            self.assertEqual(3, len(object_path.read_text().splitlines()))

    def test_backfill_cdc_merge_scenario_is_registered(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                "--help",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertIn("backfill-cdc-merge", completed.stdout)

    def test_backfill_cdc_merge_writes_merged_state_with_source_metadata(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "backfill-cdc-merge",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )

            proof = json.loads(completed.stdout)
            merged_object_path = Path(proof["mergedLocalObjectPath"])
            applied_cdc_object_path = Path(proof["appliedCdcLocalObjectPath"])
            proof_artifact_payload = json.loads(Path(proof["proofArtifact"]).read_text())
            merged_rows = [json.loads(line) for line in merged_object_path.read_text().splitlines()]
            applied_cdc_rows = [json.loads(line) for line in applied_cdc_object_path.read_text().splitlines()]

        self.assertEqual("backfill-cdc-merge", proof["scenario"])
        self.assertEqual("local-backfill-cdc-merge", proof["evidenceLabel"])
        self.assertEqual("s3://cdc-lakehouse/backfill/applicant_current_state/snapshot_date=2026-06-12/part-0001.jsonl", proof["mergedS3CompatibleUri"])
        self.assertEqual("s3://cdc-lakehouse/backfill/applied_cdc_events/snapshot_date=2026-06-12/part-0001.jsonl", proof["appliedCdcS3CompatibleUri"])
        self.assertEqual(26710000, proof["snapshotHighWatermarkLsn"])
        self.assertEqual(2, proof["snapshotRecordCount"])
        self.assertEqual(4, proof["cdcEventCount"])
        self.assertEqual(3, proof["appliedCdcEventCount"])
        self.assertEqual(1, proof["suppressedDuplicateCount"])
        self.assertEqual(1, proof["deletedRecordCount"])
        self.assertEqual(2, proof["mergedRecordCount"])
        self.assertEqual([26710752, 26711320, 26711584], proof["sourceLsn"])
        self.assertEqual([
            "cdc_merge_update_applicant_001",
            "cdc_merge_delete_applicant_002",
            "cdc_merge_create_applicant_003",
        ], proof["eventIds"])
        self.assertEqual(["cdc_merge_update_applicant_001"], proof["suppressedDuplicateEventIds"])
        self.assertEqual(["applicant-002"], proof["deletedPrimaryKeys"])
        self.assertTrue(all(key.startswith("sha256:") for key in proof["mergeIdempotencyKeys"]))

        merged_by_pk = {row["sourcePrimaryKey"]: row for row in merged_rows}
        self.assertEqual({"applicant-001", "applicant-003"}, set(merged_by_pk))
        self.assertEqual("OFFER", merged_by_pk["applicant-001"]["stage"])
        self.assertEqual("APPLIED", merged_by_pk["applicant-003"]["stage"])
        self.assertEqual(26710752, merged_by_pk["applicant-001"]["lastSourceLsn"])
        self.assertEqual(26711584, merged_by_pk["applicant-003"]["lastSourceLsn"])

        self.assertEqual(3, len(applied_cdc_rows))
        for row in applied_cdc_rows:
            self.assertTrue(row["eventId"].startswith("cdc_merge_"))
            self.assertIn(row["sourceLsn"], proof["sourceLsn"])
            self.assertEqual(row["sourceLsn"], row["eventSourceOffset"]["lsn"])
            self.assertTrue(row["mergeIdempotencyKey"].startswith("sha256:"))
        self.assertEqual("backfill-cdc-merge", proof_artifact_payload["scenario"])

    def test_multi_table_cdc_lineage_scenario_is_registered(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                "--help",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertIn("multi-table-cdc-lineage", completed.stdout)

    def test_multi_table_cdc_lineage_writes_joined_rows_with_source_metadata(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "multi-table-cdc-lineage",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )

            proof = json.loads(completed.stdout)
            lineage_object_path = Path(proof["lineageLocalObjectPath"])
            applied_cdc_object_path = Path(proof["appliedCdcLocalObjectPath"])
            proof_artifact_payload = json.loads(Path(proof["proofArtifact"]).read_text())
            lineage_rows = [json.loads(line) for line in lineage_object_path.read_text().splitlines()]
            applied_cdc_rows = [json.loads(line) for line in applied_cdc_object_path.read_text().splitlines()]

        self.assertEqual("multi-table-cdc-lineage", proof["scenario"])
        self.assertEqual("local-multi-table-cdc-lineage", proof["evidenceLabel"])
        self.assertEqual([
            "agent_tasks",
            "applicants",
            "evaluations",
            "job_postings",
        ], proof["sourceTables"])
        self.assertEqual(7, proof["rawCdcEventCount"])
        self.assertEqual(6, proof["appliedCdcEventCount"])
        self.assertEqual(1, proof["suppressedDuplicateCount"])
        self.assertEqual(["cdc_lineage_evaluation_010_created"], proof["suppressedDuplicateEventIds"])
        self.assertEqual(2, proof["lineageRecordCount"])
        self.assertEqual(1, proof["lineageCompleteRecordCount"])
        self.assertEqual(
            "s3://cdc-lakehouse/lineage/hiring_pipeline/event_date=2026-06-12/part-0001.jsonl",
            proof["lineageS3CompatibleUri"],
        )
        self.assertEqual(
            "s3://cdc-lakehouse/lineage/applied_cdc_events/event_date=2026-06-12/part-0001.jsonl",
            proof["appliedCdcS3CompatibleUri"],
        )
        self.assertEqual(
            [26720000, 26720100, 26720200, 26720300, 26720400, 26720500],
            proof["sourceLsn"],
        )
        self.assertEqual([
            "cdc_lineage_job_posting_created",
            "cdc_lineage_applicant_010_created",
            "cdc_lineage_applicant_010_screening",
            "cdc_lineage_evaluation_010_created",
            "cdc_lineage_agent_task_010_completed",
            "cdc_lineage_applicant_011_created",
        ], proof["eventIds"])
        self.assertTrue(all(key.startswith("sha256:") for key in proof["lineageIdempotencyKeys"]))

        lineage_by_applicant = {row["applicantId"]: row for row in lineage_rows}
        self.assertEqual({"applicant-010", "applicant-011"}, set(lineage_by_applicant))
        complete_row = lineage_by_applicant["applicant-010"]
        self.assertEqual("COMPLETE", complete_row["lineageStatus"])
        self.assertEqual("job-001", complete_row["jobPostingId"])
        self.assertEqual("Backend Platform Engineer", complete_row["jobTitle"])
        self.assertEqual("SCREENING", complete_row["applicantStage"])
        self.assertEqual("eval-010", complete_row["evaluationId"])
        self.assertEqual("PASS", complete_row["evaluationResult"])
        self.assertEqual("task-010", complete_row["agentTaskId"])
        self.assertEqual("COMPLETED", complete_row["agentTaskStatus"])

        applicant_only_row = lineage_by_applicant["applicant-011"]
        self.assertEqual("APPLICANT_ONLY", applicant_only_row["lineageStatus"])
        self.assertEqual("APPLIED", applicant_only_row["applicantStage"])
        self.assertIsNone(applicant_only_row["evaluationId"])
        self.assertIsNone(applicant_only_row["agentTaskId"])

        self.assertEqual(6, len(applied_cdc_rows))
        for row in applied_cdc_rows:
            self.assertIn(row["sourceTable"], proof["sourceTables"])
            self.assertIn(row["sourceLsn"], proof["sourceLsn"])
            self.assertEqual(row["sourceLsn"], row["eventSourceOffset"]["lsn"])
            self.assertTrue(row["eventId"].startswith("cdc_lineage_"))
            self.assertTrue(row["lineageIdempotencyKey"].startswith("sha256:"))

        for row in lineage_rows:
            self.assertIn("applicants", row["sourceLineage"])
            self.assertIn("job_postings", row["sourceLineage"])
            for metadata in row["sourceLineage"].values():
                if metadata is None:
                    continue
                self.assertTrue(metadata["eventId"].startswith("cdc_lineage_"))
                self.assertEqual(metadata["sourceLsn"], metadata["eventSourceOffset"]["lsn"])
        self.assertEqual("multi-table-cdc-lineage", proof_artifact_payload["scenario"])

    def test_control_plane_runtime_handoff_scenario_is_registered(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                "--help",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertIn("control-plane-runtime-handoff", completed.stdout)

    def test_control_plane_runtime_handoff_requires_runtime_record_artifact(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "control-plane-runtime-handoff",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=False,
            )

        self.assertNotEqual(0, completed.returncode)
        self.assertIn("quality-sla-runtime-record.json", completed.stderr)
        self.assertIn("control-plane runtime SLA", completed.stderr)

    def test_control_plane_runtime_handoff_writes_lakehouse_jsonl_with_source_metadata(self):
        runtime_record = {
            "scenario": "quality-sla-runtime-record",
            "evidenceLabel": "testcontainers-postgres-runtime-sla",
            "runtimeEvidenceLabel": "local-docker-debezium-runtime-sla",
            "checkName": "applicant-cdc-runtime-sla",
            "sourceTable": "applicants",
            "sourcePrimaryKey": "runtime-applicant-001",
            "sourceLsn": [26710752, 26711320, 26711584],
            "sourceLsnFrom": 26710752,
            "sourceLsnTo": 26711584,
            "eventSourceOffset": [
                {"lsn": 26710752, "sequence": "[null,\"26710752\"]", "txId": 761},
                {"lsn": 26711320, "sequence": "[\"26711320\",\"26711320\"]", "txId": 762},
                {"lsn": 26711584, "sequence": "[\"26711584\",\"26711584\"]", "txId": 763},
            ],
            "eventIds": ["cdc_runtime_a", "cdc_runtime_b", "cdc_runtime_c"],
            "sourceOffset": {"offsets": []},
            "slaStatus": "WARN",
            "completenessRatio": 1.0,
            "duplicateRatio": 0.0,
            "freshnessLagMillis": 1464,
            "lagMillis": 0,
            "missingEventCount": 0,
            "duplicateEventCount": 0,
            "violatedSloIds": [],
            "warningSloIds": ["source-lsn-lag-unavailable"],
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            proof_dir = Path(tmp_dir) / "proofs"
            proof_dir.mkdir()
            source_artifact = proof_dir / "quality-sla-runtime-record.json"
            source_artifact.write_text(json.dumps(runtime_record), encoding="utf-8")

            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "control-plane-runtime-handoff",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )

            proof = json.loads(completed.stdout)
            object_path = Path(proof["localObjectPath"])
            proof_artifact = Path(proof["proofArtifact"])
            written_record = json.loads(object_path.read_text().strip())
            proof_artifact_payload = json.loads(proof_artifact.read_text())

        self.assertEqual("control-plane-runtime-handoff", proof["scenario"])
        self.assertEqual("local-control-plane-runtime-lakehouse-handoff", proof["evidenceLabel"])
        self.assertEqual("quality-sla-runtime-record", proof["sourceProofScenario"])
        self.assertEqual(str(source_artifact), proof["sourceProofArtifact"])
        self.assertEqual("s3://cdc-lakehouse/control-plane/pipeline_quality_sla/event_date=2026-06-12/part-0001.jsonl", proof["s3CompatibleUri"])
        self.assertEqual(1, proof["objectRecordCount"])
        self.assertEqual([26710752, 26711320, 26711584], proof["sourceLsn"])
        self.assertEqual(["cdc_runtime_a", "cdc_runtime_b", "cdc_runtime_c"], proof["eventIds"])
        self.assertEqual(26710752, proof["eventSourceOffset"][0]["lsn"])
        self.assertEqual(["source-lsn-lag-unavailable"], proof["warningSloIds"])
        self.assertEqual(proof["eventIds"], written_record["eventIds"])
        self.assertEqual(proof["sourceLsn"], written_record["sourceLsn"])
        self.assertEqual(proof["eventSourceOffset"], written_record["eventSourceOffset"])
        self.assertEqual("WARN", written_record["slaStatus"])
        self.assertEqual("control-plane-runtime-handoff", proof_artifact_payload["scenario"])

    def test_runtime_lakehouse_replay_scenario_is_registered(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                "--help",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertIn("runtime-lakehouse-replay", completed.stdout)

    def test_runtime_lakehouse_replay_requires_handoff_artifact(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "runtime-lakehouse-replay",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=False,
            )

        self.assertNotEqual(0, completed.returncode)
        self.assertIn("control-plane-runtime-handoff.json", completed.stderr)
        self.assertIn("runtime lakehouse replay", completed.stderr)

    def test_runtime_lakehouse_replay_suppresses_duplicate_source_metadata(self):
        handoff_record = {
            "recordType": "pipeline_quality_sla_runtime",
            "sourceProofScenario": "quality-sla-runtime-record",
            "sourceProofArtifact": "/tmp/quality-sla-runtime-record.json",
            "sourceEvidenceLabel": "testcontainers-postgres-runtime-sla",
            "runtimeEvidenceLabel": "local-docker-debezium-runtime-sla",
            "checkName": "applicant-cdc-runtime-sla",
            "sourceTable": "applicants",
            "sourcePrimaryKey": "runtime-applicant-001",
            "slaStatus": "WARN",
            "completenessRatio": 1.0,
            "duplicateRatio": 0.0,
            "freshnessLagMillis": 1464,
            "lagMillis": 0,
            "missingEventCount": 0,
            "duplicateEventCount": 0,
            "violatedSloIds": [],
            "warningSloIds": ["source-lsn-lag-unavailable"],
            "sourceLsn": [26710752, 26711320, 26711584],
            "sourceLsnFrom": 26710752,
            "sourceLsnTo": 26711584,
            "eventSourceOffset": [
                {"lsn": 26710752, "sequence": "[null,\"26710752\"]", "txId": 761},
                {"lsn": 26711320, "sequence": "[\"26711320\",\"26711320\"]", "txId": 762},
                {"lsn": 26711584, "sequence": "[\"26711584\",\"26711584\"]", "txId": 763},
            ],
            "eventIds": ["cdc_runtime_a", "cdc_runtime_b", "cdc_runtime_c"],
            "sourceOffset": {"offsets": []},
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            proof_dir = output_dir / "proofs"
            proof_dir.mkdir()
            object_path = output_dir / "object-storage" / "cdc-lakehouse" / "control-plane" / "pipeline_quality_sla" / "event_date=2026-06-12" / "part-0001.jsonl"
            object_path.parent.mkdir(parents=True)
            object_path.write_text(json.dumps(handoff_record, sort_keys=True) + "\n", encoding="utf-8")

            source_artifact = proof_dir / "control-plane-runtime-handoff.json"
            source_artifact.write_text(
                json.dumps({
                    "scenario": "control-plane-runtime-handoff",
                    "evidenceLabel": "local-control-plane-runtime-lakehouse-handoff",
                    "localObjectPath": str(object_path),
                    "objectKey": "control-plane/pipeline_quality_sla/event_date=2026-06-12/part-0001.jsonl",
                    "s3CompatibleUri": "s3://cdc-lakehouse/control-plane/pipeline_quality_sla/event_date=2026-06-12/part-0001.jsonl",
                    "objectRecordCount": 1,
                    "sourceLsn": [26710752, 26711320, 26711584],
                    "eventSourceOffset": handoff_record["eventSourceOffset"],
                    "eventIds": handoff_record["eventIds"],
                    "warningSloIds": ["source-lsn-lag-unavailable"],
                }),
                encoding="utf-8",
            )

            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "runtime-lakehouse-replay",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )

            proof = json.loads(completed.stdout)
            replay_object_path = Path(proof["localObjectPath"])
            replay_rows = [json.loads(line) for line in replay_object_path.read_text().splitlines()]
            proof_artifact_payload = json.loads(Path(proof["proofArtifact"]).read_text())

        self.assertEqual("runtime-lakehouse-replay", proof["scenario"])
        self.assertEqual("local-runtime-lakehouse-replay", proof["evidenceLabel"])
        self.assertEqual("control-plane-runtime-handoff", proof["sourceProofScenario"])
        self.assertEqual(str(source_artifact), proof["sourceHandoffArtifact"])
        self.assertEqual(str(object_path), proof["sourceHandoffObjectPath"])
        self.assertEqual(2, proof["replayAttemptCount"])
        self.assertEqual(2, proof["inputRecordCount"])
        self.assertEqual(1, proof["replayedRecordCount"])
        self.assertEqual(1, proof["suppressedDuplicateCount"])
        self.assertEqual([26710752, 26711320, 26711584], proof["sourceLsn"])
        self.assertEqual(["cdc_runtime_a", "cdc_runtime_b", "cdc_runtime_c"], proof["eventIds"])
        self.assertEqual(26710752, proof["eventSourceOffset"][0]["lsn"])
        self.assertEqual("s3://cdc-lakehouse/control-plane/pipeline_quality_sla_replay/source_table=applicants/source_lsn_to=26711584/part-0001.jsonl", proof["s3CompatibleUri"])
        self.assertEqual(1, len(proof["idempotencyKeys"]))
        self.assertTrue(proof["idempotencyKeys"][0].startswith("sha256:"))
        self.assertEqual(1, len(replay_rows))
        self.assertEqual(proof["idempotencyKeys"][0], replay_rows[0]["replayIdempotencyKey"])
        self.assertEqual("REPLAYED", replay_rows[0]["replayDecision"])
        self.assertEqual(proof["eventIds"], replay_rows[0]["eventIds"])
        self.assertEqual(proof["sourceLsn"], replay_rows[0]["sourceLsn"])
        self.assertEqual(proof["eventSourceOffset"], replay_rows[0]["eventSourceOffset"])
        self.assertEqual("runtime-lakehouse-replay", proof_artifact_payload["scenario"])

    def test_canonical_object_sink_preserves_source_metadata(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "canonical-object-sink",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )

            proof = json.loads(completed.stdout)

            self.assertEqual("canonical-object-sink", proof["scenario"])
            self.assertEqual("local-s3-compatible-backend", proof["evidenceLabel"])
            self.assertEqual("s3://cdc-lakehouse/curated/hiring_events/event_date=2026-06-09/part-0001.jsonl", proof["s3CompatibleUri"])
            self.assertEqual("cdc-lakehouse", proof["bucket"])
            self.assertEqual(1, proof["objectEventCount"])
            self.assertTrue(proof["eventId"].startswith("cdc_"))
            self.assertEqual(88432240, proof["sourceLsn"])
            self.assertEqual(88432240, proof["eventSourceOffset"]["lsn"])
            self.assertEqual(773, proof["eventSourceOffset"]["txId"])
            self.assertEqual("26740416", proof["eventSourceOffset"]["sequence"][0])

            object_path = Path(proof["localObjectPath"])
            self.assertTrue(object_path.exists())
            written_event = json.loads(object_path.read_text().strip())
            self.assertEqual(proof["eventId"], written_event["eventId"])
            self.assertEqual(88432240, written_event["sourceLsn"])
            self.assertEqual("SCREENING", written_event["after"]["stage"])

    def test_s3_compatible_put_fixture_signs_request_and_preserves_source_metadata(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "s3-compatible-put-fixture",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )

            proof = json.loads(completed.stdout)

            self.assertEqual("s3-compatible-put-fixture", proof["scenario"])
            self.assertEqual("s3-compatible-signed-put-fixture", proof["evidenceLabel"])
            self.assertEqual("PUT", proof["httpMethod"])
            self.assertEqual("cdc-lakehouse", proof["bucket"])
            self.assertEqual("curated/hiring_events/event_date=2026-06-09/part-0001.jsonl", proof["objectKey"])
            self.assertEqual("s3://cdc-lakehouse/curated/hiring_events/event_date=2026-06-09/part-0001.jsonl", proof["s3CompatibleUri"])
            self.assertTrue(proof["authorizationHeader"].startswith("AWS4-HMAC-SHA256 "))
            self.assertEqual(1, proof["putRequestCount"])
            self.assertEqual(1, proof["objectEventCount"])
            self.assertTrue(proof["eventId"].startswith("cdc_"))
            self.assertEqual(88432240, proof["sourceLsn"])
            self.assertEqual(88432240, proof["eventSourceOffset"]["lsn"])
            self.assertEqual("26740416", proof["eventSourceOffset"]["sequence"][0])

    def test_minio_object_sink_scenario_is_registered(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                "--help",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertIn("minio-object-sink", completed.stdout)

    def test_minio_compose_object_sink_scenario_is_registered(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                "--help",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertIn("minio-compose-object-sink", completed.stdout)

    def test_lakehouse_runtime_preflight_scenario_is_registered(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                "--help",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertIn("lakehouse-runtime-preflight", completed.stdout)

    def test_lakehouse_runtime_preflight_reports_missing_opt_in_components(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            env = os.environ.copy()
            env["PATH"] = tmp_dir
            for key in [
                "CDC_LAKEHOUSE_S3_ENDPOINT",
                "CDC_LAKEHOUSE_S3_ACCESS_KEY",
                "CDC_LAKEHOUSE_S3_SECRET_KEY",
                "CDC_LAKEHOUSE_ICEBERG_ENGINE_CMD",
                "CDC_LAKEHOUSE_ATHENA_OPT_IN",
                "CDC_LAKEHOUSE_DBT_ATHENA_OPT_IN",
            ]:
                env.pop(key, None)

            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "lakehouse-runtime-preflight",
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                env=env,
                check=True,
            )

        proof = json.loads(completed.stdout)

        self.assertEqual("lakehouse-runtime-preflight", proof["scenario"])
        self.assertEqual("lakehouse-runtime-preflight", proof["evidenceLabel"])
        self.assertFalse(proof["docker"]["cliAvailable"])
        self.assertFalse(proof["readiness"]["minioObjectSinkReady"])
        self.assertFalse(proof["readiness"]["icebergEngineReady"])
        self.assertFalse(proof["readiness"]["athenaDbtReady"])
        self.assertIn("s3-endpoint-not-configured", proof["blockers"])
        self.assertIn("iceberg-engine-command-not-configured", proof["blockers"])
        self.assertIn("aws-athena-dbt-opt-in-not-configured", proof["blockers"])

    def test_lakehouse_runtime_preflight_detects_local_images_and_engine_command(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            bin_dir = Path(tmp_dir) / "bin"
            bin_dir.mkdir()
            docker = bin_dir / "docker"
            docker.write_text(
                "#!/bin/sh\n"
                "if [ \"$1\" = \"version\" ]; then echo '24.0.0'; exit 0; fi\n"
                "if [ \"$1\" = \"images\" ]; then\n"
                "  echo 'minio/minio:latest'\n"
                "  echo 'minio/mc:latest'\n"
                "  echo 'trinodb/trino:latest'\n"
                "  echo 'apache/spark:3.5.1'\n"
                "  exit 0\n"
                "fi\n"
                "exit 1\n",
                encoding="utf-8",
            )
            spark_sql = bin_dir / "spark-sql"
            spark_sql.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            for path in [docker, spark_sql]:
                path.chmod(path.stat().st_mode | stat.S_IXUSR)

            env = os.environ.copy()
            env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
            env.update({
                "CDC_LAKEHOUSE_S3_ENDPOINT": "http://127.0.0.1:9000",
                "CDC_LAKEHOUSE_S3_ACCESS_KEY": "minioadmin",
                "CDC_LAKEHOUSE_S3_SECRET_KEY": "minioadmin",
                "CDC_LAKEHOUSE_ICEBERG_ENGINE_CMD": "spark-sql --conf spark.sql.catalog.cdc=org.apache.iceberg.spark.SparkCatalog",
            })

            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "lakehouse-runtime-preflight",
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                env=env,
                check=True,
            )

        proof = json.loads(completed.stdout)

        self.assertTrue(proof["docker"]["cliAvailable"])
        self.assertTrue(proof["docker"]["daemonResponsive"])
        self.assertEqual("24.0.0", proof["docker"]["serverVersion"])
        self.assertTrue(proof["localImages"]["minioServer"])
        self.assertTrue(proof["localImages"]["minioClient"])
        self.assertTrue(proof["localImages"]["trino"])
        self.assertTrue(proof["localImages"]["spark"])
        self.assertTrue(proof["configuration"]["s3EndpointConfigured"])
        self.assertTrue(proof["configuration"]["s3CredentialsConfigured"])
        self.assertTrue(proof["configuration"]["icebergEngineCommandConfigured"])
        self.assertTrue(proof["readiness"]["minioObjectSinkReady"])
        self.assertTrue(proof["readiness"]["icebergEngineReady"])

    def test_lakehouse_runtime_preflight_does_not_treat_kafka_image_as_spark(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            bin_dir = Path(tmp_dir) / "bin"
            bin_dir.mkdir()
            docker = bin_dir / "docker"
            docker.write_text(
                "#!/bin/sh\n"
                "if [ \"$1\" = \"version\" ]; then echo '24.0.0'; exit 0; fi\n"
                "if [ \"$1\" = \"images\" ]; then\n"
                "  echo 'apache/kafka:4.2.0'\n"
                "  echo 'postgres:16-alpine'\n"
                "  exit 0\n"
                "fi\n"
                "exit 1\n",
                encoding="utf-8",
            )
            docker.chmod(docker.stat().st_mode | stat.S_IXUSR)

            env = os.environ.copy()
            env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"

            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "lakehouse-runtime-preflight",
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                env=env,
                check=True,
            )

        proof = json.loads(completed.stdout)

        self.assertFalse(proof["localImages"]["spark"])
        self.assertFalse(proof["localImages"]["trino"])
        self.assertFalse(proof["localImages"]["minioServer"])

    def test_minio_object_sink_requires_endpoint_configuration(self):
        env = os.environ.copy()
        for key in [
            "CDC_LAKEHOUSE_S3_ENDPOINT",
            "CDC_LAKEHOUSE_S3_ACCESS_KEY",
            "CDC_LAKEHOUSE_S3_SECRET_KEY",
        ]:
            env.pop(key, None)

        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                "minio-object-sink",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        self.assertNotEqual(0, completed.returncode)
        self.assertIn("CDC_LAKEHOUSE_S3_ENDPOINT", completed.stderr)

    def test_minio_object_sink_puts_bucket_and_object_to_configured_endpoint(self):
        requests = []

        class FakeS3Handler(http.server.BaseHTTPRequestHandler):
            def do_PUT(self):
                length = int(self.headers.get("content-length", "0"))
                body = self.rfile.read(length)
                requests.append({
                    "path": unquote(self.path),
                    "headers": {key.lower(): value for key, value in self.headers.items()},
                    "body": body,
                })
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.send_header("Connection", "close")
                self.end_headers()
                self.close_connection = True

            def log_message(self, format, *args):
                return

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), FakeS3Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                env = os.environ.copy()
                env.update({
                    "CDC_LAKEHOUSE_S3_ENDPOINT": f"http://127.0.0.1:{server.server_port}",
                    "CDC_LAKEHOUSE_S3_ACCESS_KEY": "minioadmin",
                    "CDC_LAKEHOUSE_S3_SECRET_KEY": "minioadmin",
                    "CDC_LAKEHOUSE_S3_BUCKET": "cdc-lakehouse",
                    "CDC_LAKEHOUSE_S3_REGION": "us-east-1",
                })
                completed = subprocess.run(
                    [
                        str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                        "minio-object-sink",
                        "--output-dir",
                        tmp_dir,
                    ],
                    cwd=ROOT,
                    text=True,
                    capture_output=True,
                    env=env,
                    check=True,
                )
                proof = json.loads(completed.stdout)
                proof_artifact = Path(proof["proofArtifact"])
                self.assertTrue(proof_artifact.exists())
                proof_artifact_payload = json.loads(proof_artifact.read_text())
        finally:
            server.server_close()

        self.assertEqual("minio-object-sink", proof["scenario"])
        self.assertEqual("s3-compatible-live-endpoint", proof["evidenceLabel"])
        self.assertEqual("http://127.0.0.1", proof["endpoint"].rsplit(":", 1)[0])
        self.assertEqual("PUT", proof["httpMethod"])
        self.assertEqual(200, proof["httpStatus"])
        self.assertEqual("cdc-lakehouse", proof["bucket"])
        self.assertEqual("curated/hiring_events/event_date=2026-06-09/part-0001.jsonl", proof["objectKey"])
        self.assertEqual("s3://cdc-lakehouse/curated/hiring_events/event_date=2026-06-09/part-0001.jsonl", proof["s3CompatibleUri"])
        self.assertEqual(2, proof["putRequestCount"])
        self.assertEqual(1, proof["objectEventCount"])
        self.assertTrue(proof["authorizationHeader"].startswith("AWS4-HMAC-SHA256 "))
        self.assertTrue(proof["eventId"].startswith("cdc_"))
        self.assertEqual(88432240, proof["sourceLsn"])
        self.assertEqual(88432240, proof["eventSourceOffset"]["lsn"])
        self.assertEqual("minio-object-sink", proof_artifact_payload["scenario"])
        self.assertEqual(["/cdc-lakehouse", "/cdc-lakehouse/curated/hiring_events/event_date=2026-06-09/part-0001.jsonl"], [request["path"] for request in requests])
        for request in requests:
            signed_at = datetime.strptime(request["headers"]["x-amz-date"], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
            self.assertLess(abs(datetime.now(UTC) - signed_at), timedelta(minutes=5))

    def test_minio_object_sink_treats_existing_bucket_conflict_as_reusable(self):
        requests = []

        class FakeS3Handler(http.server.BaseHTTPRequestHandler):
            def do_PUT(self):
                length = int(self.headers.get("content-length", "0"))
                body = self.rfile.read(length)
                requests.append({
                    "path": unquote(self.path),
                    "headers": {key.lower(): value for key, value in self.headers.items()},
                    "body": body,
                })
                if self.path == "/cdc-lakehouse":
                    response = b"<Error><Code>BucketAlreadyOwnedByYou</Code></Error>"
                    self.send_response(409)
                    self.send_header("Content-Length", str(len(response)))
                    self.end_headers()
                    self.wfile.write(response)
                    return
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.send_header("Connection", "close")
                self.end_headers()
                self.close_connection = True

            def log_message(self, format, *args):
                return

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), FakeS3Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                env = os.environ.copy()
                env.update({
                    "CDC_LAKEHOUSE_S3_ENDPOINT": f"http://127.0.0.1:{server.server_port}",
                    "CDC_LAKEHOUSE_S3_ACCESS_KEY": "minioadmin",
                    "CDC_LAKEHOUSE_S3_SECRET_KEY": "minioadmin",
                    "CDC_LAKEHOUSE_S3_BUCKET": "cdc-lakehouse",
                    "CDC_LAKEHOUSE_S3_REGION": "us-east-1",
                })
                completed = subprocess.run(
                    [
                        str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                        "minio-object-sink",
                        "--output-dir",
                        tmp_dir,
                    ],
                    cwd=ROOT,
                    text=True,
                    capture_output=True,
                    env=env,
                    check=True,
                )
        finally:
            server.server_close()

        proof = json.loads(completed.stdout)

        self.assertEqual("minio-object-sink", proof["scenario"])
        self.assertEqual(200, proof["httpStatus"])
        self.assertEqual(2, proof["putRequestCount"])
        self.assertEqual(["/cdc-lakehouse", "/cdc-lakehouse/curated/hiring_events/event_date=2026-06-09/part-0001.jsonl"], [request["path"] for request in requests])

    def test_bucket_put_signature_uses_bucket_request_path_without_trailing_slash(self):
        runner = runpy.run_path(str(ROOT / "tools" / "runner" / "lakehouse-smoke"), run_name="lakehouse_smoke_helpers")

        headers = runner["signed_s3_put_headers"](
            "http://127.0.0.1:9000",
            "cdc-lakehouse",
            "",
            b"",
            "minioadmin",
            "minioadmin",
            "us-east-1",
            datetime(2026, 6, 9, 4, 0, 0, tzinfo=UTC),
        )

        self.assertIn(
            "Signature=c0a3878b2ccce8916ad0fc2de385a2d406b1c44b0ddaec294a1b9c6183d72ea9",
            headers["Authorization"],
        )

    def test_minio_compose_object_sink_requires_local_image_without_pull(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            bin_dir = Path(tmp_dir) / "bin"
            bin_dir.mkdir()
            docker = bin_dir / "docker"
            docker.write_text(
                "#!/bin/sh\n"
                "if [ \"$1\" = \"image\" ] && [ \"$2\" = \"inspect\" ]; then exit 1; fi\n"
                "echo unexpected docker command \"$@\" >&2\n"
                "exit 1\n",
                encoding="utf-8",
            )
            docker.chmod(docker.stat().st_mode | stat.S_IXUSR)
            env = os.environ.copy()
            env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"

            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "minio-compose-object-sink",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                env=env,
                check=False,
            )

        self.assertNotEqual(0, completed.returncode)
        self.assertIn("local MinIO image", completed.stderr)
        self.assertIn("opt-in", completed.stderr)

    def test_minio_compose_object_sink_starts_minio_with_pull_never_and_puts_object(self):
        requests = []

        class FakeS3Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/minio/health/ready":
                    self.send_response(200)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                self.send_response(404)
                self.end_headers()

            def do_PUT(self):
                length = int(self.headers.get("content-length", "0"))
                body = self.rfile.read(length)
                requests.append({
                    "path": unquote(self.path),
                    "headers": {key.lower(): value for key, value in self.headers.items()},
                    "body": body,
                })
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.send_header("Connection", "close")
                self.end_headers()
                self.close_connection = True

            def log_message(self, format, *args):
                return

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), FakeS3Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                bin_dir = Path(tmp_dir) / "bin"
                bin_dir.mkdir()
                docker_log = Path(tmp_dir) / "docker.log"
                docker = bin_dir / "docker"
                docker.write_text(
                    "#!/usr/bin/env python3\n"
                    "import pathlib, sys\n"
                    f"log = pathlib.Path({str(docker_log)!r})\n"
                    "log.write_text(log.read_text() + ' '.join(sys.argv[1:]) + '\\n' if log.exists() else ' '.join(sys.argv[1:]) + '\\n')\n"
                    "args = sys.argv[1:]\n"
                    "if args[:2] == ['image', 'inspect']:\n"
                    "    raise SystemExit(0)\n"
                    "if args and args[0] == 'compose' and 'ps' in args:\n"
                    "    print('')\n"
                    "    raise SystemExit(0)\n"
                    "if args and args[0] == 'compose' and 'up' in args:\n"
                    "    raise SystemExit(0)\n"
                    "if args and args[0] == 'compose' and 'rm' in args:\n"
                    "    raise SystemExit(0)\n"
                    "raise SystemExit(1)\n",
                    encoding="utf-8",
                )
                docker.chmod(docker.stat().st_mode | stat.S_IXUSR)
                env = os.environ.copy()
                env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
                env.update({
                    "CDC_LAKEHOUSE_S3_ENDPOINT": f"http://127.0.0.1:{server.server_port}",
                    "CDC_LAKEHOUSE_S3_ACCESS_KEY": "minioadmin",
                    "CDC_LAKEHOUSE_S3_SECRET_KEY": "minioadmin",
                    "CDC_LAKEHOUSE_MINIO_START_TIMEOUT_SECONDS": "2",
                })

                completed = subprocess.run(
                    [
                        str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                        "minio-compose-object-sink",
                        "--output-dir",
                        tmp_dir,
                    ],
                    cwd=ROOT,
                    text=True,
                    capture_output=True,
                    env=env,
                    check=True,
                )
                docker_commands = docker_log.read_text()
        finally:
            server.server_close()

        proof = json.loads(completed.stdout)

        self.assertEqual("minio-compose-object-sink", proof["scenario"])
        self.assertEqual("local-minio-live-endpoint", proof["evidenceLabel"])
        self.assertEqual(200, proof["httpStatus"])
        self.assertEqual(2, proof["putRequestCount"])
        self.assertEqual(88432240, proof["sourceLsn"])
        self.assertEqual(88432240, proof["eventSourceOffset"]["lsn"])
        self.assertTrue(proof["compose"]["startedByRunner"])
        self.assertTrue(proof["compose"]["cleanupPerformed"])
        self.assertEqual("never", proof["compose"]["pullPolicy"])
        self.assertIn("image inspect minio/minio:RELEASE.2025-04-22T22-12-26Z", docker_commands)
        self.assertIn("compose --project-name cdc-data-platform", docker_commands)
        self.assertIn("up -d --no-deps --pull never minio", docker_commands)
        self.assertIn("rm -sf minio", docker_commands)
        self.assertEqual(["/cdc-lakehouse", "/cdc-lakehouse/curated/hiring_events/event_date=2026-06-09/part-0001.jsonl"], [request["path"] for request in requests])

    def test_iceberg_engine_bootstrap_scenario_is_registered(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                "--help",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertIn("iceberg-engine-bootstrap", completed.stdout)

    def test_iceberg_engine_append_scenario_is_registered(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                "--help",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertIn("iceberg-engine-append", completed.stdout)

    def test_trino_iceberg_catalog_config_uses_jdbc_catalog_and_minio_s3(self):
        catalog = (ROOT / "infra" / "local" / "trino" / "catalog" / "iceberg.properties").read_text()

        self.assertIn("connector.name=iceberg", catalog)
        self.assertIn("iceberg.catalog.type=jdbc", catalog)
        self.assertIn("iceberg.jdbc-catalog.catalog-name=cdc", catalog)
        self.assertIn("iceberg.jdbc-catalog.driver-class=org.postgresql.Driver", catalog)
        self.assertIn("jdbc:postgresql://iceberg-catalog-postgres:5432/iceberg_catalog", catalog)
        self.assertIn("iceberg.jdbc-catalog.default-warehouse-dir=s3://cdc-lakehouse/warehouse", catalog)
        self.assertIn("fs.s3.enabled=true", catalog)
        self.assertIn("s3.endpoint=http://minio:9000", catalog)
        self.assertIn("s3.path-style-access=true", catalog)
        self.assertIn("s3.aws-access-key=minioadmin", catalog)
        self.assertIn("s3.aws-secret-key=minioadmin", catalog)

    def test_compose_wires_trino_to_iceberg_catalog_postgres_and_catalog_config(self):
        compose = (ROOT / "infra" / "local" / "docker-compose.yml").read_text()

        self.assertIn("iceberg-catalog-postgres:", compose)
        self.assertIn("container_name: cdc-iceberg-catalog-postgres", compose)
        self.assertIn("POSTGRES_DB: iceberg_catalog", compose)
        self.assertIn("iceberg-catalog-postgres-data:/var/lib/postgresql/data", compose)
        self.assertIn("./trino/catalog:/etc/trino/catalog:ro", compose)
        self.assertIn("./trino/etc/config.properties:/etc/trino/config.properties:ro", compose)
        self.assertIn("./trino/etc/jvm.config:/etc/trino/jvm.config:ro", compose)
        self.assertIn("plugin_name=\"$${plugin##*/}\"", compose)
        self.assertIn("[ \"$$plugin_name\" != \"iceberg\" ]", compose)
        self.assertIn("exec /usr/lib/trino/bin/run-trino", compose)
        self.assertIn("- iceberg-catalog-postgres", compose)
        self.assertIn("iceberg-catalog-postgres-data:", compose)

    def test_trino_local_profile_caps_heap_for_small_colima_runtime(self):
        jvm_config = (ROOT / "infra" / "local" / "trino" / "etc" / "jvm.config").read_text()
        config = (ROOT / "infra" / "local" / "trino" / "etc" / "config.properties").read_text()

        self.assertIn("-Xms128M", jvm_config)
        self.assertIn("-Xmx320M", jvm_config)
        self.assertNotIn("InitialRAMPercentage", jvm_config)
        self.assertNotIn("MaxRAMPercentage", jvm_config)
        self.assertIn("query.max-memory=192MB", config)
        self.assertIn("query.max-memory-per-node=128MB", config)
        self.assertIn("memory.heap-headroom-per-node=96MB", config)

    def test_trino_compose_iceberg_scenarios_are_registered(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                "--help",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertIn("trino-compose-iceberg-bootstrap", completed.stdout)
        self.assertIn("trino-compose-iceberg-append", completed.stdout)

    def test_trino_compose_iceberg_bootstrap_requires_local_images_without_pull(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            bin_dir = Path(tmp_dir) / "bin"
            bin_dir.mkdir()
            docker = bin_dir / "docker"
            docker.write_text(
                "#!/bin/sh\n"
                "if [ \"$1\" = \"image\" ] && [ \"$2\" = \"inspect\" ]; then exit 1; fi\n"
                "echo unexpected docker command \"$@\" >&2\n"
                "exit 1\n",
                encoding="utf-8",
            )
            docker.chmod(docker.stat().st_mode | stat.S_IXUSR)
            env = os.environ.copy()
            env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"

            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "trino-compose-iceberg-bootstrap",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                env=env,
                check=False,
            )

        self.assertNotEqual(0, completed.returncode)
        self.assertIn("local Docker image", completed.stderr)
        self.assertIn("opt-in", completed.stderr)

    def test_trino_compose_iceberg_bootstrap_starts_services_with_pull_never_and_runs_sql(self):
        statements = []

        class FakeTrinoHandler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("content-length", "0"))
                statements.append(self.rfile.read(length).decode("utf-8"))
                if len(statements) == 1:
                    payload = {
                        "id": "query_starting",
                        "stats": {"state": "FAILED"},
                        "error": {
                            "message": "Trino server is still initializing",
                            "errorCode": 65548,
                            "errorName": "SERVER_STARTING_UP",
                        },
                    }
                else:
                    payload = {"id": "query_1", "stats": {"state": "FINISHED"}}
                response = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response)

            def log_message(self, format, *args):
                return

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), FakeTrinoHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                endpoint = f"http://127.0.0.1:{server.server_port}/v1/statement"
                bin_dir = Path(tmp_dir) / "bin"
                bin_dir.mkdir()
                docker_log = Path(tmp_dir) / "docker.log"
                docker = bin_dir / "docker"
                docker.write_text(
                    "#!/usr/bin/env python3\n"
                    "import pathlib, sys\n"
                    f"log = pathlib.Path({str(docker_log)!r})\n"
                    "line = ' '.join(sys.argv[1:]) + '\\n'\n"
                    "log.write_text(log.read_text() + line if log.exists() else line)\n"
                    "args = sys.argv[1:]\n"
                    "if args[:2] == ['image', 'inspect']:\n"
                    "    raise SystemExit(0)\n"
                    "if args and args[0] == 'compose' and 'ps' in args:\n"
                    "    print('')\n"
                    "    raise SystemExit(0)\n"
                    "if args and args[0] == 'compose' and 'up' in args:\n"
                    "    raise SystemExit(0)\n"
                    "if args and args[0] == 'compose' and 'exec' in args:\n"
                    "    raise SystemExit(2)\n"
                    "if args and args[0] == 'compose' and 'rm' in args:\n"
                    "    raise SystemExit(0)\n"
                    "raise SystemExit(1)\n",
                    encoding="utf-8",
                )
                docker.chmod(docker.stat().st_mode | stat.S_IXUSR)
                env = os.environ.copy()
                env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
                env["CDC_LAKEHOUSE_TRINO_START_TIMEOUT_SECONDS"] = "1"
                env["CDC_LAKEHOUSE_TRINO_HEALTH_CHECK"] = "false"
                env["CDC_LAKEHOUSE_TRINO_QUERY_ENDPOINT"] = endpoint
                env["CDC_LAKEHOUSE_TRINO_QUERY_RETRY_SECONDS"] = "0"

                completed = subprocess.run(
                    [
                        str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                        "trino-compose-iceberg-bootstrap",
                        "--output-dir",
                        tmp_dir,
                    ],
                    cwd=ROOT,
                    text=True,
                    capture_output=True,
                    env=env,
                    check=True,
                )
                proof = json.loads(completed.stdout)
                sql = Path(proof["sqlFile"]).read_text()
                docker_commands = docker_log.read_text()
        finally:
            server.server_close()

        self.assertEqual("trino-compose-iceberg-bootstrap", proof["scenario"])
        self.assertEqual("trino-iceberg-compose-opt-in", proof["evidenceLabel"])
        self.assertEqual(0, proof["engineExitCode"])
        self.assertEqual("iceberg.cdc.hiring_events", proof["tableIdentifier"])
        self.assertIn("CREATE SCHEMA IF NOT EXISTS iceberg.cdc", sql)
        self.assertIn("WITH (location = 's3://cdc-lakehouse/warehouse/cdc')", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS iceberg.cdc.hiring_events", sql)
        self.assertIn("partitioning = ARRAY['event_date']", sql)
        self.assertNotIn("USING iceberg", sql)
        self.assertEqual([sql, sql], statements)
        self.assertEqual("POST", proof["engineCommand"][0])
        self.assertIn("image inspect minio/minio:RELEASE.2025-04-22T22-12-26Z", docker_commands)
        self.assertIn("image inspect trinodb/trino:455", docker_commands)
        self.assertIn("image inspect postgres:16", docker_commands)
        self.assertIn("up -d --no-deps --pull never iceberg-catalog-postgres minio trino", docker_commands)
        self.assertNotIn("exec -T trino trino --execute", docker_commands)
        self.assertIn("rm -sf trino minio iceberg-catalog-postgres", docker_commands)

    def test_trino_compose_iceberg_bootstrap_cleans_started_services_when_health_fails(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            bin_dir = Path(tmp_dir) / "bin"
            bin_dir.mkdir()
            docker_log = Path(tmp_dir) / "docker.log"
            docker = bin_dir / "docker"
            docker.write_text(
                "#!/usr/bin/env python3\n"
                "import pathlib, sys\n"
                f"log = pathlib.Path({str(docker_log)!r})\n"
                "line = ' '.join(sys.argv[1:]) + '\\n'\n"
                "log.write_text(log.read_text() + line if log.exists() else line)\n"
                "args = sys.argv[1:]\n"
                "if args[:2] == ['image', 'inspect']:\n"
                "    raise SystemExit(0)\n"
                "if args and args[0] == 'compose' and 'ps' in args:\n"
                "    print('')\n"
                "    raise SystemExit(0)\n"
                "if args and args[0] == 'compose' and 'up' in args:\n"
                "    raise SystemExit(0)\n"
                "if args and args[0] == 'compose' and 'rm' in args:\n"
                "    raise SystemExit(0)\n"
                "raise SystemExit(1)\n",
                encoding="utf-8",
            )
            docker.chmod(docker.stat().st_mode | stat.S_IXUSR)
            env = os.environ.copy()
            env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
            env["CDC_LAKEHOUSE_TRINO_START_TIMEOUT_SECONDS"] = "0.1"
            env["CDC_LAKEHOUSE_TRINO_HEALTH_URL"] = "http://127.0.0.1:1/v1/info"

            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "trino-compose-iceberg-bootstrap",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                env=env,
                check=False,
            )
            docker_commands = docker_log.read_text()

        self.assertNotEqual(0, completed.returncode)
        self.assertIn("Trino health endpoint did not become ready", completed.stderr)
        self.assertIn("up -d --no-deps --pull never iceberg-catalog-postgres minio trino", docker_commands)
        self.assertIn("rm -sf trino minio iceberg-catalog-postgres", docker_commands)

    def test_trino_compose_iceberg_append_runs_insert_sql_and_records_count(self):
        statements = []

        class FakeTrinoHandler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("content-length", "0"))
                statements.append(self.rfile.read(length).decode("utf-8"))
                response = json.dumps({"id": "query_1", "stats": {"state": "FINISHED"}}).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response)

            def log_message(self, format, *args):
                return

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), FakeTrinoHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                endpoint = f"http://127.0.0.1:{server.server_port}/v1/statement"
                bin_dir = Path(tmp_dir) / "bin"
                bin_dir.mkdir()
                docker_log = Path(tmp_dir) / "docker.log"
                docker = bin_dir / "docker"
                docker.write_text(
                    "#!/usr/bin/env python3\n"
                    "import pathlib, sys\n"
                    f"log = pathlib.Path({str(docker_log)!r})\n"
                    "line = ' '.join(sys.argv[1:]) + '\\n'\n"
                    "log.write_text(log.read_text() + line if log.exists() else line)\n"
                    "args = sys.argv[1:]\n"
                    "if args[:2] == ['image', 'inspect']:\n"
                    "    raise SystemExit(0)\n"
                    "if args and args[0] == 'compose' and 'ps' in args:\n"
                    "    print('')\n"
                    "    raise SystemExit(0)\n"
                    "if args and args[0] == 'compose' and 'up' in args:\n"
                    "    raise SystemExit(0)\n"
                    "if args and args[0] == 'compose' and 'exec' in args:\n"
                    "    raise SystemExit(2)\n"
                    "if args and args[0] == 'compose' and 'rm' in args:\n"
                    "    raise SystemExit(0)\n"
                    "raise SystemExit(1)\n",
                    encoding="utf-8",
                )
                docker.chmod(docker.stat().st_mode | stat.S_IXUSR)
                env = os.environ.copy()
                env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
                env["CDC_LAKEHOUSE_TRINO_START_TIMEOUT_SECONDS"] = "1"
                env["CDC_LAKEHOUSE_TRINO_HEALTH_CHECK"] = "false"
                env["CDC_LAKEHOUSE_TRINO_QUERY_ENDPOINT"] = endpoint

                completed = subprocess.run(
                    [
                        str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                        "trino-compose-iceberg-append",
                        "--output-dir",
                        tmp_dir,
                    ],
                    cwd=ROOT,
                    text=True,
                    capture_output=True,
                    env=env,
                    check=True,
                )
                proof = json.loads(completed.stdout)
                sql = Path(proof["sqlFile"]).read_text()
                data_file_exists = Path(proof["dataFile"]).exists()
                docker_commands = docker_log.read_text()
        finally:
            server.server_close()

        self.assertEqual("trino-compose-iceberg-append", proof["scenario"])
        self.assertEqual(0, proof["engineExitCode"])
        self.assertEqual(3, proof["appendedRecordCount"])
        self.assertTrue(data_file_exists)
        self.assertIn("INSERT INTO iceberg.cdc.hiring_events", sql)
        self.assertIn("cdc_local_applicant_created", sql)
        self.assertIn("cdc_local_agent_task_completed", sql)
        self.assertEqual([sql], statements)
        self.assertEqual("POST", proof["engineCommand"][0])
        self.assertNotIn("exec -T trino trino --execute", docker_commands)

    def test_athena_and_dbt_athena_scenarios_are_registered(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                "--help",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertIn("athena-mart-query-plan", completed.stdout)
        self.assertIn("athena-mart-execution", completed.stdout)
        self.assertIn("dbt-athena-run", completed.stdout)

    def test_dbt_athena_project_assets_exist_and_reference_curated_source(self):
        project = ROOT / "lakehouse" / "dbt"

        self.assertTrue((project / "dbt_project.yml").exists())
        self.assertTrue((project / "profiles.yml.template").exists())
        self.assertTrue((project / "models" / "sources.yml").exists())
        self.assertTrue((project / "models" / "curated" / "curated_hiring_pipeline_lineage.sql").exists())
        self.assertTrue((project / "models" / "marts" / "mart_hiring_funnel_daily.sql").exists())
        self.assertTrue((project / "models" / "marts" / "mart_agent_task_reliability.sql").exists())
        self.assertTrue((project / "models" / "marts" / "mart_hiring_pipeline_lineage.sql").exists())

        project_yml = (project / "dbt_project.yml").read_text()
        profile_template = (project / "profiles.yml.template").read_text()
        sources_yml = (project / "models" / "sources.yml").read_text()

        self.assertIn("profile: cdc_data_platform_athena", project_yml)
        self.assertIn("type: athena", profile_template)
        self.assertIn("s3_staging_dir:", profile_template)
        self.assertIn("region_name:", profile_template)
        self.assertIn("curated_hiring_events", sources_yml)
        self.assertIn("hiring_pipeline_lineage", sources_yml)

    def test_athena_mart_query_plan_writes_rendered_sql_without_dbt_refs(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "athena-mart-query-plan",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )

            proof = json.loads(completed.stdout)
            self.assertTrue(Path(proof["curatedTableDdlFile"]).exists())
            self.assertTrue(Path(proof["lineageTableDdlFile"]).exists())
            rendered_sql = [Path(path).read_text() for path in proof["athenaQueryFiles"]]
            ddl_sql = Path(proof["curatedTableDdlFile"]).read_text()
            lineage_ddl_sql = Path(proof["lineageTableDdlFile"]).read_text()

        self.assertEqual("athena-mart-query-plan", proof["scenario"])
        self.assertEqual("athena-dbt-opt-in-plan", proof["evidenceLabel"])
        self.assertEqual(str(ROOT / "lakehouse" / "dbt"), proof["dbtProjectDir"])
        self.assertTrue(proof["profilesTemplate"].endswith("profiles.yml.template"))
        self.assertEqual(3, len(proof["martSqlFiles"]))
        self.assertEqual(3, len(proof["athenaQueryFiles"]))

        for sql in rendered_sql:
            self.assertNotIn("{{ ref(", sql)
        self.assertTrue(any("curated_hiring_events" in sql for sql in rendered_sql))
        self.assertTrue(any("curated_hiring_pipeline_lineage" in sql for sql in rendered_sql))
        self.assertIn("CREATE EXTERNAL TABLE IF NOT EXISTS curated_hiring_events", ddl_sql)
        self.assertIn("LOCATION '<set-s3-curated-hiring-events-location>'", ddl_sql)
        self.assertIn("CREATE EXTERNAL TABLE IF NOT EXISTS hiring_pipeline_lineage", lineage_ddl_sql)
        self.assertIn("LOCATION '<set-s3-hiring-pipeline-lineage-location>'", lineage_ddl_sql)

    def test_athena_mart_execution_requires_explicit_opt_in(self):
        env = os.environ.copy()
        for key in [
            "CDC_LAKEHOUSE_ATHENA_OPT_IN",
            "CDC_LAKEHOUSE_ATHENA_DATABASE",
            "CDC_LAKEHOUSE_ATHENA_OUTPUT_LOCATION",
        ]:
            env.pop(key, None)

        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                "athena-mart-execution",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        self.assertNotEqual(0, completed.returncode)
        self.assertIn("CDC_LAKEHOUSE_ATHENA_OPT_IN=true", completed.stderr)

    def test_athena_mart_execution_uses_aws_cli_query_context_and_result_location(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            bin_dir = Path(tmp_dir) / "bin"
            bin_dir.mkdir()
            aws_log = Path(tmp_dir) / "aws.log"
            aws = bin_dir / "aws"
            aws.write_text(
                "#!/usr/bin/env python3\n"
                "import json, pathlib, sys\n"
                f"log = pathlib.Path({str(aws_log)!r})\n"
                "line = ' '.join(sys.argv[1:]) + '\\n'\n"
                "log.write_text(log.read_text() + line if log.exists() else line)\n"
                "args = sys.argv[1:]\n"
                "if args[:2] == ['athena', 'start-query-execution']:\n"
                "    print(json.dumps({'QueryExecutionId': 'query-' + str(len(log.read_text().splitlines()))}))\n"
                "    raise SystemExit(0)\n"
                "if args[:2] == ['athena', 'get-query-execution']:\n"
                "    print(json.dumps({'QueryExecution': {'Status': {'State': 'SUCCEEDED'}, 'ResultConfiguration': {'OutputLocation': 's3://cdc-athena-results/query.csv'}}}))\n"
                "    raise SystemExit(0)\n"
                "raise SystemExit(1)\n",
                encoding="utf-8",
            )
            aws.chmod(aws.stat().st_mode | stat.S_IXUSR)
            env = os.environ.copy()
            env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
            env.update({
                "CDC_LAKEHOUSE_ATHENA_OPT_IN": "true",
                "CDC_LAKEHOUSE_ATHENA_DATABASE": "cdc_mart",
                "CDC_LAKEHOUSE_ATHENA_OUTPUT_LOCATION": "s3://cdc-athena-results/",
                "CDC_LAKEHOUSE_ATHENA_WORKGROUP": "cdc-workgroup",
                "CDC_LAKEHOUSE_ATHENA_CATALOG": "AwsDataCatalog",
                "CDC_LAKEHOUSE_ATHENA_POLL_SECONDS": "0",
            })

            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "athena-mart-execution",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                env=env,
                check=True,
            )

            proof = json.loads(completed.stdout)
            aws_commands = aws_log.read_text()
            proof_artifact = Path(proof["proofArtifact"])
            self.assertTrue(proof_artifact.exists())
            proof_artifact_scenario = json.loads(proof_artifact.read_text())["scenario"]

        self.assertEqual("athena-mart-execution", proof["scenario"])
        self.assertEqual("athena-cli-opt-in", proof["evidenceLabel"])
        self.assertEqual("cdc_mart", proof["athenaDatabase"])
        self.assertEqual("cdc-workgroup", proof["athenaWorkgroup"])
        self.assertEqual("s3://cdc-athena-results/", proof["athenaOutputLocation"])
        self.assertEqual(3, len(proof["queryExecutions"]))
        self.assertEqual(["SUCCEEDED", "SUCCEEDED", "SUCCEEDED"], [query["state"] for query in proof["queryExecutions"]])
        self.assertEqual("athena-mart-execution", proof_artifact_scenario)
        self.assertIn("athena start-query-execution", aws_commands)
        self.assertIn("--query-execution-context Database=cdc_mart,Catalog=AwsDataCatalog", aws_commands)
        self.assertIn("--result-configuration OutputLocation=s3://cdc-athena-results/", aws_commands)
        self.assertIn("--work-group cdc-workgroup", aws_commands)
        self.assertIn("athena get-query-execution", aws_commands)

    def test_dbt_athena_run_requires_explicit_opt_in(self):
        env = os.environ.copy()
        env.pop("CDC_LAKEHOUSE_DBT_ATHENA_OPT_IN", None)

        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                "dbt-athena-run",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        self.assertNotEqual(0, completed.returncode)
        self.assertIn("CDC_LAKEHOUSE_DBT_ATHENA_OPT_IN=true", completed.stderr)

    def test_dbt_athena_run_invokes_project_with_profiles_dir_and_target(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            bin_dir = Path(tmp_dir) / "bin"
            bin_dir.mkdir()
            dbt_log = Path(tmp_dir) / "dbt.log"
            dbt = bin_dir / "dbt"
            dbt.write_text(
                "#!/usr/bin/env python3\n"
                "import pathlib, sys\n"
                f"log = pathlib.Path({str(dbt_log)!r})\n"
                "log.write_text(' '.join(sys.argv[1:]) + '\\n')\n"
                "print('dbt fake run ok')\n",
                encoding="utf-8",
            )
            dbt.chmod(dbt.stat().st_mode | stat.S_IXUSR)
            env = os.environ.copy()
            env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
            env.update({
                "CDC_LAKEHOUSE_DBT_ATHENA_OPT_IN": "true",
                "CDC_LAKEHOUSE_DBT_TARGET": "athena",
            })

            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "dbt-athena-run",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                env=env,
                check=True,
            )

            proof = json.loads(completed.stdout)
            dbt_command = dbt_log.read_text()
            self.assertTrue(Path(proof["profilesDir"]).exists())
            self.assertTrue((Path(proof["profilesDir"]) / "profiles.yml").exists())
            proof_artifact = Path(proof["proofArtifact"])
            self.assertTrue(proof_artifact.exists())
            proof_artifact_scenario = json.loads(proof_artifact.read_text())["scenario"]

        self.assertEqual("dbt-athena-run", proof["scenario"])
        self.assertEqual("dbt-athena-cli-opt-in", proof["evidenceLabel"])
        self.assertEqual(0, proof["dbtExitCode"])
        self.assertEqual(str(ROOT / "lakehouse" / "dbt"), proof["dbtProjectDir"])
        self.assertIn("dbt fake run ok", proof["dbtStdout"])
        self.assertEqual("dbt-athena-run", proof_artifact_scenario)
        self.assertIn("run --project-dir", dbt_command)
        self.assertIn(str(ROOT / "lakehouse" / "dbt"), dbt_command)
        self.assertIn("--profiles-dir", dbt_command)
        self.assertIn("--target athena", dbt_command)

    def test_lakehouse_proof_audit_scenario_is_registered(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                "--help",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertIn("lakehouse-proof-audit", completed.stdout)

    def test_lakehouse_proof_audit_reports_live_proof_gaps_without_opt_in(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            env = os.environ.copy()
            for key in [
                "CDC_LAKEHOUSE_S3_ENDPOINT",
                "CDC_LAKEHOUSE_S3_ACCESS_KEY",
                "CDC_LAKEHOUSE_S3_SECRET_KEY",
                "CDC_LAKEHOUSE_ICEBERG_ENGINE_CMD",
                "CDC_LAKEHOUSE_ATHENA_OPT_IN",
                "CDC_LAKEHOUSE_DBT_ATHENA_OPT_IN",
            ]:
                env.pop(key, None)

            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "lakehouse-proof-audit",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                env=env,
                check=True,
            )

        proof = json.loads(completed.stdout)

        self.assertEqual("lakehouse-proof-audit", proof["scenario"])
        self.assertEqual("lakehouse-proof-audit", proof["evidenceLabel"])
        self.assertFalse(proof["completionReady"])
        self.assertFalse(proof["localMvpReady"])
        self.assertFalse(proof["optionalCloudProofReady"])
        self.assertEqual("actual-live-proof-required", proof["auditMode"])

        unmet_ids = {item["id"] for item in proof["unmetRequirements"]}
        blocking_unmet_ids = {item["id"] for item in proof["blockingUnmetRequirements"]}
        optional_unmet_ids = {item["id"] for item in proof["optionalUnmetRequirements"]}
        self.assertIn("live-minio-object-sink", unmet_ids)
        self.assertIn("engine-backed-iceberg-bootstrap", unmet_ids)
        self.assertIn("engine-backed-iceberg-append", unmet_ids)
        self.assertIn("athena-dbt-runtime", unmet_ids)
        self.assertIn("live-minio-object-sink", blocking_unmet_ids)
        self.assertIn("engine-backed-iceberg-bootstrap", blocking_unmet_ids)
        self.assertIn("engine-backed-iceberg-append", blocking_unmet_ids)
        self.assertNotIn("athena-dbt-runtime", blocking_unmet_ids)
        self.assertIn("athena-dbt-runtime", optional_unmet_ids)

        requirements = {item["id"]: item for item in proof["requirements"]}
        self.assertEqual("satisfied-local-evidence", requirements["local-curated-jsonl"]["status"])
        self.assertEqual("satisfied-local-evidence", requirements["local-mart-validation"]["status"])
        self.assertEqual("missing-live-proof", requirements["live-minio-object-sink"]["status"])
        self.assertEqual("missing-engine-proof", requirements["engine-backed-iceberg-bootstrap"]["status"])
        self.assertEqual("missing-engine-proof", requirements["engine-backed-iceberg-append"]["status"])
        self.assertEqual("missing-athena-dbt-proof", requirements["athena-dbt-runtime"]["status"])
        self.assertEqual(
            [
                "minio-object-sink",
                "minio-compose-object-sink",
            ],
            requirements["live-minio-object-sink"]["acceptableScenarios"],
        )

    def test_lakehouse_proof_audit_marks_readiness_without_claiming_completion(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            bin_dir = Path(tmp_dir) / "bin"
            bin_dir.mkdir()
            fake_engine = bin_dir / "fake-iceberg-engine"
            fake_engine.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            fake_engine.chmod(fake_engine.stat().st_mode | stat.S_IXUSR)
            for command_name in ["aws", "dbt"]:
                command = bin_dir / command_name
                command.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
                command.chmod(command.stat().st_mode | stat.S_IXUSR)

            env = os.environ.copy()
            env["PATH"] = f"{bin_dir}{os.pathsep}{env['PATH']}"
            env.update({
                "CDC_LAKEHOUSE_S3_ENDPOINT": "http://127.0.0.1:9000",
                "CDC_LAKEHOUSE_S3_ACCESS_KEY": "minioadmin",
                "CDC_LAKEHOUSE_S3_SECRET_KEY": "minioadmin",
                "CDC_LAKEHOUSE_ICEBERG_ENGINE_CMD": str(fake_engine),
                "CDC_LAKEHOUSE_ATHENA_OPT_IN": "true",
                "CDC_LAKEHOUSE_DBT_ATHENA_OPT_IN": "true",
            })

            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "lakehouse-proof-audit",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                env=env,
                check=True,
            )

        proof = json.loads(completed.stdout)
        requirements = {item["id"]: item for item in proof["requirements"]}

        self.assertFalse(proof["completionReady"])
        self.assertFalse(proof["localMvpReady"])
        self.assertFalse(proof["optionalCloudProofReady"])
        self.assertEqual("ready-for-live-run", requirements["live-minio-object-sink"]["status"])
        self.assertEqual("ready-for-engine-run", requirements["engine-backed-iceberg-bootstrap"]["status"])
        self.assertEqual("ready-for-engine-run", requirements["engine-backed-iceberg-append"]["status"])
        self.assertEqual("ready-for-athena-dbt-run", requirements["athena-dbt-runtime"]["status"])
        self.assertIn("Athena/dbt-athena remains optional cloud proof", proof["notes"])

    def test_lakehouse_proof_audit_accepts_recorded_minio_live_proof(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            proof_dir = Path(tmp_dir) / "proofs"
            proof_dir.mkdir()
            proof_artifact = proof_dir / "minio-compose-object-sink.json"
            proof_artifact.write_text(
                json.dumps({
                    "scenario": "minio-compose-object-sink",
                    "evidenceLabel": "local-minio-live-endpoint",
                    "httpStatus": 200,
                    "eventId": "cdc_c17221c61d9646622322f39090f950c9afcba798",
                    "sourceLsn": 88432240,
                    "eventSourceOffset": {
                        "lsn": 88432240,
                        "txId": 773,
                        "sequence": ["26740416"],
                    },
                }),
                encoding="utf-8",
            )

            env = os.environ.copy()
            for key in [
                "CDC_LAKEHOUSE_S3_ENDPOINT",
                "CDC_LAKEHOUSE_S3_ACCESS_KEY",
                "CDC_LAKEHOUSE_S3_SECRET_KEY",
                "CDC_LAKEHOUSE_ICEBERG_ENGINE_CMD",
                "CDC_LAKEHOUSE_ATHENA_OPT_IN",
                "CDC_LAKEHOUSE_DBT_ATHENA_OPT_IN",
            ]:
                env.pop(key, None)

            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "lakehouse-proof-audit",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                env=env,
                check=True,
            )

        proof = json.loads(completed.stdout)
        requirements = {item["id"]: item for item in proof["requirements"]}
        unmet_ids = {item["id"] for item in proof["unmetRequirements"]}

        self.assertEqual("satisfied-live-proof", requirements["live-minio-object-sink"]["status"])
        self.assertEqual(str(proof_artifact), requirements["live-minio-object-sink"]["evidenceArtifact"])
        self.assertNotIn("live-minio-object-sink", unmet_ids)

    def test_lakehouse_proof_audit_accepts_recorded_engine_proofs(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            proof_dir = Path(tmp_dir) / "proofs"
            proof_dir.mkdir()
            bootstrap_artifact = proof_dir / "trino-compose-iceberg-bootstrap.json"
            append_artifact = proof_dir / "trino-compose-iceberg-append.json"
            bootstrap_artifact.write_text(
                json.dumps({
                    "scenario": "trino-compose-iceberg-bootstrap",
                    "evidenceLabel": "trino-iceberg-compose-opt-in",
                    "engineExitCode": 0,
                    "tableIdentifier": "iceberg.cdc.hiring_events",
                }),
                encoding="utf-8",
            )
            append_artifact.write_text(
                json.dumps({
                    "scenario": "trino-compose-iceberg-append",
                    "evidenceLabel": "trino-iceberg-compose-opt-in",
                    "engineExitCode": 0,
                    "tableIdentifier": "iceberg.cdc.hiring_events",
                    "appendedRecordCount": 3,
                }),
                encoding="utf-8",
            )

            env = os.environ.copy()
            for key in [
                "CDC_LAKEHOUSE_S3_ENDPOINT",
                "CDC_LAKEHOUSE_S3_ACCESS_KEY",
                "CDC_LAKEHOUSE_S3_SECRET_KEY",
                "CDC_LAKEHOUSE_ICEBERG_ENGINE_CMD",
                "CDC_LAKEHOUSE_ATHENA_OPT_IN",
                "CDC_LAKEHOUSE_DBT_ATHENA_OPT_IN",
            ]:
                env.pop(key, None)

            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "lakehouse-proof-audit",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                env=env,
                check=True,
            )

        proof = json.loads(completed.stdout)
        requirements = {item["id"]: item for item in proof["requirements"]}
        unmet_ids = {item["id"] for item in proof["unmetRequirements"]}

        self.assertEqual("satisfied-engine-proof", requirements["engine-backed-iceberg-bootstrap"]["status"])
        self.assertEqual("satisfied-engine-proof", requirements["engine-backed-iceberg-append"]["status"])
        self.assertEqual(str(bootstrap_artifact), requirements["engine-backed-iceberg-bootstrap"]["evidenceArtifact"])
        self.assertEqual(str(append_artifact), requirements["engine-backed-iceberg-append"]["evidenceArtifact"])
        self.assertNotIn("engine-backed-iceberg-bootstrap", unmet_ids)
        self.assertNotIn("engine-backed-iceberg-append", unmet_ids)

    def test_lakehouse_proof_audit_completes_local_mvp_with_minio_and_engine_proofs(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            proof_dir = Path(tmp_dir) / "proofs"
            proof_dir.mkdir()
            minio_artifact = proof_dir / "minio-compose-object-sink.json"
            bootstrap_artifact = proof_dir / "iceberg-engine-bootstrap.json"
            append_artifact = proof_dir / "iceberg-engine-append.json"
            minio_artifact.write_text(
                json.dumps({
                    "scenario": "minio-compose-object-sink",
                    "evidenceLabel": "local-minio-live-endpoint",
                    "httpStatus": 200,
                    "eventId": "cdc_c17221c61d9646622322f39090f950c9afcba798",
                    "sourceLsn": 88432240,
                    "eventSourceOffset": {"lsn": 88432240},
                }),
                encoding="utf-8",
            )
            bootstrap_artifact.write_text(
                json.dumps({
                    "scenario": "iceberg-engine-bootstrap",
                    "evidenceLabel": "iceberg-engine-opt-in",
                    "engineExitCode": 0,
                    "tableIdentifier": "cdc.hiring_events",
                }),
                encoding="utf-8",
            )
            append_artifact.write_text(
                json.dumps({
                    "scenario": "iceberg-engine-append",
                    "evidenceLabel": "iceberg-engine-opt-in",
                    "engineExitCode": 0,
                    "tableIdentifier": "cdc.hiring_events",
                    "appendedRecordCount": 3,
                }),
                encoding="utf-8",
            )

            env = os.environ.copy()
            for key in [
                "CDC_LAKEHOUSE_S3_ENDPOINT",
                "CDC_LAKEHOUSE_S3_ACCESS_KEY",
                "CDC_LAKEHOUSE_S3_SECRET_KEY",
                "CDC_LAKEHOUSE_ICEBERG_ENGINE_CMD",
                "CDC_LAKEHOUSE_ATHENA_OPT_IN",
                "CDC_LAKEHOUSE_DBT_ATHENA_OPT_IN",
            ]:
                env.pop(key, None)

            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "lakehouse-proof-audit",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                env=env,
                check=True,
            )

        proof = json.loads(completed.stdout)
        requirements = {item["id"]: item for item in proof["requirements"]}
        blocking_unmet_ids = {item["id"] for item in proof["blockingUnmetRequirements"]}
        optional_unmet_ids = {item["id"] for item in proof["optionalUnmetRequirements"]}

        self.assertTrue(proof["completionReady"])
        self.assertTrue(proof["localMvpReady"])
        self.assertFalse(proof["optionalCloudProofReady"])
        self.assertEqual(set(), blocking_unmet_ids)
        self.assertIn("athena-dbt-runtime", optional_unmet_ids)
        self.assertEqual("optional-cloud-proof", requirements["athena-dbt-runtime"]["requirementScope"])

    def test_lakehouse_proof_audit_accepts_recorded_optional_cloud_proof(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            proof_dir = Path(tmp_dir) / "proofs"
            proof_dir.mkdir()
            (proof_dir / "minio-compose-object-sink.json").write_text(
                json.dumps({
                    "scenario": "minio-compose-object-sink",
                    "evidenceLabel": "local-minio-live-endpoint",
                    "httpStatus": 200,
                    "eventId": "cdc_c17221c61d9646622322f39090f950c9afcba798",
                    "sourceLsn": 88432240,
                    "eventSourceOffset": {"lsn": 88432240},
                }),
                encoding="utf-8",
            )
            (proof_dir / "iceberg-engine-bootstrap.json").write_text(
                json.dumps({
                    "scenario": "iceberg-engine-bootstrap",
                    "evidenceLabel": "iceberg-engine-opt-in",
                    "engineExitCode": 0,
                    "tableIdentifier": "cdc.hiring_events",
                }),
                encoding="utf-8",
            )
            (proof_dir / "iceberg-engine-append.json").write_text(
                json.dumps({
                    "scenario": "iceberg-engine-append",
                    "evidenceLabel": "iceberg-engine-opt-in",
                    "engineExitCode": 0,
                    "tableIdentifier": "cdc.hiring_events",
                    "appendedRecordCount": 3,
                }),
                encoding="utf-8",
            )
            optional_artifact = proof_dir / "dbt-athena-run.json"
            optional_artifact.write_text(
                json.dumps({
                    "scenario": "dbt-athena-run",
                    "evidenceLabel": "dbt-athena-cli-opt-in",
                    "dbtExitCode": 0,
                    "target": "athena",
                }),
                encoding="utf-8",
            )

            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "lakehouse-proof-audit",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )

        proof = json.loads(completed.stdout)
        requirements = {item["id"]: item for item in proof["requirements"]}

        self.assertTrue(proof["completionReady"])
        self.assertTrue(proof["localMvpReady"])
        self.assertTrue(proof["optionalCloudProofReady"])
        self.assertEqual([], proof["blockingUnmetRequirements"])
        self.assertEqual([], proof["optionalUnmetRequirements"])
        self.assertEqual("satisfied-athena-dbt-proof", requirements["athena-dbt-runtime"]["status"])
        self.assertEqual(str(optional_artifact), requirements["athena-dbt-runtime"]["evidenceArtifact"])

    def test_iceberg_engine_bootstrap_requires_engine_command(self):
        env = os.environ.copy()
        env.pop("CDC_LAKEHOUSE_ICEBERG_ENGINE_CMD", None)

        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                "iceberg-engine-bootstrap",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        self.assertNotEqual(0, completed.returncode)
        self.assertIn("CDC_LAKEHOUSE_ICEBERG_ENGINE_CMD", completed.stderr)

    def test_iceberg_engine_bootstrap_passes_create_table_sql_to_configured_command(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            sql_path = Path(tmp_dir) / "bootstrap.sql"
            fake_engine = Path(tmp_dir) / "fake-iceberg-engine.py"
            fake_engine.write_text(
                "#!/usr/bin/env python3\n"
                "import pathlib, sys\n"
                f"pathlib.Path({str(sql_path)!r}).write_text(sys.stdin.read())\n"
                "print('fake iceberg bootstrap ok')\n",
                encoding="utf-8",
            )
            fake_engine.chmod(fake_engine.stat().st_mode | stat.S_IXUSR)
            env = os.environ.copy()
            env["CDC_LAKEHOUSE_ICEBERG_ENGINE_CMD"] = str(fake_engine)

            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "iceberg-engine-bootstrap",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                env=env,
                check=True,
            )

            proof = json.loads(completed.stdout)
            sql = sql_path.read_text()

            self.assertEqual("iceberg-engine-bootstrap", proof["scenario"])
            self.assertEqual("iceberg-engine-opt-in", proof["evidenceLabel"])
            self.assertEqual(0, proof["engineExitCode"])
            proof_artifact = Path(proof["proofArtifact"])
            self.assertTrue(proof_artifact.exists())
            self.assertEqual("iceberg-engine-bootstrap", json.loads(proof_artifact.read_text())["scenario"])
            self.assertIn("fake iceberg bootstrap ok", proof["engineStdout"])
            self.assertEqual("cdc.hiring_events", proof["tableIdentifier"])
            self.assertTrue(sql.startswith("CREATE TABLE IF NOT EXISTS cdc.hiring_events"))
            self.assertIn("USING iceberg", sql)
            self.assertIn("PARTITIONED BY (event_date)", sql)

    def test_iceberg_engine_append_passes_insert_sql_and_writes_data_file(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            sql_path = Path(tmp_dir) / "append.sql"
            fake_engine = Path(tmp_dir) / "fake-iceberg-engine.py"
            fake_engine.write_text(
                "#!/usr/bin/env python3\n"
                "import pathlib, sys\n"
                f"pathlib.Path({str(sql_path)!r}).write_text(sys.stdin.read())\n"
                "print('fake iceberg append ok')\n",
                encoding="utf-8",
            )
            fake_engine.chmod(fake_engine.stat().st_mode | stat.S_IXUSR)
            env = os.environ.copy()
            env["CDC_LAKEHOUSE_ICEBERG_ENGINE_CMD"] = str(fake_engine)

            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "iceberg-engine-append",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                env=env,
                check=True,
            )

            proof = json.loads(completed.stdout)
            sql = sql_path.read_text()

            self.assertEqual("iceberg-engine-append", proof["scenario"])
            self.assertEqual("iceberg-engine-opt-in", proof["evidenceLabel"])
            self.assertEqual(0, proof["engineExitCode"])
            proof_artifact = Path(proof["proofArtifact"])
            self.assertTrue(proof_artifact.exists())
            self.assertEqual("iceberg-engine-append", json.loads(proof_artifact.read_text())["scenario"])
            self.assertEqual(3, proof["appendedRecordCount"])
            self.assertTrue(Path(proof["dataFile"]).exists())
            self.assertIn("fake iceberg append ok", proof["engineStdout"])
            self.assertIn("INSERT INTO cdc.hiring_events", sql)
            self.assertIn("cdc_local_applicant_created", sql)
            self.assertIn("cdc_local_agent_task_completed", sql)

    def test_iceberg_metadata_bootstrap_writes_versioned_metadata_json(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "iceberg-metadata-bootstrap",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )

            proof = json.loads(completed.stdout)

            self.assertEqual("iceberg-metadata-bootstrap", proof["scenario"])
            self.assertEqual("local-iceberg-metadata", proof["evidenceLabel"])
            self.assertEqual("hiring_events", proof["tableName"])
            self.assertEqual(2, proof["formatVersion"])
            self.assertEqual(0, proof["snapshotCount"])
            self.assertTrue(proof["metadataFile"].endswith(".metadata.json"))

            metadata = json.loads(Path(proof["metadataFile"]).read_text())
            self.assertEqual(2, metadata["format-version"])
            self.assertEqual(proof["tableUuid"], metadata["table-uuid"])
            self.assertEqual("hiring_events", metadata["properties"]["table.name"])
            self.assertEqual(0, metadata["current-schema-id"])
            self.assertEqual([], metadata["snapshots"])

    def test_iceberg_append_fixture_writes_new_snapshot_metadata(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "lakehouse-smoke"),
                    "iceberg-append-fixture",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )

            proof = json.loads(completed.stdout)

            self.assertEqual("iceberg-append-fixture", proof["scenario"])
            self.assertEqual("local-iceberg-metadata", proof["evidenceLabel"])
            self.assertEqual(3, proof["appendedRecordCount"])
            self.assertEqual(1, proof["snapshotCount"])
            self.assertTrue(proof["currentSnapshotId"])
            self.assertTrue(Path(proof["dataFile"]).exists())

            metadata = json.loads(Path(proof["metadataFile"]).read_text())
            self.assertEqual(proof["currentSnapshotId"], metadata["current-snapshot-id"])
            self.assertEqual(1, len(metadata["snapshots"]))
            self.assertEqual("append", metadata["snapshots"][0]["summary"]["operation"])
            self.assertEqual("3", metadata["snapshots"][0]["summary"]["added-records"])
            self.assertEqual(1, len(metadata["snapshot-log"]))


if __name__ == "__main__":
    unittest.main()
