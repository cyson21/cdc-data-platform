#!/usr/bin/env python3
import json
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class ControlPlaneSmokeTest(unittest.TestCase):

    def test_canonical_ingest_runs_raw_envelope_flow(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "control-plane-smoke"),
                "canonical-ingest",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        proof = json.loads(completed.stdout)

        self.assertEqual("canonical-ingest", proof["scenario"])
        self.assertEqual("unit-service-api", proof["evidenceLabel"])
        self.assertEqual("applicants", proof["sourceTable"])
        self.assertEqual("19c14e85-9840-7a5a-bb16-3c7f70547d9f", proof["sourcePrimaryKey"])
        self.assertEqual(88432240, proof["sourceLsn"])
        self.assertEqual(88432240, proof["eventSourceOffset"]["lsn"])
        self.assertEqual(773, proof["eventSourceOffset"]["txId"])
        self.assertTrue(proof["eventId"].startswith("cdc_"))
        self.assertEqual(1, proof["canonicalEventCount"])
        self.assertEqual(1, proof["duplicateEventCount"])
        self.assertEqual("passed", proof["testResult"])

    def test_sink_failure_replay_runs_db_backed_flow(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "control-plane-smoke"),
                "sink-failure-replay",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        proof = json.loads(completed.stdout)

        self.assertEqual("sink-failure-replay", proof["scenario"])
        self.assertEqual("testcontainers-postgres", proof["evidenceLabel"])
        self.assertEqual("applicants", proof["sourceTable"])
        self.assertEqual("19c14e85-9840-7a5a-bb16-3c7f70547d9f", proof["sourcePrimaryKey"])
        self.assertEqual(88432240, proof["sourceLsn"])
        self.assertEqual("REQUESTED", proof["replayStatus"])
        self.assertEqual("PUBLISHED", proof["retryStatus"])
        self.assertEqual("cdc.retry.applicants", proof["retryTopic"])
        self.assertTrue(proof["duplicateLedgerBlocked"])
        self.assertEqual("passed", proof["testResult"])

    def test_kafka_outage_recovery_runs_db_backed_flow(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "control-plane-smoke"),
                "kafka-outage-recovery",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        proof = json.loads(completed.stdout)

        self.assertEqual("kafka-outage-recovery", proof["scenario"])
        self.assertEqual("testcontainers-postgres-recovery", proof["evidenceLabel"])
        self.assertEqual("KAFKA_OUTAGE", proof["recoveryScenario"])
        self.assertEqual("ats-source", proof["connectorName"])
        self.assertEqual("applicants", proof["sourceTable"])
        self.assertEqual(88432240, proof["sourceLsnFrom"])
        self.assertEqual(88432288, proof["sourceLsnTo"])
        self.assertEqual(88432240, proof["sourceOffsetFrom"]["lsn"])
        self.assertEqual(88432288, proof["sourceOffsetTo"]["lsn"])
        self.assertTrue(proof["eventId"].startswith("cdc_"))
        self.assertEqual("RECOVERED", proof["recoveryStatus"])
        self.assertEqual(3, proof["recoveredEventCount"])
        self.assertEqual(1, proof["duplicateEventCount"])
        self.assertFalse(proof["gapDetected"])
        self.assertEqual("passed", proof["testResult"])

    def test_connector_restart_recovery_runs_db_backed_flow(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "control-plane-smoke"),
                "connector-restart-recovery",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        proof = json.loads(completed.stdout)

        self.assertEqual("connector-restart-recovery", proof["scenario"])
        self.assertEqual("testcontainers-postgres-recovery", proof["evidenceLabel"])
        self.assertEqual("CONNECTOR_RESTART", proof["recoveryScenario"])
        self.assertEqual("ats-source", proof["connectorName"])
        self.assertEqual("applicants", proof["sourceTable"])
        self.assertEqual(88432288, proof["sourceLsnFrom"])
        self.assertEqual(88432320, proof["sourceLsnTo"])
        self.assertEqual(88432288, proof["sourceOffsetFrom"]["lsn"])
        self.assertEqual(88432320, proof["sourceOffsetTo"]["lsn"])
        self.assertTrue(proof["eventId"].startswith("cdc_"))
        self.assertEqual("RECOVERED", proof["recoveryStatus"])
        self.assertEqual(1, proof["recoveredEventCount"])
        self.assertEqual(0, proof["duplicateEventCount"])
        self.assertFalse(proof["gapDetected"])
        self.assertEqual("passed", proof["testResult"])

    def test_kafka_backed_sink_failure_replay_publishes_to_embedded_broker(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "control-plane-smoke"),
                "kafka-backed-sink-failure-replay",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        proof = json.loads(completed.stdout)

        self.assertEqual("kafka-backed-sink-failure-replay", proof["scenario"])
        self.assertEqual("embedded-kafka-broker", proof["evidenceLabel"])
        self.assertEqual("candidate-cdc-evt-kafka-1", proof["eventId"])
        self.assertEqual(88432400, proof["sourceLsn"])
        self.assertEqual(88432400, proof["sourceOffset"]["lsn"])
        self.assertEqual("cdc.retry.applicants", proof["retryTopic"])
        self.assertGreaterEqual(proof["brokerPartition"], 0)
        self.assertGreaterEqual(proof["brokerOffset"], 0)
        self.assertTrue(proof["brokerAccepted"])
        self.assertEqual("PUBLISHED", proof["retryStatus"])
        self.assertEqual(1, proof["attemptCount"])
        self.assertEqual("passed", proof["testResult"])

    def test_pipeline_quality_reports_completeness_with_source_metadata(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "control-plane-smoke"),
                "pipeline-quality",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        proof = json.loads(completed.stdout)

        self.assertEqual("pipeline-quality", proof["scenario"])
        self.assertEqual("unit-service-api", proof["evidenceLabel"])
        self.assertEqual("applicant-cdc-completeness", proof["checkName"])
        self.assertEqual("applicants", proof["sourceTable"])
        self.assertEqual(3, proof["sourceRowCount"])
        self.assertEqual(3, proof["rawEventCount"])
        self.assertEqual(3, proof["canonicalEventCount"])
        self.assertEqual(0, proof["duplicateEventCount"])
        self.assertEqual(0, proof["missingEventCount"])
        self.assertEqual("PASSED", proof["checkStatus"])
        self.assertEqual(88432240, proof["sourceLsnFrom"])
        self.assertEqual(88432288, proof["sourceLsnTo"])
        self.assertTrue(proof["eventIds"][0].startswith("cdc_"))
        self.assertEqual("passed", proof["testResult"])

    def test_pipeline_quality_db_records_and_reads_latest_check(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "control-plane-smoke"),
                "pipeline-quality-db",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        proof = json.loads(completed.stdout)

        self.assertEqual("pipeline-quality-db", proof["scenario"])
        self.assertEqual("testcontainers-postgres-quality", proof["evidenceLabel"])
        self.assertEqual("applicant-cdc-completeness", proof["checkName"])
        self.assertEqual("applicants", proof["sourceTable"])
        self.assertEqual("PASSED", proof["checkStatus"])
        self.assertEqual(88432240, proof["sourceLsnFrom"])
        self.assertEqual(88432288, proof["sourceLsnTo"])
        self.assertGreaterEqual(proof["latestQualityCheckId"], 1)
        self.assertEqual("passed", proof["testResult"])

    def test_quality_sla_reports_freshness_completeness_duplicate_and_lag(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "control-plane-smoke"),
                "quality-sla",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        proof = json.loads(completed.stdout)

        self.assertEqual("quality-sla", proof["scenario"])
        self.assertEqual("unit-service-api-sla", proof["evidenceLabel"])
        self.assertEqual("applicant-cdc-sla", proof["checkName"])
        self.assertEqual("applicants", proof["sourceTable"])
        self.assertEqual("FAILED", proof["slaStatus"])
        self.assertEqual(0.98, proof["completenessRatio"])
        self.assertAlmostEqual(0.0196, proof["duplicateRatio"], places=4)
        self.assertEqual(90000, proof["freshnessLagMillis"])
        self.assertEqual(180000, proof["lagMillis"])
        self.assertEqual(
            ["completeness", "duplicate-rate", "freshness", "lag"],
            proof["violatedSloIds"],
        )
        self.assertEqual(88432240, proof["sourceLsnFrom"])
        self.assertEqual(88432320, proof["sourceLsnTo"])
        self.assertTrue(proof["eventIds"][0].startswith("cdc_"))
        self.assertEqual("passed", proof["testResult"])

    def test_quality_sla_db_records_and_reads_latest_evaluation(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "control-plane-smoke"),
                "quality-sla-db",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        proof = json.loads(completed.stdout)

        self.assertEqual("quality-sla-db", proof["scenario"])
        self.assertEqual("testcontainers-postgres-sla", proof["evidenceLabel"])
        self.assertEqual("applicant-cdc-sla", proof["checkName"])
        self.assertEqual("applicants", proof["sourceTable"])
        self.assertEqual("FAILED", proof["slaStatus"])
        self.assertEqual(0.98, proof["completenessRatio"])
        self.assertAlmostEqual(0.0196, proof["duplicateRatio"], places=4)
        self.assertEqual(90000, proof["freshnessLagMillis"])
        self.assertEqual(180000, proof["lagMillis"])
        self.assertEqual(88432240, proof["sourceLsnFrom"])
        self.assertEqual(88432320, proof["sourceLsnTo"])
        self.assertGreaterEqual(proof["latestSlaEvaluationId"], 1)
        self.assertTrue(proof["eventIds"][0].startswith("cdc_"))
        self.assertEqual("passed", proof["testResult"])

    def test_quality_sla_runtime_record_persists_runtime_observation_evidence(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "control-plane-smoke"),
                    "--proof-dir",
                    tmp_dir,
                    "quality-sla-runtime-record",
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )

        proof = json.loads(completed.stdout)

        self.assertEqual("quality-sla-runtime-record", proof["scenario"])
        self.assertEqual("testcontainers-postgres-runtime-sla", proof["evidenceLabel"])
        self.assertEqual("local-docker-debezium-runtime-sla", proof["runtimeEvidenceLabel"])
        self.assertEqual("applicant-cdc-runtime-sla", proof["checkName"])
        self.assertEqual("applicants", proof["sourceTable"])
        self.assertEqual("WARN", proof["slaStatus"])
        self.assertEqual(["source-lsn-lag-unavailable"], proof["warningSloIds"])
        self.assertEqual(1.0, proof["completenessRatio"])
        self.assertEqual(0.0, proof["duplicateRatio"])
        self.assertEqual(1580, proof["freshnessLagMillis"])
        self.assertEqual(0, proof["lagMillis"])
        self.assertEqual(26710752, proof["sourceLsnFrom"])
        self.assertEqual(26711584, proof["sourceLsnTo"])
        self.assertIsNone(proof["sourceConnectorOffsetLsn"])
        self.assertIsNone(proof["sourceLsnLag"])
        self.assertEqual(26710752, proof["eventSourceOffset"][0]["lsn"])
        self.assertEqual(761, proof["eventSourceOffset"][0]["txId"])
        self.assertEqual(3, len(proof["eventIds"]))
        self.assertTrue(proof["eventIds"][0].startswith("cdc_"))
        self.assertGreaterEqual(proof["latestSlaEvaluationId"], 1)
        self.assertEqual("passed", proof["testResult"])

    def test_quality_sla_runtime_record_reuses_runtime_observation_artifact_metadata(self):
        runtime_proof = {
            "scenario": "quality-sla-runtime",
            "evidenceLabel": "local-docker-debezium-runtime-sla",
            "sourceTable": "applicants",
            "sourcePrimaryKey": "runtime-applicant-from-artifact",
            "sourceLsn": [3001, 3002, 3003],
            "sourceLsnFrom": 3001,
            "sourceLsnTo": 3003,
            "eventSourceOffset": [
                {"lsn": 3001, "sequence": "[null,\"3001\"]", "txId": 801},
                {"lsn": 3002, "sequence": "[\"3002\",\"3002\"]", "txId": 802},
                {"lsn": 3003, "sequence": "[\"3003\",\"3003\"]", "txId": 803},
            ],
            "eventIds": ["cdc_runtime_1", "cdc_runtime_2", "cdc_runtime_3"],
            "rawEventCount": 3,
            "observedRuntimeEventCount": 3,
            "duplicateEventCount": 0,
            "missingEventCount": 0,
            "completenessRatio": 1.0,
            "duplicateRatio": 0.0,
            "freshnessLagMillis": 3210,
            "sourceConnectorOffsetLsn": None,
            "sourceLsnLag": None,
            "sourceOffset": {"offsets": []},
            "slaStatus": "WARN",
            "violatedSloIds": [],
            "warningSloIds": ["source-lsn-lag-unavailable"],
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            runtime_path = Path(tmp_dir) / "quality-sla-runtime.json"
            runtime_path.write_text(json.dumps(runtime_proof), encoding="utf-8")
            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "control-plane-smoke"),
                    "--proof-dir",
                    tmp_dir,
                    "quality-sla-runtime-record",
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )

            proof = json.loads(completed.stdout)

        self.assertEqual("runtime-applicant-from-artifact", proof["sourcePrimaryKey"])
        self.assertEqual([3001, 3002, 3003], proof["sourceLsn"])
        self.assertEqual(["cdc_runtime_1", "cdc_runtime_2", "cdc_runtime_3"], proof["eventIds"])
        self.assertEqual(runtime_proof["eventSourceOffset"], proof["eventSourceOffset"])
        self.assertEqual(3210, proof["freshnessLagMillis"])
        self.assertEqual(["source-lsn-lag-unavailable"], proof["warningSloIds"])

    def test_quality_sla_runtime_record_writes_proof_artifact(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "control-plane-smoke"),
                    "--proof-dir",
                    tmp_dir,
                    "quality-sla-runtime-record",
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )

            proof = json.loads(completed.stdout)
            artifact_path = Path(tmp_dir) / "quality-sla-runtime-record.json"
            self.assertTrue(artifact_path.exists())
            artifact = json.loads(artifact_path.read_text())

        self.assertEqual(proof, artifact)
        self.assertEqual("quality-sla-runtime-record", artifact["scenario"])
        self.assertEqual("testcontainers-postgres-runtime-sla", artifact["evidenceLabel"])
        self.assertEqual("local-docker-debezium-runtime-sla", artifact["runtimeEvidenceLabel"])
        self.assertEqual([26710752, 26711320, 26711584], artifact["sourceLsn"])
        self.assertEqual(26710752, artifact["eventSourceOffset"][0]["lsn"])
        self.assertEqual(["source-lsn-lag-unavailable"], artifact["warningSloIds"])
        self.assertEqual(artifact["eventId"], artifact["eventIds"])


if __name__ == "__main__":
    unittest.main()
