#!/usr/bin/env python3
import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class ProjectAuditTest(unittest.TestCase):

    def test_project_audit_reports_repository_boundary_todo_and_lakehouse_gaps(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "project-audit"),
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        proof = json.loads(completed.stdout)

        self.assertEqual("project-completion-audit", proof["scenario"])
        self.assertEqual("project-completion-audit", proof["evidenceLabel"])
        self.assertTrue(proof["completionReady"])
        self.assertEqual("local-first-mvp", proof["completionScope"])
        self.assertEqual(str(ROOT), proof["repository"]["gitRoot"])
        self.assertTrue(proof["repository"]["insideExpectedRoot"])

        self.assertGreaterEqual(proof["todo"]["uncheckedCount"], 1)
        self.assertEqual(0, proof["todo"]["mvpUncheckedCount"])
        self.assertGreaterEqual(proof["todo"]["optionalUncheckedCount"], 1)
        optional_texts = [item["text"] for item in proof["todo"]["optionalUncheckedItems"]]
        self.assertIn("opt-in Athena/dbt-athena mart execution proof 확보", optional_texts)
        mvp_unchecked_texts = [item["text"] for item in proof["todo"]["mvpUncheckedItems"]]
        self.assertNotIn("engine-backed Iceberg table bootstrap proof 확보", mvp_unchecked_texts)
        self.assertNotIn("engine-backed curated event -> Iceberg append proof 확보", mvp_unchecked_texts)

        lakehouse = proof["lakehouseProofAudit"]
        self.assertEqual("lakehouse-proof-audit", lakehouse["scenario"])
        self.assertTrue(lakehouse["completionReady"])
        self.assertTrue(lakehouse["localMvpReady"])
        self.assertFalse(lakehouse["optionalCloudProofReady"])
        requirements = {item["id"]: item for item in lakehouse["requirements"]}
        unmet_ids = {item["id"] for item in lakehouse["unmetRequirements"]}
        blocking_unmet_ids = {item["id"] for item in lakehouse["blockingUnmetRequirements"]}
        optional_unmet_ids = {item["id"] for item in lakehouse["optionalUnmetRequirements"]}
        self.assertIn("athena-dbt-runtime", unmet_ids)
        self.assertNotIn("athena-dbt-runtime", blocking_unmet_ids)
        self.assertIn("athena-dbt-runtime", optional_unmet_ids)
        self.assertIn(
            requirements["live-minio-object-sink"]["status"],
            {"missing-live-proof", "satisfied-live-proof"},
        )
        self.assertIn(
            requirements["engine-backed-iceberg-bootstrap"]["status"],
            {"missing-engine-proof", "satisfied-engine-proof"},
        )
        self.assertIn(
            requirements["engine-backed-iceberg-append"]["status"],
            {"missing-engine-proof", "satisfied-engine-proof"},
        )

        phase2 = proof["phase2RuntimeSlaProofAudit"]
        self.assertTrue(phase2["completionReady"])
        self.assertEqual("satisfied-paired-runtime-sla-proof", phase2["status"])
        self.assertEqual("quality-sla-runtime", phase2["runtimeObservation"]["scenario"])
        self.assertEqual("quality-sla-runtime-record", phase2["controlPlanePersistence"]["scenario"])
        self.assertEqual(
            phase2["runtimeObservation"]["eventIds"],
            phase2["controlPlanePersistence"]["eventIds"],
        )
        self.assertEqual(
            phase2["runtimeObservation"]["eventSourceOffset"],
            phase2["controlPlanePersistence"]["eventSourceOffset"],
        )

        replay = proof["phase2RuntimeLakehouseReplayAudit"]
        self.assertTrue(replay["completionReady"])
        self.assertEqual("satisfied-runtime-lakehouse-replay-proof", replay["status"])
        self.assertEqual("control-plane-runtime-handoff", replay["handoff"]["scenario"])
        self.assertEqual("runtime-lakehouse-replay", replay["replay"]["scenario"])
        self.assertEqual("control-plane-runtime-handoff", replay["replay"]["sourceProofScenario"])
        self.assertEqual(replay["handoff"]["sourceLsn"], replay["replay"]["sourceLsn"])
        self.assertEqual(replay["handoff"]["eventIds"], replay["replay"]["eventIds"])
        self.assertEqual(replay["handoff"]["eventSourceOffset"], replay["replay"]["eventSourceOffset"])
        self.assertEqual(2, replay["replay"]["replayAttemptCount"])
        self.assertEqual(1, replay["replay"]["suppressedDuplicateCount"])
        self.assertTrue(replay["replay"]["idempotencyKeys"][0].startswith("sha256:"))

    def test_project_audit_blocks_completion_when_phase2_runtime_sla_pair_is_missing(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "project-audit"),
                    "--proof-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )

            proof = json.loads(completed.stdout)

        self.assertFalse(proof["completionReady"])
        phase2 = proof["phase2RuntimeSlaProofAudit"]
        self.assertFalse(phase2["completionReady"])
        self.assertEqual("missing-runtime-sla-proof", phase2["status"])
        next_action_text = "\n".join(item["text"] for item in proof["nextActions"])
        self.assertIn("cdc-smoke quality-sla-runtime", next_action_text)
        self.assertIn("control-plane-smoke quality-sla-runtime-record", next_action_text)

    def test_project_audit_blocks_completion_when_runtime_lakehouse_replay_is_missing(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            proof_dir = Path(tmp_dir)
            default_proof_dir = ROOT / "lakehouse" / "out" / "proofs"
            for artifact_name in [
                "quality-sla-runtime.json",
                "quality-sla-runtime-record.json",
                "control-plane-runtime-handoff.json",
            ]:
                shutil.copy(default_proof_dir / artifact_name, proof_dir / artifact_name)

            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "project-audit"),
                    "--proof-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )

            proof = json.loads(completed.stdout)

        self.assertFalse(proof["completionReady"])
        replay = proof["phase2RuntimeLakehouseReplayAudit"]
        self.assertFalse(replay["completionReady"])
        self.assertEqual("missing-runtime-lakehouse-replay-proof", replay["status"])
        next_action_text = "\n".join(item["text"] for item in proof["nextActions"])
        self.assertIn("lakehouse-smoke runtime-lakehouse-replay", next_action_text)

    def test_project_audit_help_describes_completion_gate(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "project-audit"),
                "--help",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertIn("project completion audit", completed.stdout)
        self.assertIn("--todo-file", completed.stdout)
        self.assertIn("--write-completion-report", completed.stdout)

    def test_project_audit_writes_next_proof_manifest_without_install_or_aws_defaults(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "project-audit"),
                    "--write-next-proof-manifest",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )

            proof = json.loads(completed.stdout)
            manifest_path = Path(proof["artifacts"]["nextProofManifest"])
            audit_path = Path(proof["artifacts"]["auditJson"])
            self.assertTrue(manifest_path.exists())
            self.assertTrue(audit_path.exists())
            manifest = manifest_path.read_text()

        self.assertIn("Next Proof Commands", manifest)
        self.assertIn("./tools/runner/lakehouse-smoke minio-object-sink", manifest)
        self.assertIn("./tools/runner/lakehouse-smoke minio-compose-object-sink", manifest)
        self.assertIn("--pull never", manifest)
        self.assertIn("CDC_LAKEHOUSE_ICEBERG_ENGINE_CMD=", manifest)
        self.assertIn("./tools/runner/iceberg-java-engine", manifest)
        self.assertIn("./tools/runner/lakehouse-smoke iceberg-engine-bootstrap", manifest)
        self.assertIn("./tools/runner/lakehouse-smoke iceberg-engine-append", manifest)
        self.assertIn("./tools/runner/lakehouse-smoke trino-compose-iceberg-bootstrap", manifest)
        self.assertIn("./tools/runner/lakehouse-smoke trino-compose-iceberg-append", manifest)
        self.assertIn("CDC_LAKEHOUSE_ATHENA_OPT_IN=true", manifest)
        self.assertIn("CDC_LAKEHOUSE_DBT_ATHENA_OPT_IN=true", manifest)
        self.assertIn("./tools/runner/lakehouse-smoke athena-mart-execution", manifest)
        self.assertIn("./tools/runner/lakehouse-smoke dbt-athena-run", manifest)
        self.assertIn("This manifest is not proof", manifest)
        self.assertNotIn("docker pull", manifest)
        self.assertNotIn("AKIA", manifest)

    def test_project_audit_writes_completion_report_with_local_scope_and_evidence(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "project-audit"),
                    "--write-completion-report",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )

            proof = json.loads(completed.stdout)
            report_path = Path(proof["artifacts"]["completionReport"])
            self.assertTrue(report_path.exists())
            report = report_path.read_text()

        self.assertIn("Project 05 Local-First Completion Report", report)
        self.assertIn("completionReady: true", report)
        self.assertIn("completionScope: local-first-mvp", report)
        self.assertIn("optionalCloudProofReady: false", report)
        self.assertIn("source LSN", report)
        self.assertIn("event source offset", report)
        self.assertIn("event id", report)
        self.assertIn("AWS Athena/dbt-athena remains opt-in", report)
        self.assertIn("./tools/runner/project-audit --write-next-proof-manifest --write-completion-report", report)
        self.assertNotIn("docker pull", report)
        self.assertNotIn("AKIA", report)

    def test_project_audit_runs_local_verification_bundle(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            completed = subprocess.run(
                [
                    str(ROOT / "tools" / "runner" / "project-audit"),
                    "--run-local-verification",
                    "--output-dir",
                    tmp_dir,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )

            proof = json.loads(completed.stdout)
            verification_path = Path(proof["artifacts"]["localVerificationJson"])
            self.assertTrue(verification_path.exists())
            verification = json.loads(verification_path.read_text())

        self.assertEqual("local-verification", verification["evidenceLabel"])
        self.assertFalse(verification["includesLiveProof"])
        command_ids = [item["id"] for item in verification["commands"]]
        self.assertIn("runner-py-compile", command_ids)
        self.assertIn("lakehouse-proof-audit-focused-test", command_ids)
        self.assertIn("compose-config", command_ids)
        self.assertIn("project-audit-manifest", command_ids)
        self.assertTrue(all(item["exitCode"] == 0 for item in verification["commands"]))
        self.assertTrue(proof["completionReady"])


if __name__ == "__main__":
    unittest.main()
