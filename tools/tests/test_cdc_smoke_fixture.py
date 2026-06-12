#!/usr/bin/env python3
import json
import os
import subprocess
import tempfile
import unittest
import importlib.util
import urllib.error
from importlib.machinery import SourceFileLoader
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CONNECTOR_FILE = ROOT / "infra" / "local" / "debezium-postgres-connector.json"


def load_cdc_smoke_module():
    module_path = ROOT / "tools" / "runner" / "cdc-smoke"
    spec = importlib.util.spec_from_loader("cdc_smoke", SourceFileLoader("cdc_smoke", str(module_path)))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def source_event_id(source_connector, source_schema, source_table, source_primary_key, source_lsn, operation):
    import hashlib

    material = "\n".join(
        [
            source_connector,
            source_schema,
            source_table,
            source_primary_key,
            str(source_lsn),
            operation,
        ]
    )
    return "cdc_" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:40]


def raw_applicant_event_line(applicant_id, operation, source_lsn, ts_ms=None):
    before = {"id": applicant_id, "stage": "APPLIED"} if operation in {"u", "d"} else None
    after = None if operation == "d" else {"id": applicant_id, "stage": "SCREENING"}
    payload = {
        "before": before,
        "after": after,
        "source": {
            "name": "cdc.raw",
            "schema": "public",
            "table": "applicants",
            "lsn": source_lsn,
            "txId": 900 + source_lsn,
        },
        "op": operation,
    }
    if ts_ms is not None:
        payload["ts_ms"] = ts_ms
    return json.dumps({
        "payload": payload,
    })


class CdcSmokeFixtureTest(unittest.TestCase):

    def test_build_runtime_sla_proof_reports_source_metadata_and_slo_status(self):
        module = load_cdc_smoke_module()
        applicant_id = "applicant-runtime-sla"
        events = [
            module.parse_event(raw_applicant_event_line(applicant_id, "c", 88432240, ts_ms=1780987200000)),
            module.parse_event(raw_applicant_event_line(applicant_id, "u", 88432288, ts_ms=1780987201000)),
            module.parse_event(raw_applicant_event_line(applicant_id, "d", 88432320, ts_ms=1780987202000)),
        ]

        proof = module.build_runtime_sla_proof(
            "cdc-data-platform-postgres-source",
            applicant_id,
            events,
            {"offsets": [{"offset": {"lsn": 88432320}}]},
            observed_at_millis=1780987203000,
        )

        self.assertEqual("quality-sla-runtime", proof["scenario"])
        self.assertEqual("local-docker-debezium-runtime-sla", proof["evidenceLabel"])
        self.assertEqual("applicants", proof["sourceTable"])
        self.assertEqual(applicant_id, proof["sourcePrimaryKey"])
        self.assertEqual("cdc-data-platform-postgres-source", proof["connectorName"])
        self.assertEqual(["c", "u", "d"], proof["operations"])
        self.assertEqual([88432240, 88432288, 88432320], proof["sourceLsn"])
        self.assertEqual(88432240, proof["sourceLsnFrom"])
        self.assertEqual(88432320, proof["sourceLsnTo"])
        self.assertEqual(
            [
                {"lsn": 88432240, "txId": 88433140},
                {"lsn": 88432288, "txId": 88433188},
                {"lsn": 88432320, "txId": 88433220},
            ],
            proof["eventSourceOffset"],
        )
        self.assertEqual(
            [
                source_event_id("cdc.raw", "public", "applicants", applicant_id, 88432240, "c"),
                source_event_id("cdc.raw", "public", "applicants", applicant_id, 88432288, "u"),
                source_event_id("cdc.raw", "public", "applicants", applicant_id, 88432320, "d"),
            ],
            proof["eventId"],
        )
        self.assertEqual(3, proof["expectedEventCount"])
        self.assertEqual(3, proof["rawEventCount"])
        self.assertEqual(3, proof["observedRuntimeEventCount"])
        self.assertEqual(0, proof["duplicateEventCount"])
        self.assertEqual(0, proof["missingEventCount"])
        self.assertEqual(1.0, proof["completenessRatio"])
        self.assertEqual(0.0, proof["duplicateRatio"])
        self.assertEqual(1000, proof["freshnessLagMillis"])
        self.assertEqual(0, proof["sourceLsnLag"])
        self.assertEqual("PASSED", proof["slaStatus"])
        self.assertEqual([], proof["violatedSloIds"])
        self.assertEqual([], proof["warningSloIds"])

    def test_quality_sla_runtime_scenario_is_registered(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "cdc-smoke"),
                "--help",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertIn("quality-sla-runtime", completed.stdout)

    def test_quality_sla_runtime_writes_proof_artifact(self):
        module = load_cdc_smoke_module()
        applicant_id = "applicant-runtime-sla"
        events = [
            module.parse_event(raw_applicant_event_line(applicant_id, "c", 26710752, ts_ms=1780987200000)),
            module.parse_event(raw_applicant_event_line(applicant_id, "u", 26711320, ts_ms=1780987201000)),
            module.parse_event(raw_applicant_event_line(applicant_id, "d", 26711584, ts_ms=1780987202000)),
        ]

        replacements = {
            "prepare_runtime_connector": lambda: "cdc-data-platform-postgres-source",
            "mutate_applicant": lambda: applicant_id,
            "consume_applicant_events": lambda _applicant_id: events,
            "connector_offsets": lambda _connector_name: {"offsets": []},
            "current_time_millis": lambda: 1780987203580,
        }
        originals = {name: getattr(module, name) for name in replacements}
        with tempfile.TemporaryDirectory() as tmp_dir:
            proof_dir = Path(tmp_dir) / "proofs"
            for name, replacement in replacements.items():
                setattr(module, name, replacement)
            try:
                module.quality_sla_runtime(proof_dir=proof_dir)
            finally:
                for name, original in originals.items():
                    setattr(module, name, original)

            artifact_path = proof_dir / "quality-sla-runtime.json"
            self.assertTrue(artifact_path.exists())
            proof = json.loads(artifact_path.read_text())

        self.assertEqual("quality-sla-runtime", proof["scenario"])
        self.assertEqual("local-docker-debezium-runtime-sla", proof["evidenceLabel"])
        self.assertEqual([26710752, 26711320, 26711584], proof["sourceLsn"])
        self.assertEqual(26710752, proof["eventSourceOffset"][0]["lsn"])
        self.assertEqual("WARN", proof["slaStatus"])
        self.assertEqual(["source-lsn-lag-unavailable"], proof["warningSloIds"])
        self.assertEqual(proof["eventId"], proof["eventIds"])

    def test_schema_evolution_fixture_prints_source_metadata_and_event_ids(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "cdc-smoke"),
                "schema-evolution-fixture",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        proof = json.loads(completed.stdout)

        self.assertEqual("schema-evolution-fixture", proof["scenario"])
        self.assertEqual("fixture", proof["evidenceLabel"])
        self.assertEqual("applicants", proof["sourceTable"])
        self.assertEqual(["c", "u"], proof["operations"])
        self.assertEqual([88432240, 88432241], proof["sourceLsn"])
        self.assertEqual(
            [
                source_event_id("debezium-source", "public", "applicants", "applicant-001", 88432240, "c"),
                source_event_id("debezium-source", "public", "applicants", "applicant-001", 88432241, "u"),
            ],
            proof["eventId"],
        )
        self.assertEqual(["v1", "v2_added_screening_score"], proof["schemaVersions"])
        self.assertEqual({"file": "000000010000000000000058", "pos": 32240}, proof["sourceOffset"])
        self.assertEqual(
            [
                {"lsn": 88432240},
                {"lsn": 88432241},
            ],
            proof["eventSourceOffset"],
        )
        self.assertEqual(0, proof["runtimeEventCount"])

    def test_compose_command_prefix_includes_extra_compose_files_from_env(self):
        module = load_cdc_smoke_module()
        extra = ROOT / "infra" / "local" / "docker-compose.apache-kafka.yml"
        previous = os.environ.get("CDC_SMOKE_EXTRA_COMPOSE_FILES")
        os.environ["CDC_SMOKE_EXTRA_COMPOSE_FILES"] = str(extra)
        try:
            command = module.compose_command("config", "-q")
        finally:
            if previous is None:
                os.environ.pop("CDC_SMOKE_EXTRA_COMPOSE_FILES", None)
            else:
                os.environ["CDC_SMOKE_EXTRA_COMPOSE_FILES"] = previous

        self.assertEqual(
            [
                "docker",
                "compose",
                "--project-name",
                "cdc-data-platform",
                "-f",
                str(ROOT / "infra" / "local" / "docker-compose.yml"),
                "-f",
                str(extra),
                "config",
                "-q",
            ],
            command,
        )

    def test_kafka_tool_uses_configured_bin_dir(self):
        module = load_cdc_smoke_module()
        previous = os.environ.get("CDC_SMOKE_KAFKA_BIN_DIR")
        os.environ["CDC_SMOKE_KAFKA_BIN_DIR"] = "/opt/kafka/bin"
        try:
            command = module.kafka_tool("kafka-topics")
        finally:
            if previous is None:
                os.environ.pop("CDC_SMOKE_KAFKA_BIN_DIR", None)
            else:
                os.environ["CDC_SMOKE_KAFKA_BIN_DIR"] = previous

        self.assertEqual("/opt/kafka/bin/kafka-topics.sh", command)

    def test_runtime_connector_payload_defaults_to_applicant_only_scope(self):
        module = load_cdc_smoke_module()
        previous = os.environ.pop("CDC_SMOKE_TABLE_INCLUDE_LIST", None)
        try:
            payload = module.runtime_connector_payload()
        finally:
            if previous is not None:
                os.environ["CDC_SMOKE_TABLE_INCLUDE_LIST"] = previous

        self.assertEqual("public.applicants", payload["config"]["table.include.list"])

    def test_runtime_connector_payload_can_use_full_table_scope_from_env(self):
        module = load_cdc_smoke_module()
        full_scope = "public.job_postings,public.applicants,public.evaluations,public.agent_tasks"
        previous = os.environ.get("CDC_SMOKE_TABLE_INCLUDE_LIST")
        os.environ["CDC_SMOKE_TABLE_INCLUDE_LIST"] = full_scope
        try:
            payload = module.runtime_connector_payload()
        finally:
            if previous is None:
                os.environ.pop("CDC_SMOKE_TABLE_INCLUDE_LIST", None)
            else:
                os.environ["CDC_SMOKE_TABLE_INCLUDE_LIST"] = previous

        self.assertEqual(full_scope, payload["config"]["table.include.list"])

    def test_connector_offsets_records_unavailable_when_connect_is_down(self):
        module = load_cdc_smoke_module()

        def unavailable(_path, expected):
            raise urllib.error.URLError("connection refused")

        original = module.http_json
        module.http_json = unavailable
        try:
            offsets = module.connector_offsets("cdc-data-platform-postgres-source")
        finally:
            module.http_json = original

        self.assertIn("unavailable", offsets)
        self.assertIn("connection refused", offsets["unavailable"])

    def test_connector_offsets_records_unavailable_when_request_times_out(self):
        module = load_cdc_smoke_module()

        def timed_out(_path, expected):
            raise TimeoutError("timed out")

        original = module.http_json
        module.http_json = timed_out
        try:
            offsets = module.connector_offsets("cdc-data-platform-postgres-source")
        finally:
            module.http_json = original

        self.assertIn("unavailable", offsets)
        self.assertIn("timed out", offsets["unavailable"])

    def test_wait_for_debezium_streaming_slot_polls_until_slot_is_active(self):
        module = load_cdc_smoke_module()

        class Result:
            def __init__(self, stdout):
                self.returncode = 0
                self.stdout = stdout
                self.stderr = ""

        responses = [Result("f\n"), Result("t\n")]
        calls = []

        def fake_compose(*args, **kwargs):
            calls.append((args, kwargs))
            return responses.pop(0)

        ticks = iter([0, 0, 1, 1])
        original_compose = module.compose
        original_monotonic = module.time.monotonic
        original_sleep = module.time.sleep
        module.compose = fake_compose
        module.time.monotonic = lambda: next(ticks)
        module.time.sleep = lambda _seconds: None
        try:
            module.wait_for_debezium_streaming_slot()
        finally:
            module.compose = original_compose
            module.time.monotonic = original_monotonic
            module.time.sleep = original_sleep

        self.assertEqual(2, len(calls))
        self.assertIn("pg_replication_slots", " ".join(calls[0][0]))
        self.assertIn("cdc_data_platform_slot", " ".join(calls[0][0]))

    def test_prepare_runtime_connector_waits_for_streaming_slot_after_connector_running(self):
        module = load_cdc_smoke_module()
        calls = []

        replacements = {
            "require_docker_available": lambda: calls.append("docker"),
            "wait_for_postgres": lambda: calls.append("postgres"),
            "ensure_source_database": lambda: calls.append("database"),
            "apply_source_schema": lambda: calls.append("schema"),
            "wait_for_kafka": lambda: calls.append("kafka"),
            "reset_cdc_topics": lambda: calls.append("reset-cdc-topics"),
            "create_topics": lambda: calls.append("topics"),
            "reset_connect_internal_topics": lambda: calls.append("reset-connect-topics"),
            "create_connect_internal_topics": lambda: calls.append("connect-internal-topics"),
            "wait_for_connect": lambda: calls.append("connect-http"),
            "register_connector": lambda: calls.append("register") or "cdc-data-platform-postgres-source",
            "wait_for_connector_running": lambda _name: calls.append("connector-running"),
            "wait_for_debezium_streaming_slot": lambda: calls.append("slot-active"),
        }

        def fake_compose(*args, **_kwargs):
            calls.append("compose:" + " ".join(args))

            class Result:
                returncode = 0
                stdout = ""
                stderr = ""

            return Result()

        originals = {name: getattr(module, name, None) for name in replacements}
        original_compose = module.compose
        module.compose = fake_compose
        for name, replacement in replacements.items():
            setattr(module, name, replacement)
        try:
            connector_name = module.prepare_runtime_connector()
        finally:
            module.compose = original_compose
            for name, original in originals.items():
                if original is None:
                    delattr(module, name)
                else:
                    setattr(module, name, original)

        self.assertEqual("cdc-data-platform-postgres-source", connector_name)
        self.assertLess(calls.index("reset-cdc-topics"), calls.index("topics"))
        self.assertLess(calls.index("reset-connect-topics"), calls.index("connect-internal-topics"))
        self.assertLess(calls.index("connect-internal-topics"), calls.index("connect-http"))
        self.assertLess(calls.index("connector-running"), calls.index("slot-active"))

    def test_reset_cdc_topics_deletes_runtime_topics_only(self):
        module = load_cdc_smoke_module()
        calls = []

        def fake_compose(*args, **kwargs):
            calls.append((args, kwargs))

            class Result:
                returncode = 0
                stdout = ""
                stderr = ""

            return Result()

        original_compose = module.compose
        module.compose = fake_compose
        try:
            module.reset_cdc_topics()
        finally:
            module.compose = original_compose

        deleted_topics = [call[0][9] for call in calls]
        self.assertEqual(list(module.CDC_TOPICS), deleted_topics)
        for args, kwargs in calls:
            self.assertEqual("kafka", args[2])
            self.assertIn("--delete", args)
            self.assertIn("--if-exists", args)
            self.assertFalse(kwargs["check"])

    def test_create_topics_uses_single_partition_by_default_for_local_smoke(self):
        module = load_cdc_smoke_module()
        calls = []
        previous = os.environ.pop("CDC_SMOKE_TOPIC_PARTITIONS", None)

        def fake_compose(*args, **kwargs):
            calls.append((args, kwargs))

            class Result:
                returncode = 0
                stdout = ""
                stderr = ""

            return Result()

        original_compose = module.compose
        module.compose = fake_compose
        try:
            module.create_topics()
        finally:
            module.compose = original_compose
            if previous is not None:
                os.environ["CDC_SMOKE_TOPIC_PARTITIONS"] = previous

        self.assertEqual(len(module.CDC_TOPICS), len(calls))
        for args, _kwargs in calls:
            self.assertEqual("1", args[args.index("--partitions") + 1])

    def test_create_connect_internal_topics_precreates_single_partition_compacted_topics(self):
        module = load_cdc_smoke_module()
        calls = []

        def fake_compose(*args, **kwargs):
            calls.append((args, kwargs))

            class Result:
                returncode = 0
                stdout = ""
                stderr = ""

            return Result()

        original_compose = module.compose
        module.compose = fake_compose
        try:
            module.create_connect_internal_topics()
        finally:
            module.compose = original_compose

        created_topics = [call[0][call[0].index("--topic") + 1] for call in calls]
        self.assertEqual(list(module.CONNECT_INTERNAL_TOPICS), created_topics)
        for args, _kwargs in calls:
            self.assertEqual("1", args[args.index("--partitions") + 1])
            self.assertEqual("1", args[args.index("--replication-factor") + 1])
            self.assertIn("cleanup.policy=compact", args)

    def test_observe_connector_stability_reports_samples_and_service_status_on_timeout(self):
        module = load_cdc_smoke_module()
        summaries = [
            {"connectorState": "RUNNING", "taskStates": ["RUNNING"]},
            TimeoutError("timed out"),
        ]

        def fake_connector_status_summary(_name):
            value = summaries.pop(0)
            if isinstance(value, Exception):
                raise value
            return value

        def fake_service_status(service):
            return {"service": service, "state": "exited", "exitCode": 137}

        ticks = iter([0, 0, 1, 1, 2])
        original_connector_status_summary = module.connector_status_summary
        original_service_status = module.service_status
        original_monotonic = module.time.monotonic
        original_sleep = module.time.sleep
        module.connector_status_summary = fake_connector_status_summary
        module.service_status = fake_service_status
        module.time.monotonic = lambda: next(ticks)
        module.time.sleep = lambda _seconds: None
        try:
            with self.assertRaisesRegex(
                RuntimeError,
                "Kafka Connect stability status probe failed.*elapsedSeconds.*exitCode.*137",
            ):
                module.observe_connector_stability("cdc-data-platform-postgres-source", 20)
        finally:
            module.connector_status_summary = original_connector_status_summary
            module.service_status = original_service_status
            module.time.monotonic = original_monotonic
            module.time.sleep = original_sleep

    def test_container_inspect_extracts_exit_and_oom_metadata(self):
        module = load_cdc_smoke_module()
        calls = []

        class Result:
            returncode = 0
            stdout = json.dumps([
                {
                    "Name": "/cdc05-kafka-connect",
                    "RestartCount": 1,
                    "Config": {"Image": "local/connect-postgres:latest"},
                    "State": {
                        "Status": "exited",
                        "Running": False,
                        "ExitCode": 137,
                        "OOMKilled": True,
                        "Error": "",
                        "StartedAt": "2026-06-10T00:00:00Z",
                        "FinishedAt": "2026-06-10T00:01:00Z",
                    },
                }
            ])
            stderr = ""

        def fake_run(command, **kwargs):
            calls.append((command, kwargs))
            return Result()

        original_run = module.run
        module.run = fake_run
        try:
            details = module.container_inspect("cdc05-kafka-connect")
        finally:
            module.run = original_run

        self.assertEqual(["docker", "inspect", "cdc05-kafka-connect"], calls[0][0])
        self.assertEqual(10, calls[0][1]["timeout"])
        self.assertTrue(details["available"])
        self.assertEqual("cdc05-kafka-connect", details["name"])
        self.assertEqual("exited", details["state"]["status"])
        self.assertEqual(137, details["state"]["exitCode"])
        self.assertTrue(details["state"]["oomKilled"])
        self.assertEqual(1, details["restartCount"])
        self.assertEqual("local/connect-postgres:latest", details["image"])

    def test_service_failure_diagnostics_includes_status_inspect_and_recent_logs(self):
        module = load_cdc_smoke_module()
        calls = []

        def fake_service_status(service):
            calls.append(f"status:{service}")
            return {
                "service": service,
                "name": "cdc05-kafka-connect",
                "state": "exited",
                "exitCode": 137,
            }

        def fake_container_inspect(name):
            calls.append(f"inspect:{name}")
            return {
                "available": True,
                "name": name,
                "state": {
                    "status": "exited",
                    "exitCode": 137,
                    "oomKilled": True,
                },
            }

        def fake_service_log_tail(service):
            calls.append(f"logs:{service}")
            return {
                "available": True,
                "tailLineCount": 2,
                "tail": [
                    "Starting Kafka Connect",
                    "Killed",
                ],
            }

        def fake_docker_health_probe(timeout_seconds=None):
            calls.append(f"docker:{timeout_seconds}")
            return {"scenario": "docker-preflight", "status": "ok", "available": True}

        original_service_status = module.service_status
        original_container_inspect = module.container_inspect
        original_service_log_tail = module.service_log_tail
        original_docker_health_probe = module.docker_health_probe
        module.service_status = fake_service_status
        module.container_inspect = fake_container_inspect
        module.service_log_tail = fake_service_log_tail
        module.docker_health_probe = fake_docker_health_probe
        try:
            diagnostics = module.service_failure_diagnostics("kafka-connect")
        finally:
            module.service_status = original_service_status
            module.container_inspect = original_container_inspect
            module.service_log_tail = original_service_log_tail
            module.docker_health_probe = original_docker_health_probe

        self.assertEqual(
            [
                "status:kafka-connect",
                "inspect:cdc05-kafka-connect",
                "logs:kafka-connect",
                "docker:3",
            ],
            calls,
        )
        self.assertEqual(137, diagnostics["serviceStatus"]["exitCode"])
        self.assertTrue(diagnostics["containerInspect"]["state"]["oomKilled"])
        self.assertEqual(["Starting Kafka Connect", "Killed"], diagnostics["recentLogs"]["tail"])
        self.assertEqual("ok", diagnostics["dockerPreflight"]["status"])

    def test_observe_connector_stability_reports_container_diagnostics_when_task_stops(self):
        module = load_cdc_smoke_module()
        summaries = [
            {"connectorState": "RUNNING", "taskStates": ["RUNNING"]},
            {"connectorState": "RUNNING", "taskStates": ["FAILED"]},
        ]

        def fake_connector_status_summary(_name):
            return summaries.pop(0)

        def fake_service_failure_diagnostics(service):
            return {
                "serviceStatus": {
                    "service": service,
                    "name": "cdc05-kafka-connect",
                    "state": "exited",
                    "exitCode": 137,
                },
                "containerInspect": {
                    "available": True,
                    "state": {
                        "oomKilled": True,
                    },
                },
                "recentLogs": {
                    "tail": ["Killed"],
                },
            }

        ticks = iter([0, 0, 1, 1, 2])
        original_connector_status_summary = module.connector_status_summary
        original_service_failure_diagnostics = module.service_failure_diagnostics
        original_monotonic = module.time.monotonic
        original_sleep = module.time.sleep
        module.connector_status_summary = fake_connector_status_summary
        module.service_failure_diagnostics = fake_service_failure_diagnostics
        module.time.monotonic = lambda: next(ticks)
        module.time.sleep = lambda _seconds: None
        try:
            with self.assertRaisesRegex(
                RuntimeError,
                "Kafka Connect did not stay running.*elapsedSeconds.*oomKilled.*Killed",
            ):
                module.observe_connector_stability("cdc-data-platform-postgres-source", 20)
        finally:
            module.connector_status_summary = original_connector_status_summary
            module.service_failure_diagnostics = original_service_failure_diagnostics
            module.time.monotonic = original_monotonic
            module.time.sleep = original_sleep

    def test_connect_stability_failure_after_raw_capture_includes_cdc_metadata(self):
        module = load_cdc_smoke_module()
        event_id = module.source_event_id(
            "debezium-source",
            "public",
            "applicants",
            "applicant-001",
            88432240,
            "c",
        )
        events = [
            {
                "operation": "c",
                "sourceLsn": 88432240,
                "sourceOffset": {"lsn": 88432240, "txId": 772},
                "eventId": event_id,
            }
        ]

        def fake_prepare_runtime_connector():
            return "cdc-data-platform-postgres-source"

        def fake_mutate_applicant():
            return "applicant-001"

        def fake_consume_applicant_events(applicant_id):
            self.assertEqual("applicant-001", applicant_id)
            return events

        def fake_stability_duration_seconds():
            return 20

        def fake_observe_connector_stability(_name, _duration):
            raise RuntimeError('status probe failed; diagnostics={"serviceStatus":{"exitCode":137}}')

        original_prepare = module.prepare_runtime_connector
        original_mutate = module.mutate_applicant
        original_consume = module.consume_applicant_events
        original_duration = module.stability_duration_seconds
        original_observe = module.observe_connector_stability
        module.prepare_runtime_connector = fake_prepare_runtime_connector
        module.mutate_applicant = fake_mutate_applicant
        module.consume_applicant_events = fake_consume_applicant_events
        module.stability_duration_seconds = fake_stability_duration_seconds
        module.observe_connector_stability = fake_observe_connector_stability
        try:
            with self.assertRaisesRegex(
                RuntimeError,
                "connect-stability failed after raw CDC capture.*connectorTableIncludeList.*public.applicants.*sourceLsn.*88432240.*eventSourceOffset.*txId.*eventId.*cdc_.*exitCode.*137",
            ):
                module.connect_stability()
        finally:
            module.prepare_runtime_connector = original_prepare
            module.mutate_applicant = original_mutate
            module.consume_applicant_events = original_consume
            module.stability_duration_seconds = original_duration
            module.observe_connector_stability = original_observe

    def test_connect_stability_scenario_is_registered(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "cdc-smoke"),
                "--help",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertIn("connect-stability", completed.stdout)

    def test_schema_evolution_runtime_scenario_is_registered(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "cdc-smoke"),
                "--help",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertIn("schema-evolution-runtime", completed.stdout)

    def test_connector_restart_recovery_scenario_is_registered(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "cdc-smoke"),
                "--help",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertIn("connector-restart-recovery", completed.stdout)

    def test_kafka_outage_recovery_scenario_is_registered(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "cdc-smoke"),
                "--help",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertIn("kafka-outage-recovery", completed.stdout)

    def test_kafka_outage_recovery_proof_records_broker_outage_and_cdc_metadata(self):
        module = load_cdc_smoke_module()
        event_ids = [
            module.source_event_id("debezium-source", "public", "applicants", "applicant-001", 88432240, "c"),
            module.source_event_id("debezium-source", "public", "applicants", "applicant-001", 88432241, "u"),
            module.source_event_id("debezium-source", "public", "applicants", "applicant-001", 88432242, "d"),
        ]
        pre_outage_events = [
            {
                "operation": "c",
                "sourceLsn": 88432240,
                "sourceOffset": {"lsn": 88432240, "txId": 772},
                "eventId": event_ids[0],
            }
        ]
        all_events = [
            pre_outage_events[0],
            {
                "operation": "u",
                "sourceLsn": 88432241,
                "sourceOffset": {"lsn": 88432241, "txId": 773},
                "eventId": event_ids[1],
            },
            {
                "operation": "d",
                "sourceLsn": 88432242,
                "sourceOffset": {"lsn": 88432242, "txId": 774},
                "eventId": event_ids[2],
            },
        ]
        calls = []
        offsets = [
            {"offsets": [{"partition": {"server": "debezium-source"}, "offset": {"lsn": 88432240}}]},
            {"offsets": [{"partition": {"server": "debezium-source"}, "offset": {"lsn": 88432242}}]},
        ]
        kafka_statuses = [
            {"service": "kafka", "state": "running"},
            {"service": "kafka", "state": "exited"},
            {"service": "kafka", "state": "running"},
        ]

        def fake_prepare_runtime_connector():
            calls.append("prepare")
            return "cdc-data-platform-postgres-source"

        def fake_insert_applicant(*_args):
            calls.append("insert")
            return "applicant-001"

        def fake_update_applicant(applicant_id):
            calls.append(f"update:{applicant_id}")

        def fake_delete_applicant(applicant_id):
            calls.append(f"delete:{applicant_id}")

        def fake_consume_applicant_events_for_operations(applicant_id, operations):
            calls.append(f"consume:{applicant_id}:{'/'.join(sorted(operations))}")
            if set(operations) == {"c"}:
                return pre_outage_events
            return all_events

        def fake_connector_status_summary(name):
            calls.append(f"status:{name}")
            return {
                "connectorState": "RUNNING",
                "taskStates": ["RUNNING"],
            }

        def fake_connector_offsets(name):
            calls.append(f"offsets:{name}")
            return offsets.pop(0)

        def fake_service_status(service):
            calls.append(f"service-status:{service}")
            return kafka_statuses.pop(0)

        def fake_stop_kafka_for_outage():
            calls.append("stop-kafka")

        def fake_start_kafka_after_outage(name):
            calls.append(f"start-kafka:{name}")

        original_prepare = module.prepare_runtime_connector
        original_insert = module.insert_applicant
        original_update = module.update_applicant
        original_delete = module.delete_applicant
        original_consume_for_operations = module.consume_applicant_events_for_operations
        original_status = module.connector_status_summary
        original_offsets = module.connector_offsets
        original_service_status = module.service_status
        original_stop_kafka = module.stop_kafka_for_outage
        original_start_kafka = module.start_kafka_after_outage
        module.prepare_runtime_connector = fake_prepare_runtime_connector
        module.insert_applicant = fake_insert_applicant
        module.update_applicant = fake_update_applicant
        module.delete_applicant = fake_delete_applicant
        module.consume_applicant_events_for_operations = fake_consume_applicant_events_for_operations
        module.connector_status_summary = fake_connector_status_summary
        module.connector_offsets = fake_connector_offsets
        module.service_status = fake_service_status
        module.stop_kafka_for_outage = fake_stop_kafka_for_outage
        module.start_kafka_after_outage = fake_start_kafka_after_outage
        try:
            import io
            from contextlib import redirect_stdout

            output = io.StringIO()
            with redirect_stdout(output):
                module.kafka_outage_recovery()
        finally:
            module.prepare_runtime_connector = original_prepare
            module.insert_applicant = original_insert
            module.update_applicant = original_update
            module.delete_applicant = original_delete
            module.consume_applicant_events_for_operations = original_consume_for_operations
            module.connector_status_summary = original_status
            module.connector_offsets = original_offsets
            module.service_status = original_service_status
            module.stop_kafka_for_outage = original_stop_kafka
            module.start_kafka_after_outage = original_start_kafka

        proof = json.loads(output.getvalue())

        self.assertEqual(
            [
                "prepare",
                "insert",
                "consume:applicant-001:c",
                "status:cdc-data-platform-postgres-source",
                "offsets:cdc-data-platform-postgres-source",
                "service-status:kafka",
                "stop-kafka",
                "service-status:kafka",
                "update:applicant-001",
                "delete:applicant-001",
                "start-kafka:cdc-data-platform-postgres-source",
                "service-status:kafka",
                "status:cdc-data-platform-postgres-source",
                "consume:applicant-001:c/d/u",
                "offsets:cdc-data-platform-postgres-source",
            ],
            calls,
        )
        self.assertEqual("kafka-outage-recovery", proof["scenario"])
        self.assertEqual("local-docker-debezium-runtime", proof["evidenceLabel"])
        self.assertEqual("kafka-stop-start", proof["recoveryAction"])
        self.assertEqual(["c"], proof["operationsBeforeOutage"])
        self.assertEqual(["u", "d"], proof["sourceMutationsDuringOutage"])
        self.assertEqual(["u", "d"], proof["operationsAfterRecovery"])
        self.assertEqual(["c", "u", "d"], proof["operations"])
        self.assertEqual([88432240, 88432241, 88432242], proof["sourceLsn"])
        self.assertEqual(event_ids, proof["eventId"])
        self.assertEqual(
            [
                {"lsn": 88432240, "txId": 772},
                {"lsn": 88432241, "txId": 773},
                {"lsn": 88432242, "txId": 774},
            ],
            proof["eventSourceOffset"],
        )
        self.assertEqual({"service": "kafka", "state": "running"}, proof["kafkaStatusBeforeOutage"])
        self.assertEqual({"service": "kafka", "state": "exited"}, proof["kafkaStatusDuringOutage"])
        self.assertEqual({"service": "kafka", "state": "running"}, proof["kafkaStatusAfterRecovery"])
        self.assertTrue(proof["durableOffsetResumed"])
        self.assertEqual(0, proof["missingEventCount"])
        self.assertEqual(0, proof["duplicateEventCount"])

    def test_connector_restart_recovery_proof_records_restart_and_cdc_metadata(self):
        module = load_cdc_smoke_module()
        event_ids = [
            module.source_event_id("debezium-source", "public", "applicants", "applicant-001", 88432240, "c"),
            module.source_event_id("debezium-source", "public", "applicants", "applicant-001", 88432241, "u"),
            module.source_event_id("debezium-source", "public", "applicants", "applicant-001", 88432242, "d"),
        ]
        pre_restart_events = [
            {
                "operation": "c",
                "sourceLsn": 88432240,
                "sourceOffset": {"lsn": 88432240, "txId": 772},
                "eventId": event_ids[0],
            }
        ]
        all_events = [
            pre_restart_events[0],
            {
                "operation": "u",
                "sourceLsn": 88432241,
                "sourceOffset": {"lsn": 88432241, "txId": 773},
                "eventId": event_ids[1],
            },
            {
                "operation": "d",
                "sourceLsn": 88432242,
                "sourceOffset": {"lsn": 88432242, "txId": 774},
                "eventId": event_ids[2],
            },
        ]
        calls = []
        offsets = [
            {"offsets": [{"partition": {"server": "debezium-source"}, "offset": {"lsn": 88432240}}]},
            {"offsets": [{"partition": {"server": "debezium-source"}, "offset": {"lsn": 88432242}}]},
        ]

        def fake_prepare_runtime_connector():
            calls.append("prepare")
            return "cdc-data-platform-postgres-source"

        def fake_insert_applicant():
            calls.append("insert")
            return "applicant-001"

        def fake_update_applicant(applicant_id):
            calls.append(f"update:{applicant_id}")

        def fake_delete_applicant(applicant_id):
            calls.append(f"delete:{applicant_id}")

        def fake_consume_applicant_events_for_operations(applicant_id, operations):
            calls.append(f"consume:{applicant_id}:{'/'.join(sorted(operations))}")
            if set(operations) == {"c"}:
                return pre_restart_events
            return all_events

        def fake_connector_status_summary(name):
            calls.append(f"status:{name}")
            return {
                "connectorState": "RUNNING",
                "taskStates": ["RUNNING"],
            }

        def fake_restart_kafka_connect(name):
            calls.append(f"restart:{name}")

        def fake_connector_offsets(name):
            calls.append(f"offsets:{name}")
            return offsets.pop(0)

        original_prepare = module.prepare_runtime_connector
        original_insert = module.insert_applicant
        original_update = module.update_applicant
        original_delete = module.delete_applicant
        original_consume_for_operations = module.consume_applicant_events_for_operations
        original_status = module.connector_status_summary
        original_restart = module.restart_kafka_connect
        original_offsets = module.connector_offsets
        module.prepare_runtime_connector = fake_prepare_runtime_connector
        module.insert_applicant = fake_insert_applicant
        module.update_applicant = fake_update_applicant
        module.delete_applicant = fake_delete_applicant
        module.consume_applicant_events_for_operations = fake_consume_applicant_events_for_operations
        module.connector_status_summary = fake_connector_status_summary
        module.restart_kafka_connect = fake_restart_kafka_connect
        module.connector_offsets = fake_connector_offsets
        try:
            import io
            from contextlib import redirect_stdout

            output = io.StringIO()
            with redirect_stdout(output):
                module.connector_restart_recovery()
        finally:
            module.prepare_runtime_connector = original_prepare
            module.insert_applicant = original_insert
            module.update_applicant = original_update
            module.delete_applicant = original_delete
            module.consume_applicant_events_for_operations = original_consume_for_operations
            module.connector_status_summary = original_status
            module.restart_kafka_connect = original_restart
            module.connector_offsets = original_offsets

        proof = json.loads(output.getvalue())

        self.assertEqual(
            [
                "prepare",
                "insert",
                "consume:applicant-001:c",
                "status:cdc-data-platform-postgres-source",
                "offsets:cdc-data-platform-postgres-source",
                "restart:cdc-data-platform-postgres-source",
                "status:cdc-data-platform-postgres-source",
                "update:applicant-001",
                "delete:applicant-001",
                "consume:applicant-001:c/d/u",
                "offsets:cdc-data-platform-postgres-source",
            ],
            calls,
        )
        self.assertEqual("connector-restart-recovery", proof["scenario"])
        self.assertEqual("local-docker-debezium-runtime", proof["evidenceLabel"])
        self.assertEqual("kafka-connect-force-recreate", proof["recoveryAction"])
        self.assertEqual({"connectorState": "RUNNING", "taskStates": ["RUNNING"]}, proof["connectorStatusBeforeRestart"])
        self.assertEqual({"connectorState": "RUNNING", "taskStates": ["RUNNING"]}, proof["connectorStatusAfterRestart"])
        self.assertEqual(["c"], proof["operationsBeforeRestart"])
        self.assertEqual(["u", "d"], proof["operationsAfterRestart"])
        self.assertEqual(["c", "u", "d"], proof["operations"])
        self.assertEqual([88432240, 88432241, 88432242], proof["sourceLsn"])
        self.assertEqual(
            [
                {"lsn": 88432240, "txId": 772},
                {"lsn": 88432241, "txId": 773},
                {"lsn": 88432242, "txId": 774},
            ],
            proof["eventSourceOffset"],
        )
        self.assertEqual(event_ids, proof["eventId"])
        self.assertEqual(
            {"offsets": [{"partition": {"server": "debezium-source"}, "offset": {"lsn": 88432240}}]},
            proof["sourceOffsetBeforeRestart"],
        )
        self.assertEqual(
            {"offsets": [{"partition": {"server": "debezium-source"}, "offset": {"lsn": 88432242}}]},
            proof["sourceOffsetAfterRestart"],
        )
        self.assertTrue(proof["durableOffsetResumed"])
        self.assertEqual(0, proof["missingEventCount"])
        self.assertEqual(0, proof["duplicateEventCount"])

    def test_parse_event_preserves_schema_added_after_fields(self):
        module = load_cdc_smoke_module()

        event = module.parse_event(json.dumps({
            "payload": {
                "before": {
                    "id": "applicant-001",
                    "stage": "APPLIED",
                },
                "after": {
                    "id": "applicant-001",
                    "stage": "SCREENING",
                    "screening_score": 87,
                },
                "source": {
                    "name": "debezium-source",
                    "schema": "public",
                    "table": "applicants",
                    "lsn": 88432241,
                    "sequence": ["26740416", "26740416"],
                    "txId": 773,
                },
                "op": "u",
                "ts_ms": 1780987205000,
            }
        }))

        self.assertEqual(87, event["after"]["screening_score"])
        self.assertEqual("SCREENING", event["after"]["stage"])
        self.assertEqual(88432241, event["sourceLsn"])
        self.assertEqual(773, event["sourceOffset"]["txId"])

    def test_parse_event_ignores_debezium_null_payload_tombstone(self):
        module = load_cdc_smoke_module()

        event = module.parse_event(json.dumps({"payload": None}))

        self.assertIsNone(event)

    def test_docker_preflight_scenario_is_registered(self):
        completed = subprocess.run(
            [
                str(ROOT / "tools" / "runner" / "cdc-smoke"),
                "--help",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertIn("docker-preflight", completed.stdout)

    def test_docker_probe_timeout_seconds_can_be_overridden(self):
        module = load_cdc_smoke_module()
        previous = os.environ.get("CDC_SMOKE_DOCKER_TIMEOUT_SECONDS")
        os.environ["CDC_SMOKE_DOCKER_TIMEOUT_SECONDS"] = "3"
        try:
            timeout_seconds = module.docker_probe_timeout_seconds()
        finally:
            if previous is None:
                os.environ.pop("CDC_SMOKE_DOCKER_TIMEOUT_SECONDS", None)
            else:
                os.environ["CDC_SMOKE_DOCKER_TIMEOUT_SECONDS"] = previous

        self.assertEqual(3, timeout_seconds)

    def test_docker_health_probe_records_timeout_without_hanging(self):
        module = load_cdc_smoke_module()

        def timeout_run(command, **kwargs):
            raise subprocess.TimeoutExpired(command, timeout=2)

        original = module.run
        module.run = timeout_run
        try:
            health = module.docker_health_probe(timeout_seconds=2)
        finally:
            module.run = original

        self.assertFalse(health["available"])
        self.assertEqual("timeout", health["status"])
        self.assertIn("timed out after 2 seconds", health["reason"])

    def test_stability_duration_seconds_can_be_overridden(self):
        module = load_cdc_smoke_module()
        previous = os.environ.get("CDC_SMOKE_STABILITY_SECONDS")
        os.environ["CDC_SMOKE_STABILITY_SECONDS"] = "7"
        try:
            duration = module.stability_duration_seconds()
        finally:
            if previous is None:
                os.environ.pop("CDC_SMOKE_STABILITY_SECONDS", None)
            else:
                os.environ["CDC_SMOKE_STABILITY_SECONDS"] = previous

        self.assertEqual(7, duration)

    def test_consumer_max_messages_defaults_above_local_topic_backlog(self):
        module = load_cdc_smoke_module()
        previous = os.environ.get("CDC_SMOKE_CONSUMER_MAX_MESSAGES")
        os.environ.pop("CDC_SMOKE_CONSUMER_MAX_MESSAGES", None)
        try:
            max_messages = module.consumer_max_messages()
        finally:
            if previous is not None:
                os.environ["CDC_SMOKE_CONSUMER_MAX_MESSAGES"] = previous

        self.assertGreaterEqual(int(max_messages), 500)

    def test_consumer_max_messages_can_be_overridden(self):
        module = load_cdc_smoke_module()
        previous = os.environ.get("CDC_SMOKE_CONSUMER_MAX_MESSAGES")
        os.environ["CDC_SMOKE_CONSUMER_MAX_MESSAGES"] = "120"
        try:
            max_messages = module.consumer_max_messages()
        finally:
            if previous is None:
                os.environ.pop("CDC_SMOKE_CONSUMER_MAX_MESSAGES", None)
            else:
                os.environ["CDC_SMOKE_CONSUMER_MAX_MESSAGES"] = previous

        self.assertEqual("120", max_messages)

    def test_consume_applicant_events_fails_fast_when_kafka_consumer_command_fails(self):
        module = load_cdc_smoke_module()

        class EmptyStream:
            def __init__(self, text=""):
                self.text = text
                self.closed = False

            def readline(self):
                return ""

            def read(self):
                return self.text

            def close(self):
                self.closed = True

        class FailedConsumer:
            def __init__(self):
                self.returncode = 1
                self.stdout = EmptyStream()
                self.stderr = EmptyStream('service "kafka" is not running')

            def poll(self):
                return self.returncode

            def wait(self, timeout=None):
                return self.returncode

        failed_consumer = FailedConsumer()

        def failed_popen(*_args, **_kwargs):
            return failed_consumer

        ticks = iter([0, 0, 100])
        original_popen = module.subprocess.Popen
        original_monotonic = module.time.monotonic
        original_sleep = module.time.sleep
        module.subprocess.Popen = failed_popen
        module.time.monotonic = lambda: next(ticks)
        module.time.sleep = lambda _seconds: None
        try:
            with self.assertRaisesRegex(RuntimeError, 'service "kafka" is not running'):
                module.consume_applicant_events_for_operations("applicant-001", {"c"})
        finally:
            module.subprocess.Popen = original_popen
            module.time.monotonic = original_monotonic
            module.time.sleep = original_sleep

        self.assertTrue(failed_consumer.stdout.closed)
        self.assertTrue(failed_consumer.stderr.closed)

    def test_consume_applicant_events_streams_and_stops_after_target_operations(self):
        module = load_cdc_smoke_module()
        applicant_id = "applicant-stream-target"
        lines = [
            raw_applicant_event_line("other-applicant", "c", 88432230) + "\n",
            raw_applicant_event_line(applicant_id, "c", 88432240) + "\n",
            raw_applicant_event_line(applicant_id, "u", 88432241) + "\n",
            raw_applicant_event_line(applicant_id, "d", 88432242) + "\n",
            raw_applicant_event_line(applicant_id, "u", 88432243) + "\n",
        ]

        class FakeStdout:
            def __init__(self, values):
                self.values = values
                self.index = 0

            def readline(self):
                if self.index >= len(self.values):
                    return ""
                value = self.values[self.index]
                self.index += 1
                return value

        class FakeProcess:
            def __init__(self):
                self.stdout = FakeStdout(lines)
                self.stderr = FakeStdout([])
                self.terminated = False
                self.killed = False
                self.waited = False
                self.returncode = None

            def poll(self):
                return self.returncode

            def terminate(self):
                self.terminated = True
                self.returncode = 0

            def kill(self):
                self.killed = True
                self.returncode = -9

            def wait(self, timeout=None):
                self.waited = True
                return self.returncode

        process = FakeProcess()
        popen_calls = []

        def fake_popen(command, **kwargs):
            popen_calls.append((command, kwargs))
            return process

        def forbidden_compose(*_args, **_kwargs):
            raise AssertionError("batch compose consumer should not be used for applicant event consumption")

        original_popen = module.subprocess.Popen
        original_compose = module.compose
        module.subprocess.Popen = fake_popen
        module.compose = forbidden_compose
        try:
            events = module.consume_applicant_events_for_operations(applicant_id, {"c", "u", "d"})
        finally:
            module.subprocess.Popen = original_popen
            module.compose = original_compose

        self.assertEqual(["c", "u", "d"], [event["operation"] for event in events])
        self.assertEqual([88432240, 88432241, 88432242], [event["sourceLsn"] for event in events])
        self.assertEqual(4, process.stdout.index)
        self.assertTrue(process.terminated)
        self.assertTrue(process.waited)
        self.assertFalse(process.killed)
        self.assertEqual(1, len(popen_calls))
        self.assertIn("kafka-console-consumer", " ".join(popen_calls[0][0]))

    def test_consume_applicant_events_timeout_reports_observed_applicant_candidates(self):
        module = load_cdc_smoke_module()
        target_id = "missing-target-applicant"
        lines = [
            raw_applicant_event_line("other-applicant", "c", 88432230) + "\n",
            raw_applicant_event_line("other-applicant", "u", 88432231) + "\n",
        ]

        class FakeStdout:
            def __init__(self, values):
                self.values = values
                self.index = 0

            def readline(self):
                if self.index >= len(self.values):
                    return ""
                value = self.values[self.index]
                self.index += 1
                return value

            def close(self):
                return

        class FakeProcess:
            def __init__(self):
                self.stdout = FakeStdout(lines)
                self.stderr = FakeStdout([])
                self.returncode = 0

            def poll(self):
                return self.returncode

            def wait(self, timeout=None):
                return self.returncode

        def fake_popen(*_args, **_kwargs):
            return FakeProcess()

        ticks = iter([0, 0, 91])
        original_popen = module.subprocess.Popen
        original_monotonic = module.time.monotonic
        original_sleep = module.time.sleep
        module.subprocess.Popen = fake_popen
        module.time.monotonic = lambda: next(ticks)
        module.time.sleep = lambda _seconds: None
        try:
            with self.assertRaisesRegex(
                TimeoutError,
                "observedApplicantEvents=.*other-applicant.*88432231",
            ):
                module.consume_applicant_events_for_operations(target_id, {"c"})
        finally:
            module.subprocess.Popen = original_popen
            module.time.monotonic = original_monotonic
            module.time.sleep = original_sleep

    def test_connector_config_uses_streaming_only_snapshot_for_runtime_smoke(self):
        connector = json.loads(CONNECTOR_FILE.read_text())
        config = connector["config"]

        self.assertEqual("no_data", config["snapshot.mode"])
        self.assertEqual("1048576", config["producer.buffer.memory"])
        self.assertEqual("4096", config["producer.batch.size"])
        self.assertEqual(
            "public.job_postings,public.applicants,public.evaluations,public.agent_tasks",
            config["table.include.list"],
        )

    def test_apache_kafka_override_keeps_project05_runtime_low_memory(self):
        override = (ROOT / "infra" / "local" / "docker-compose.apache-kafka.yml").read_text()

        self.assertIn("image: postgres:16-alpine", override)
        self.assertIn("shared_buffers=16MB", override)
        self.assertIn("max_connections=20", override)
        self.assertIn("KAFKA_HEAP_OPTS: -Xms64m -Xmx128m", override)
        self.assertIn("KAFKA_HEAP_OPTS: -Xms64m -Xmx160m", override)
        self.assertIn("KAFKA_JVM_PERFORMANCE_OPTS", override)
        self.assertIn("-XX:+UseSerialGC", override)
        self.assertIn("-XX:MaxDirectMemorySize=32m", override)
        self.assertIn("-XX:ReservedCodeCacheSize=32m", override)

    def test_connect_stability_proof_records_connector_table_scope(self):
        module = load_cdc_smoke_module()
        event_id = module.source_event_id(
            "debezium-source",
            "public",
            "applicants",
            "applicant-001",
            88432240,
            "c",
        )
        events = [
            {
                "operation": "c",
                "sourceLsn": 88432240,
                "sourceOffset": {"lsn": 88432240, "txId": 772},
                "eventId": event_id,
            }
        ]

        replacements = {
            "prepare_runtime_connector": lambda: "cdc-data-platform-postgres-source",
            "mutate_applicant": lambda: "applicant-001",
            "consume_applicant_events": lambda _applicant_id: events,
            "stability_duration_seconds": lambda: 20,
            "observe_connector_stability": lambda _name, _duration: [
                {
                    "elapsedSeconds": 20,
                    "connectorState": "RUNNING",
                    "taskStates": ["RUNNING"],
                    "running": True,
                }
            ],
            "connector_offsets": lambda _name: {"offsets": []},
            "runtime_connector_table_include_list": lambda: "public.applicants",
        }
        originals = {name: getattr(module, name) for name in replacements}
        for name, replacement in replacements.items():
            setattr(module, name, replacement)
        try:
            import io
            from contextlib import redirect_stdout

            output = io.StringIO()
            with redirect_stdout(output):
                module.connect_stability()
        finally:
            for name, original in originals.items():
                setattr(module, name, original)

        proof = json.loads(output.getvalue())
        self.assertEqual("public.applicants", proof["connectorTableIncludeList"])


if __name__ == "__main__":
    unittest.main()
