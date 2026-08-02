#!/usr/bin/env python3
"""CI regression scope lists for CDC Data Platform.

These lists are the source of truth for which Surefire classes run in the
non-cloud job versus the Testcontainers job. Keep them disjoint and complete
for every ``*Test.java`` under ``backend/src/test``.
"""

from __future__ import annotations

# Pure unit, MockMvc, in-memory doubles, local filesystem sink, and Spring
# context load with datasource autoconfig excluded. No Docker daemon required.
NON_CLOUD_MAVEN_TESTS: tuple[str, ...] = (
    "CdcDataPlatformApplicationTest",
    "CanonicalIngestControllerTest",
    "CanonicalIngestServiceTest",
    "CdcEventIdTest",
    "DebeziumEnvelopeTest",
    "ConnectorHealthControllerTest",
    "ConnectorHealthHttpControllerTest",
    "ConnectorHealthServiceTest",
    "LocalLakehouseObjectSinkTest",
    "PipelineQualityControllerTest",
    "PipelineQualityServiceTest",
    "ReplayRequestControllerTest",
    "RetryEventControllerTest",
)

# PostgreSQL Testcontainers (and Embedded Kafka where paired). Requires Docker.
TESTCONTAINERS_MAVEN_TESTS: tuple[str, ...] = (
    "ConnectorHealthRepositoryTest",
    "CdcEventLedgerRepositoryTest",
    "PipelineQualityCheckRepositoryTest",
    "PipelineQualitySlaRepositoryTest",
    "ReplayRequestServiceTest",
    "KafkaRetryEventPublisherTest",
    "RecoveryRunServiceTest",
    "SinkFailureReplayFlowTest",
    "RetryEventServiceTest",
    "SchemaMigrationTest",
)

# Python unittest modules that run on a clean checkout without a live Docker
# daemon, AWS, Athena, or dbt. Fixture suites may stub docker on PATH but must
# not start compose stacks.
#
# Excluded from CI: tools.tests.test_project_audit (needs gitignored TODO.md /
# local proof artifacts) and tools.tests.test_control_plane_smoke (re-wraps
# Maven and mixes unit + Testcontainers scenarios).
NON_CLOUD_PYTHON_TESTS: tuple[str, ...] = (
    "tools.tests.test_portfolio_evidence",
    "tools.tests.test_cdc_smoke_fixture",
    "tools.tests.test_lakehouse_smoke",
    "tools.tests.test_ci_coverage_scopes",
)

# control-plane-smoke wraps Maven; DB-backed scenarios belong with Testcontainers.
# Full local compose (cdc-smoke applicant-change-capture) stays out of CI.
OPT_IN_ONLY_MARKERS: tuple[str, ...] = (
    "CDC_LAKEHOUSE_ATHENA_OPT_IN",
    "CDC_LAKEHOUSE_DBT_ATHENA_OPT_IN",
    "athena-mart-execution",
    "dbt-athena-run",
)


def maven_test_selector(tests: tuple[str, ...]) -> str:
    return ",".join(tests)
