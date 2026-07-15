#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


SCHEMA_VERSION = 1
PROJECT = "cdc-data-platform"
VERIFICATION_SCOPE = "local-no-docker"
PROVEN_BOUNDARIES = [
    "canonical envelope normalization and duplicate event-id suppression",
    "Spring MVC request/response contract for canonical ingest",
    "pipeline quality rule evaluation with in-memory test doubles",
    "deterministic local JSONL object key and serialization",
]
NOT_PROVEN_BOUNDARIES = [
    "live PostgreSQL-to-Debezium-to-Kafka change capture",
    "continuous Kafka handoff from the CDC runtime to the control plane",
    "automatic handoff from the control plane to the lakehouse sink",
    "S3, Athena, dbt-athena, or deployed cloud execution",
]


def _non_negative_int(value: str | None, *, field: str) -> int:
    try:
        parsed = int(value or 0)
    except ValueError as exc:
        raise ValueError(f"invalid JUnit {field}: {value!r}") from exc
    if parsed < 0:
        raise ValueError(f"JUnit {field} must be non-negative: {parsed}")
    return parsed


def _leaf_suites(root: ET.Element) -> Iterable[ET.Element]:
    suites = [root] if root.tag == "testsuite" else list(root.iter("testsuite"))
    return (suite for suite in suites if suite.findall("testcase"))


def read_suites(report_dir: Path) -> list[dict[str, object]]:
    report_files = sorted(report_dir.glob("TEST-*.xml"), key=lambda path: path.name)
    if not report_files:
        raise FileNotFoundError(f"no Surefire TEST-*.xml reports found in {report_dir}")

    suites: list[dict[str, object]] = []
    for report_file in report_files:
        root = ET.parse(report_file).getroot()
        for suite in _leaf_suites(root):
            tests = _non_negative_int(suite.get("tests"), field="tests")
            failures = _non_negative_int(suite.get("failures"), field="failures")
            errors = _non_negative_int(suite.get("errors"), field="errors")
            skipped = _non_negative_int(suite.get("skipped"), field="skipped")
            non_passing = failures + errors + skipped
            if non_passing > tests:
                raise ValueError("JUnit failure, error, and skipped counts exceed total tests")
            suites.append(
                {
                    "name": suite.get("name") or report_file.stem.removeprefix("TEST-"),
                    "tests": tests,
                    "passed": tests - non_passing,
                    "failures": failures,
                    "errors": errors,
                    "skipped": skipped,
                    "report_file": report_file.name,
                }
            )
    return sorted(suites, key=lambda suite: (str(suite["name"]), str(suite["report_file"])))


def source_timestamp(environ: dict[str, str]) -> str | None:
    raw_epoch = environ.get("SOURCE_DATE_EPOCH")
    if raw_epoch is None:
        return None
    try:
        epoch = int(raw_epoch)
    except ValueError as exc:
        raise ValueError("SOURCE_DATE_EPOCH must be a non-negative integer") from exc
    if epoch < 0:
        raise ValueError("SOURCE_DATE_EPOCH must be a non-negative integer")
    try:
        return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except (OverflowError, OSError, ValueError) as exc:
        raise ValueError("SOURCE_DATE_EPOCH is outside the supported timestamp range") from exc


def build_report(report_dir: Path, environ: dict[str, str] | None = None) -> dict[str, object]:
    environ = dict(os.environ if environ is None else environ)
    suites = read_suites(report_dir)
    totals = {
        key: sum(int(suite[key]) for suite in suites)
        for key in ("tests", "passed", "failures", "errors", "skipped")
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "project": PROJECT,
        "source": {
            "commit": environ.get("EVIDENCE_COMMIT") or environ.get("GITHUB_SHA"),
            "timestamp": source_timestamp(environ),
        },
        "runtime": {"java_release": 21},
        "verification": {
            "scope": VERIFICATION_SCOPE,
            "passed": totals["failures"] == 0 and totals["errors"] == 0,
            "proven": PROVEN_BOUNDARIES,
            "not_proven": NOT_PROVEN_BOUNDARIES,
        },
        "totals": totals,
        "suites": suites,
    }


def write_report(report: dict[str, object], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=output.parent,
        prefix=f".{output.name}.",
        delete=False,
    ) as temporary:
        temporary.write(payload)
        temporary_path = Path(temporary.name)
    temporary_path.replace(output)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate deterministic CDC portfolio test evidence.")
    parser.add_argument("--reports", type=Path, required=True, help="Surefire report directory")
    parser.add_argument("--output", type=Path, required=True, help="JSON output path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_report(args.reports)
    write_report(report, args.output)
    return 0 if report["verification"]["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
