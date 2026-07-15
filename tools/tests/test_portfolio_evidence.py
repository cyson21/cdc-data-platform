import json
import tempfile
import unittest
from pathlib import Path

from tools.portfolio_evidence import build_report, source_timestamp, write_report


PASSING_XML = """<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="zeta.Suite" tests="3" failures="0" errors="0" skipped="1" time="1.25">
  <testcase name="first" />
  <testcase name="second" />
  <testcase name="third"><skipped /></testcase>
</testsuite>
"""

FAILING_XML = """<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="alpha.Suite" tests="2" failures="1" errors="0" skipped="0" time="0.5">
  <testcase name="first"><failure /></testcase>
  <testcase name="second" />
</testsuite>
"""


class PortfolioEvidenceTest(unittest.TestCase):
    def _report_dir(self, files: dict[str, str]) -> tuple[tempfile.TemporaryDirectory, Path]:
        temporary = tempfile.TemporaryDirectory()
        report_dir = Path(temporary.name)
        for name, content in files.items():
            (report_dir / name).write_text(content, encoding="utf-8")
        return temporary, report_dir

    def test_report_aggregates_counts_and_sorts_suites(self) -> None:
        temporary, report_dir = self._report_dir(
            {"TEST-zeta.xml": PASSING_XML, "TEST-alpha.xml": FAILING_XML}
        )
        self.addCleanup(temporary.cleanup)

        report = build_report(
            report_dir,
            {"SOURCE_DATE_EPOCH": "0", "EVIDENCE_COMMIT": "abc123"},
        )

        self.assertEqual(1, report["schema_version"])
        self.assertEqual(["alpha.Suite", "zeta.Suite"], [suite["name"] for suite in report["suites"]])
        self.assertEqual(
            {"tests": 5, "passed": 3, "failures": 1, "errors": 0, "skipped": 1},
            report["totals"],
        )
        self.assertFalse(report["verification"]["passed"])
        self.assertEqual("local-no-docker", report["verification"]["scope"])
        self.assertEqual("abc123", report["source"]["commit"])
        self.assertEqual("1970-01-01T00:00:00Z", report["source"]["timestamp"])

    def test_same_inputs_produce_byte_identical_json(self) -> None:
        temporary, report_dir = self._report_dir({"TEST-zeta.xml": PASSING_XML})
        self.addCleanup(temporary.cleanup)
        report = build_report(report_dir, {"SOURCE_DATE_EPOCH": "123", "EVIDENCE_COMMIT": "same"})
        first = report_dir / "first.json"
        second = report_dir / "second.json"

        write_report(report, first)
        write_report(report, second)

        self.assertEqual(first.read_bytes(), second.read_bytes())
        self.assertEqual(report, json.loads(first.read_text(encoding="utf-8")))

    def test_missing_reports_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with self.assertRaisesRegex(FileNotFoundError, "no Surefire"):
                build_report(Path(tmp_dir), {})

    def test_inconsistent_junit_counts_fail_closed(self) -> None:
        temporary, report_dir = self._report_dir(
            {
                "TEST-bad.xml": (
                    '<testsuite name="bad" tests="1" failures="1" errors="1" skipped="0">'
                    '<testcase name="bad" /></testsuite>'
                )
            }
        )
        self.addCleanup(temporary.cleanup)

        with self.assertRaisesRegex(ValueError, "counts exceed total tests"):
            build_report(report_dir, {})

    def test_invalid_or_negative_source_date_epoch_is_rejected(self) -> None:
        for value in ("invalid", "-1"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "non-negative integer"):
                    source_timestamp({"SOURCE_DATE_EPOCH": value})

    def test_absent_source_date_epoch_is_explicitly_null(self) -> None:
        self.assertIsNone(source_timestamp({}))

    def test_out_of_range_source_date_epoch_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "outside the supported"):
            source_timestamp({"SOURCE_DATE_EPOCH": "9" * 200})


if __name__ == "__main__":
    unittest.main()
