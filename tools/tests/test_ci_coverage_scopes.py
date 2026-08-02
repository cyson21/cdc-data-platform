#!/usr/bin/env python3
import re
import unittest
from pathlib import Path

from tools.ci_coverage_scopes import (
    NON_CLOUD_MAVEN_TESTS,
    NON_CLOUD_PYTHON_TESTS,
    OPT_IN_ONLY_MARKERS,
    TESTCONTAINERS_MAVEN_TESTS,
    maven_test_selector,
)


ROOT = Path(__file__).resolve().parents[2]
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
BACKEND_TEST_ROOT = ROOT / "backend" / "src" / "test" / "java"


def discover_maven_test_classes() -> set[str]:
    return {path.stem for path in BACKEND_TEST_ROOT.rglob("*Test.java")}


class CiCoverageScopesTest(unittest.TestCase):
    def test_non_cloud_and_testcontainers_lists_are_disjoint(self) -> None:
        overlap = set(NON_CLOUD_MAVEN_TESTS) & set(TESTCONTAINERS_MAVEN_TESTS)
        self.assertEqual(set(), overlap)

    def test_scope_lists_cover_every_backend_test_class(self) -> None:
        discovered = discover_maven_test_classes()
        listed = set(NON_CLOUD_MAVEN_TESTS) | set(TESTCONTAINERS_MAVEN_TESTS)
        self.assertEqual(discovered, listed)

    def test_scope_lists_have_no_duplicates(self) -> None:
        self.assertEqual(len(NON_CLOUD_MAVEN_TESTS), len(set(NON_CLOUD_MAVEN_TESTS)))
        self.assertEqual(len(TESTCONTAINERS_MAVEN_TESTS), len(set(TESTCONTAINERS_MAVEN_TESTS)))

    def test_workflow_splits_non_cloud_and_testcontainers_jobs(self) -> None:
        workflow = CI_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("non-cloud-regression:", workflow)
        self.assertIn("testcontainers-regression:", workflow)
        self.assertIn(maven_test_selector(NON_CLOUD_MAVEN_TESTS), workflow)
        self.assertIn(maven_test_selector(TESTCONTAINERS_MAVEN_TESTS), workflow)
        self.assertIn("cdc-data-platform-non-cloud-evidence", workflow)
        self.assertIn("cdc-data-platform-testcontainers-surefire", workflow)
        self.assertNotIn("cdc-data-platform-local-boundary-evidence", workflow)

    def test_workflow_does_not_run_opt_in_athena_or_dbt(self) -> None:
        workflow = CI_WORKFLOW.read_text(encoding="utf-8")
        for marker in OPT_IN_ONLY_MARKERS:
            self.assertNotIn(marker, workflow)

    def test_workflow_does_not_claim_full_compose_runtime(self) -> None:
        workflow = CI_WORKFLOW.read_text(encoding="utf-8")
        self.assertNotRegex(workflow, re.compile(r"docker\s+compose", re.IGNORECASE))
        self.assertNotIn("applicant-change-capture", workflow)
        self.assertNotIn("infra/local/docker-compose.yml", workflow)

    def test_non_cloud_python_modules_are_importable_names(self) -> None:
        for module_name in NON_CLOUD_PYTHON_TESTS:
            self.assertTrue(module_name.startswith("tools.tests."))
            path = ROOT.joinpath(*module_name.split(".")).with_suffix(".py")
            self.assertTrue(path.is_file(), msg=f"missing {path}")


if __name__ == "__main__":
    unittest.main()
