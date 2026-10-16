# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from unittest import TestCase, skipUnless

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
VALUES_FILES = [REPO_ROOT / "docker" / name
                for name in ("test-values.yml", "prod-values.yml")]
SHARED_SECTIONS = ("run_queue", "concurrency", "run_monitoring")


# .dockerignore keeps values files out of the image; CI runs this from the checkout.
@skipUnless(all(path.exists() for path in VALUES_FILES),
            "deploy values files not present")
class TestInstanceConfig(TestCase):

    def test_deployed_configmaps_match_repo_dagster_yaml(self):
        repo = yaml.safe_load((REPO_ROOT / "dagster.yaml").read_text())
        for values_file in VALUES_FILES:
            values = yaml.safe_load(values_file.read_text())
            deployed = yaml.safe_load(
                values["configmaps"]["dagster-instance"]["dagster.yaml"])
            for section in SHARED_SECTIONS:
                self.assertEqual(deployed.get(section), repo[section],
                                 f"{values_file.name}: {section} out of sync")

    def test_daemon_inherits_azure_sql_resolver(self):
        for values_file in VALUES_FILES:
            values = yaml.safe_load(values_file.read_text())
            environment = {
                item["name"]: item.get("value")
                for item in values["environmentVariables"]
            }
            daemon = values["daemon"]["daemons"][0]

            self.assertEqual(environment["LD_PRELOAD"],
                             "libnss_wrapper.so")
            self.assertEqual(
                environment["NSS_WRAPPER_HOSTS"],
                "/app/project/azure-sql-hosts")
            self.assertEqual(daemon["command"],
                             ["/app/bin/dagster-daemon"])
