# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from unittest import TestCase

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
SHARED_SECTIONS = ("concurrency", "run_monitoring")


class TestInstanceConfig(TestCase):

    def test_deployed_configmaps_match_repo_dagster_yaml(self):
        repo = yaml.safe_load((REPO_ROOT / "dagster.yaml").read_text())
        for values_file in ("test-values.yml", "prod-values.yml"):
            values = yaml.safe_load(
                (REPO_ROOT / "docker" / values_file).read_text())
            deployed = yaml.safe_load(
                values["configmaps"]["dagster-instance"]["dagster.yaml"])
            for section in SHARED_SECTIONS:
                self.assertEqual(deployed.get(section), repo[section],
                                 f"{values_file}: {section} out of sync")
