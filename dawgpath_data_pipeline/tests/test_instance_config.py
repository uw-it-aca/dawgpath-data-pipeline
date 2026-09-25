# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from unittest import TestCase, skipUnless

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
VALUES_FILES = [REPO_ROOT / "docker" / name
                for name in ("test-values.yml", "prod-values.yml")]
SHARED_SECTIONS = ("concurrency", "run_monitoring")


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
