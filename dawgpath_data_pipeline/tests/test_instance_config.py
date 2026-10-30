# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

from configparser import ConfigParser
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

    def test_uwca_tls_configuration_reaches_all_runtime_containers(self):
        for values_file in VALUES_FILES:
            with self.subTest(values=values_file.name):
                values = yaml.safe_load(values_file.read_text())
                environment = {item["name"]: item.get("value")
                               for item in values["environmentVariables"]}
                self.assertEqual(environment["RESTCLIENTS_CA_BUNDLE"],
                                 "/etc/ssl/certs/ca-certificates.crt")
                self.assertEqual(environment["RESTCLIENTS_SWS_VERIFY_HTTPS"], "True")
                self.assertEqual(environment["RESTCLIENTS_SWS_DAO_CLASS"], "Live")
                self.assertEqual(environment["OPENSSL_CONF"], "/ssl/openssl.cnf")
                mount = values["podVolumes"]["openssl-config"]
                self.assertEqual(mount["volume"]["configMap"]["name"], "openssl-conf")
                self.assertEqual(mount["mount"]["mountPath"], "/ssl")
                self.assertTrue(mount["mount"]["readOnly"])
                self.assertNotIn("containers", mount)
                config = values["configmaps"]["openssl-conf"]["openssl.cnf"]
                self.assertTrue(config.startswith("openssl_conf = default_conf\n"))
                parser = ConfigParser()
                parser.read_string("[openssl]\n" + config)
                self.assertEqual(parser["default_conf"]["ssl_conf"], "ssl_sect")
                self.assertEqual(parser["ssl_sect"]["system_default"], "system_default_sect")
                self.assertEqual(parser["system_default_sect"]["MinProtocol"], "TLSv1.2")
                self.assertEqual(parser["system_default_sect"]["CipherString"],
                                 "DEFAULT:@SECLEVEL=1")

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

    def test_azure_blob_url_uses_shared_vault_secret(self):
        for values_file in VALUES_FILES:
            with self.subTest(values=values_file.name):
                values = yaml.safe_load(values_file.read_text())
                mapping = values["environmentVariablesSecrets"]["azureBlobStorageUrl"]
                self.assertEqual(mapping["name"], "AZURE_BLOB_STORAGE_URL")
                self.assertEqual(mapping["secretKey"], "blob-url")
                secret = next(
                    item for item in values["externalSecrets"]["secrets"]
                    if item["name"] == mapping["secretName"])
                self.assertEqual(
                    secret["externalKey"], "dawgpath/pipeline/shared/secrets")
                self.assertIn(
                    {"name": "blob-url", "property": "blob-url"}, secret["data"])
                self.assertNotIn(
                    "AZURE_BLOB_STORAGE_URL",
                    [item["name"] for item in values["environmentVariables"]])
