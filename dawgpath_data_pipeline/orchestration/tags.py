# Copyright 2026 UW-IT, University of Washington
# SPDX-License-Identifier: Apache-2.0

"""
Kubernetes Pod Sizing Tags for Dagster Executor.
Allows GKE to launch dedicated worker pods sized appropriately for each job tier.
"""

# The k8s tags below are inert under the default run launcher (see
# docs/pipeline-runner-plan.md). TIER_KEY is matched by the default
# executor's tag_concurrency_limits in definitions.py.
TIER_KEY = "dawgpath/tier"
TIER_3_POOL = "tier_3"

TIER_1_K8S_TAGS = {
    TIER_KEY: "tier_1",
    "dagster-k8s/config": {
        "container_config": {
            "resources": {
                "requests": {"cpu": "500m", "memory": "512Mi"},
                "limits": {"cpu": "1000m", "memory": "2Gi"},
            }
        }
    }
}

TIER_2_K8S_TAGS = {
    TIER_KEY: "tier_2",
    "dagster-k8s/config": {
        "container_config": {
            "resources": {
                "requests": {"cpu": "2", "memory": "4Gi"},
                "limits": {"cpu": "4", "memory": "8Gi"},
            }
        }
    }
}

TIER_3_K8S_TAGS = {
    TIER_KEY: "tier_3",
    "dagster-k8s/config": {
        "container_config": {
            "resources": {
                "requests": {"cpu": "4", "memory": "8Gi"},
                "limits": {"cpu": "8", "memory": "16Gi"},
            }
        }
    }
}
