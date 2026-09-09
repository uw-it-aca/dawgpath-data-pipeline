"""
Kubernetes Pod Sizing Tags for Dagster Executor.
Allows GKE to launch dedicated worker pods sized appropriately for each job tier.
"""

TIER_1_K8S_TAGS = {
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
    "dagster-k8s/config": {
        "container_config": {
            "resources": {
                "requests": {"cpu": "4", "memory": "8Gi"},
                "limits": {"cpu": "8", "memory": "16Gi"},
            }
        }
    }
}
