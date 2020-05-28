from typing import Dict, TypeVar

from kubernetes_asyncio.client.models import (
    V1beta1CronJob,
    V1ConfigMap,
    V1Deployment,
    V1PersistentVolume,
    V1PersistentVolumeClaim,
    V1Secret,
    V1Service,
    V1StatefulSet,
)

LABEL_TYPE = Dict[str, str]
K8S_MODEL = TypeVar(
    "K8S_MODEL",
    V1beta1CronJob,
    V1ConfigMap,
    V1Deployment,
    V1PersistentVolume,
    V1PersistentVolumeClaim,
    V1Secret,
    V1Service,
    V1StatefulSet,
)
