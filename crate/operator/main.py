import logging

# Perform Kubernetes authentication during Kopf startup. This also triggers the
# login for the Kopf framework through PyKube
import crate.operator.kube_auth  # noqa

logger = logging.getLogger(__name__)
