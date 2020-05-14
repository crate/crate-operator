import kopf
from kubernetes_asyncio.config import load_incluster_config, load_kube_config

from crate.operator.config import config


@kopf.on.startup()
async def configure_kubernetes_client(**kwargs):
    """
    Authenticate with the Kubernetes cluster.

    Upon startup of the Kopf operator, this function attempts to authenticate
    with a Kubernetes cluster. If the
    :attr:`~crate.operator.config.Config.KUBECONFIG` is defined, an attempt
    will be made to use that config file. In other cases, an in-cluster
    authentication will be tried.
    """
    if config.KUBECONFIG:
        await load_kube_config(config_file=config.KUBECONFIG)
    else:
        load_incluster_config()
