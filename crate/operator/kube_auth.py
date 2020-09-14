# CrateDB Kubernetes Operator
# Copyright (C) 2020 Crate.IO GmbH
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import logging
from typing import Any, Optional, Sequence, Union

from kopf import ConnectionInfo
from kubernetes_asyncio.client import Configuration
from kubernetes_asyncio.config import load_incluster_config, load_kube_config

from crate.operator.config import config


async def login_via_kubernetes_asyncio(
    logger: Union[logging.Logger, logging.LoggerAdapter], **kwargs: Any,
) -> ConnectionInfo:
    """
    Authenticate with the Kubernetes cluster.

    Upon startup of the Kopf operator, this function attempts to authenticate
    with a Kubernetes cluster. If the
    :attr:`~crate.operator.config.Config.KUBECONFIG` is defined, an attempt
    will be made to use that config file. In other cases, an in-cluster
    authentication will be tried.
    """
    if config.KUBECONFIG:
        logger.info("Authenticating with KUBECONFIG='%s'", config.KUBECONFIG)
        await load_kube_config(config_file=config.KUBECONFIG)
    else:
        logger.info("Authenticating with in-cluster config")
        load_incluster_config()

    # Below follows a copy of Kopf's `kopf.utilities.piggybacking.login_via_client`

    # We do not even try to understand how it works and why. Just load it, and
    # extract the results.
    k8s_config = Configuration.get_default_copy()

    # For auth-providers, this method is monkey-patched with the
    # auth-provider's one.
    # We need the actual auth-provider's token, so we call it instead of
    # accessing api_key.
    # Other keys (token, tokenFile) also end up being retrieved via this method.
    header: Optional[str] = k8s_config.get_api_key_with_prefix("authorization")
    parts: Sequence[str] = header.split(" ", 1) if header else []
    scheme, token = (
        (None, None)
        if len(parts) == 0
        else (None, parts[0])
        if len(parts) == 1
        else (parts[0], parts[1])
    )  # RFC-7235, Appendix C.

    # Interpret the k8s_config object for our own minimalistic credentials.
    # Note: kubernetes client has no concept of a "current" context's namespace.
    c = ConnectionInfo(
        server=k8s_config.host,
        ca_path=k8s_config.ssl_ca_cert,  # can be a temporary file
        insecure=not k8s_config.verify_ssl,
        username=k8s_config.username or None,  # an empty string when not defined
        password=k8s_config.password or None,  # an empty string when not defined
        scheme=scheme,
        token=token,
        certificate_path=k8s_config.cert_file,  # can be a temporary file
        private_key_path=k8s_config.key_file,  # can be a temporary file
        priority=30,  # The priorities for `client` and `pykube-ng` are 10 and 20.
    )
    return c
