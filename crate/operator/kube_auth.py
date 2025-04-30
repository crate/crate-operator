# CrateDB Kubernetes Operator
#
# Licensed to Crate.IO GmbH ("Crate") under one or more contributor
# license agreements.  See the NOTICE file distributed with this work for
# additional information regarding copyright ownership.  Crate licenses
# this file to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.
#
# However, if you have executed another commercial license agreement
# with Crate these terms will supersede the license and you may use the
# software solely pursuant to the terms of the relevant commercial agreement.

import logging
from typing import Any, Optional, Sequence, Union

from kopf import ConnectionInfo
from kubernetes_asyncio.client import Configuration
from kubernetes_asyncio.config import load_incluster_config, load_kube_config

from crate.operator.config import config


async def login_via_kubernetes_asyncio(
    logger: Union[logging.Logger, logging.LoggerAdapter], **kwargs: Any
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
    header: Optional[str] = k8s_config.get_api_key_with_prefix("BearerToken")
    parts: Sequence[str] = header.split(" ", 1) if header else []
    scheme, token = (
        (None, None)
        if len(parts) == 0
        else (None, parts[0]) if len(parts) == 1 else (parts[0], parts[1])
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
