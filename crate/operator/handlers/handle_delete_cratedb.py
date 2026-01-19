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

from kubernetes_asyncio.client import ApiException, CustomObjectsApi

from crate.operator.config import config
from crate.operator.constants import CloudProvider
from crate.operator.utils.k8s_api_client import GlobalApiClient


async def delete_cratedb(
    namespace: str,
    name: str,
    logger: logging.Logger,
) -> None:
    """
    Clean up cluster-scoped resources that cannot be garbage-collected
    via Kubernetes owner references.

    Namespace-scoped resources (Services, Secrets, StatefulSets, etc.)
    are cleaned up automatically via owner references. However,
    SecurityContextConstraints are cluster-scoped and require explicit
    deletion here.
    """
    await _delete_crate_scc(namespace, name, logger)


async def _delete_crate_scc(
    namespace: str,
    name: str,
    logger: logging.Logger,
) -> None:
    """
    Delete the OpenShift SecurityContextConstraint for a CrateDB cluster.
    """
    if config.CLOUD_PROVIDER != CloudProvider.OPENSHIFT:
        return

    scc_name = f"crate-anyuid-{namespace}-{name}"

    async with GlobalApiClient() as api_client:
        custom_api = CustomObjectsApi(api_client)
        try:
            await custom_api.delete_cluster_custom_object(
                group="security.openshift.io",
                version="v1",
                plural="securitycontextconstraints",
                name=scc_name,
            )
            logger.info("Deleted SCC %s", scc_name)
        except ApiException as e:
            if e.status == 404:
                logger.info("SCC %s already deleted", scc_name)
            else:
                logger.warning(
                    "Could not delete SCC %s (status=%s reason=%s) — "
                    "it may need to be removed manually",
                    scc_name,
                    e.status,
                    e.reason,
                )
