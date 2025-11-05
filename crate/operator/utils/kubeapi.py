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
from typing import Awaitable, Callable, Optional

import kopf
from kubernetes_asyncio.client import (
    ApiException,
    CoreV1Api,
    CustomObjectsApi,
    V1ObjectMeta,
    V1Secret,
)

from crate.operator.config import config
from crate.operator.constants import (
    API_GROUP,
    GC_USER_SECRET_NAME,
    LABEL_USER_PASSWORD,
    RESOURCE_CRATEDB,
)
from crate.operator.utils.formatting import b64decode
from crate.operator.utils.k8s_api_client import GlobalApiClient
from crate.operator.utils.typing import K8sModel, SecretKeyRef


async def call_kubeapi(
    method: Callable[..., Awaitable],
    logger: logging.Logger,
    *,
    continue_on_absence=False,
    continue_on_conflict=False,
    namespace: Optional[str] = None,
    body: Optional[K8sModel] = None,
    **kwargs,
) -> Optional[Awaitable[K8sModel]]:
    """
    Await a Kubernetes API method and return its result.

    If the API fails with an HTTP 404 NOT FOUND error and
    ``continue_on_absence`` is set to ``True`` a warning is raised and
    ``call_kubeapi`` returns ``None``.

    If the API fails with an HTTP 409 CONFLICT error and
    ``continue_on_conflict`` is set to ``True`` a warning is raised and
    ``call_kubeapi`` returns ``None``.

    In case of any other error or when either option is set to ``False``
    (default) the :exc:`kubernetes_asyncio.client.exceptions.ApiException` is
    re-raised.

    :param method: A Kubernetes API function which will be called with
        ``namespace`` and ``body``, if provided, and all other ``kwargs``. The
        function will also be awaited and the response returned.
    :param logger:
    :param continue_on_absence: When ``True``, emit a warning instead of an
        error on HTTP 404 responses.
    :param continue_on_conflict: When ``True``, emit a warning instead of an
        error on HTTP 409 responses.
    :param namespace: The namespace passed to namespaced K8s API endpoints.
    :param body: The body passed to the K8s API endpoints.
    """
    try:
        if namespace is not None:
            kwargs["namespace"] = namespace
        if body is not None:
            kwargs["body"] = body
        return await method(**kwargs)
    except ApiException as e:
        if (
            e.status == 409
            and continue_on_conflict
            or e.status == 404
            and continue_on_absence
        ):
            msg = ["Failed", "creating" if e.status == 409 else "deleting"]
            args = []

            if body:
                if e.status == 409:
                    # For 404 the body is `V1DeleteOptions`; not very helpful.
                    msg.append("%s")
                    args.append(body.__class__.__name__)

                if namespace:
                    obj_name = None
                    if e.status == 404:
                        # Let's try the explicit name
                        obj_name = kwargs.get("name")
                    if obj_name is None:
                        obj_name = getattr(
                            getattr(body, "metadata", None), "name", "<unknown>"
                        )
                    msg.append("'%s/%s'")
                    args.extend([namespace, obj_name])

            cause = "already exists" if e.status == 409 else "doesn't exist"
            msg.append(f"because it {cause}. Continuing.")
            logger.info(" ".join(msg), *args)
            return None
        else:
            raise


async def resolve_secret_key_ref(
    core: CoreV1Api, namespace: str, secret_key_ref: SecretKeyRef
) -> str:
    """
    Lookup the secret value defined by ``secret_key_ref`` in ``namespace``.

    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The namespace where to lookup a secret and its value.
    :param secret_key_ref: a ``secretKeyRef`` containing the secret name and
        key within that holds the desired value.
    """
    secret_name = secret_key_ref["name"]
    key = secret_key_ref["key"]
    secret = await core.read_namespaced_secret(namespace=namespace, name=secret_name)
    return b64decode(secret.data[key])


async def get_system_user_password(core: CoreV1Api, namespace: str, name: str) -> str:
    """
    Return the password for the system user of cluster ``name`` in ``namespace``.

    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The namespace where the CrateDB cluster is deployed.
    :param name: The name of the CrateDB cluster.
    """
    return await resolve_secret_key_ref(
        core, namespace, {"key": "password", "name": f"user-system-{name}"}
    )


async def get_service_public_hostname(
    core: CoreV1Api, namespace: str, name: str
) -> str:
    """
    Query the given CrateDB Kubernetes Service fo it's public IP address / hostname.

    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The namespace where the CrateDB cluster is deployed.
    :param name: The name of the CrateDB cluster.
    """
    services = await core.list_namespaced_service(namespace)
    service = next(
        (svc for svc in services.items if svc.metadata.name == f"crate-{name}"),
        None,
    )
    if (
        service
        and service.status
        and service.status.load_balancer
        and service.status.load_balancer.ingress
        and service.status.load_balancer.ingress[0]
    ):
        if service.status.load_balancer.ingress[0].ip:
            return service.status.load_balancer.ingress[0].ip
        elif service.status.load_balancer.ingress[0].hostname:
            return service.status.load_balancer.ingress[0].hostname

    raise kopf.TemporaryError("Waiting for service to be created...", delay=5)


async def get_host(core: CoreV1Api, namespace: str, name: str) -> str:
    """
    Return the hostname to the CrateDB cluster within the Kubernetes cluster.
    This uses the "discovery" service for internal access, since the public-facing
    "crate" service can be IP-restricted.

    During testing, the function returns the public IP address, because the
    operator doesn't run inside Kubernetes during tests but outside. And the
    only way to connect to the CrateDB cluster is to go through the public
    interface.

    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The namespace where the CrateDB cluster is deployed.
    :param name: The name of the CrateDB cluster.
    """
    if config.TESTING:
        # During testing we need to connect to the cluster via its public IP
        # address, because the operator isn't running inside the Kubernetes
        # cluster.
        return await get_service_public_hostname(core, namespace, name)

    return f"crate-discovery-{name}.{namespace}"


async def ensure_user_password_label(core: CoreV1Api, namespace: str, secret_name: str):
    """
    Add the LABEL_USER_PASSWORD label to a namespaced secret.

    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The namespace where the Kubernetes Secret is deployed.
    :param secret_name: The name of the Kubernetes Secret.
    """
    await core.patch_namespaced_secret(
        namespace=namespace,
        name=secret_name,
        body=V1Secret(
            metadata=V1ObjectMeta(
                labels={LABEL_USER_PASSWORD: "true"},
            ),
        ),
    )


async def get_cratedb_resource(namespace: str, name: str) -> dict:
    """
    Return the CrateDB custom resource.

    :param namespace: The Kubernetes namespace where to look up the CrateDB
        cluster.
    :param name: The CrateDB custom resource name defining the CrateDB cluster.
    """
    async with GlobalApiClient() as api_client:
        coapi = CustomObjectsApi(api_client)
        return await coapi.get_namespaced_custom_object(
            group=API_GROUP,
            version="v1",
            plural=RESOURCE_CRATEDB,
            namespace=namespace,
            name=name,
        )


async def get_gc_user_password(core: CoreV1Api, namespace: str, name: str) -> str:
    """
    Return the password for the grand-central user of cluster ``name`` in ``namespace``.

    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The namespace where the CrateDB cluster is deployed.
    :param name: The name of the CrateDB cluster.
    """
    return await resolve_secret_key_ref(
        core,
        namespace,
        {"key": "password", "name": GC_USER_SECRET_NAME.format(name=name)},
    )
