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

import asyncio
import logging
from typing import Awaitable, Callable, Optional

from kubernetes_asyncio.client import ApiException, CoreV1Api, V1ObjectMeta, V1Secret

from crate.operator.config import config
from crate.operator.constants import BACKOFF_TIME, LABEL_USER_PASSWORD
from crate.operator.utils.formatting import b64decode
from crate.operator.utils.typing import K8sModel, SecretKeyRef


async def call_kubeapi(
    method: Callable[..., Awaitable],
    logger: logging.Logger,
    *,
    continue_on_absence=False,
    continue_on_conflict=False,
    namespace: str = None,
    body: K8sModel = None,
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


async def get_public_host(core: CoreV1Api, namespace: str, name: str) -> str:
    """
    Query the Kubernetes service deployed alongside CrateDB for the public
    CrateDB cluster IP or hostname.

    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The namespace where the CrateDB cluster is deployed.
    :param name: The name of the CrateDB cluster.
    """
    while True:
        try:
            service = await core.read_namespaced_service(
                namespace=namespace, name=f"crate-{name}"
            )
            status = service.status
            if (
                status
                and status.load_balancer
                and status.load_balancer.ingress
                and status.load_balancer.ingress[0]
            ):
                if status.load_balancer.ingress[0].ip:
                    return status.load_balancer.ingress[0].ip
                elif status.load_balancer.ingress[0].hostname:
                    return status.load_balancer.ingress[0].hostname
        except ApiException:
            pass

        await asyncio.sleep(BACKOFF_TIME / 2)


async def get_host(core: CoreV1Api, namespace: str, name: str) -> str:
    """
    Return the hostname to the CrateDB cluster within the Kubernetes cluster.

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
        return await get_public_host(core, namespace, name)

    return f"crate-{name}.{namespace}"


async def ensure_user_password_label(core: CoreV1Api, namespace: str, secret_name: str):
    """
    Add the LABEL_USER_PASSWORD label to a namespaced secret.

    During testing, the function returns the public IP address, because the
    operator doesn't run inside Kubernetes during tests but outside. And the
    only way to connect to the CrateDB cluster is to go through the public
    interface.

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
