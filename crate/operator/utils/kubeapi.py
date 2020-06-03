import asyncio
import logging
from typing import Awaitable, Callable, Optional

from kubernetes_asyncio.client import ApiException, CoreV1Api

from crate.operator.constants import BACKOFF_TIME
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
    namespace: str, secret_key_ref: SecretKeyRef, core: Optional[CoreV1Api] = None
) -> str:
    """
    Lookup the secret value defined by ``secret_key_ref`` in ``namespace``.

    :param namespace: The namespace where to lookup a secret and its value.
    :param secret_key_ref: a ``secretKeyRef`` containing the secret name and
        key within that holds the desired value.
    """
    core = core or CoreV1Api()
    secret_name = secret_key_ref["name"]
    key = secret_key_ref["key"]
    secret = await core.read_namespaced_secret(namespace=namespace, name=secret_name)
    return b64decode(secret.data[key])


async def get_system_user_password(
    namespace: str, name: str, core: Optional[CoreV1Api] = None
) -> str:
    """
    Return the password for the system user of cluster ``name`` in ``namespace``.

    :param namespace: The namespace where the CrateDB cluster is deployed.
    :param name: The name of the CrateDB cluster.
    """
    return await resolve_secret_key_ref(
        namespace, {"key": "password", "name": f"user-system-{name}"}, core,
    )


async def get_public_ip(core: CoreV1Api, namespace: str, name: str) -> str:
    """
    Query the Kubernetes service deployed alongside CrateDB for the public CrateDB
    cluster IP.

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
                and status.load_balancer.ingress[0].ip
            ):
                return status.load_balancer.ingress[0].ip
        except ApiException:
            pass

        await asyncio.sleep(BACKOFF_TIME / 2)
