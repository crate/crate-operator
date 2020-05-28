import logging
from typing import Awaitable, Callable, Optional

from kubernetes_asyncio.client import ApiException

from crate.operator.utils.typing import K8S_MODEL


async def call_kubeapi(
    method: Callable[..., Awaitable],
    logger: logging.Logger,
    *,
    continue_on_absence=False,
    continue_on_conflict=False,
    namespace: str = None,
    body: K8S_MODEL = None,
    **kwargs,
) -> Optional[Awaitable[K8S_MODEL]]:
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
