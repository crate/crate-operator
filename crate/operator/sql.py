import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import httpx
import kopf
from aiohttp.client_exceptions import WSServerHandshakeError
from kubernetes_asyncio.client import ApiException, CoreV1Api
from kubernetes_asyncio.stream import WsApiClient

from crate.operator.config import config
from crate.operator.constants import CRATE_CONTROL_PORT, CloudProvider
from crate.operator.utils.k8s_api_client import GlobalApiClient
from crate.operator.utils.kubeapi import resolve_secret_key_ref


@dataclass
class SQLResult:
    rowcount: int | None
    rows: list | None
    error_code: int | None
    error_message: str | None

    @property
    def ok(self) -> bool:
        return self.error_code is None and self.error_message is None


def normalize_crate_control(resp: dict) -> SQLResult:
    err = None

    if "error" in resp:
        err = resp["error"]
    elif "detail" in resp:
        detail = resp["detail"]

        if isinstance(detail, str):
            try:
                parsed = json.loads(detail)
                if isinstance(parsed, dict) and "error" in parsed:
                    err = parsed["error"]
                else:
                    err = {"message": detail}
            except Exception:
                err = {"message": detail}

    if err is not None:
        if isinstance(err, str):
            try:
                err = json.loads(err)
            except Exception:
                return SQLResult(
                    rowcount=None,
                    rows=None,
                    error_code=None,
                    error_message=err,
                )

        return SQLResult(
            rowcount=None,
            rows=None,
            error_code=err.get("code"),
            error_message=err.get("message"),
        )

    return SQLResult(
        rowcount=resp.get("rowcount"),
        rows=resp.get("rows"),
        error_code=None,
        error_message=None,
    )


def _convert_cell(value: str) -> Any:
    """
    Convert crash cell value to int/float/bool/None if possible.
    """
    if value == "":
        return None

    if re.fullmatch(r"-?\d+", value):
        return int(value)

    if re.fullmatch(r"-?\d+\.\d+", value):
        return float(value)

    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False

    return value


def parse_crash_table(output: str) -> List[Tuple[Any, ...]]:
    lines = [line.rstrip() for line in output.splitlines() if line.strip()]

    data_lines = [line for line in lines if line.startswith("|") and line.endswith("|")]

    if len(data_lines) < 2:
        return []

    _ = data_lines[0]
    rows = data_lines[1:]

    parsed_rows = []
    for row in rows:
        cells = [cell.strip() for cell in row[1:-1].split("|")]
        parsed_rows.append(tuple(_convert_cell(cell) for cell in cells))

    return parsed_rows


def normalize_crash(output: str) -> SQLResult:
    if "Exception" in output:
        return SQLResult(
            rowcount=None,
            rows=None,
            error_code=None,
            error_message=output,
        )

    if "ALTER OK" in output or "CREATE OK" in output or "GRANT OK" in output:
        return SQLResult(rowcount=1, rows=None, error_code=None, error_message=None)

    # SELECT output
    rows = parse_crash_table(output)
    return SQLResult(rowcount=len(rows), rows=rows, error_code=None, error_message=None)


async def run_crash_command(
    namespace: str,
    pod_name: str,
    scheme: str,
    command: str,
    logger,
    delay: int = 30,
):
    """
    This connects to a CrateDB pod and executes a crash command in the
    ``crate`` container. It returns the result of the execution.

    :param namespace: The Kubernetes namespace of the CrateDB cluster.
    :param pod_name: The pod name where the command should be run.
    :param scheme: The host scheme for running the command.
    :param command: The SQL query that should be run.
    :param logger: the logger on which we're logging
    :param delay: Time in seconds between the retries when executing
        the query.
    """
    async with WsApiClient() as ws_api_client:
        core_ws = CoreV1Api(ws_api_client)
        try:
            exception_logger = logger.exception if config.TESTING else logger.error
            crash_command = [
                "crash",
                "--verify-ssl=false",
                f"--host={scheme}://localhost:4200",
                "-c",
                command,
            ]
            result = await core_ws.connect_get_namespaced_pod_exec(
                namespace=namespace,
                name=pod_name,
                command=crash_command,
                container="crate",
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
            )
        except ApiException as e:
            # We don't use `logger.exception()` to not accidentally include sensitive
            # data in the log messages which might be part of the string
            # representation of the exception.
            exception_logger("... failed. Status: %s Reason: %s", e.status, e.reason)
            raise kopf.TemporaryError(delay=delay)
        except WSServerHandshakeError as e:
            # We don't use `logger.exception()` to not accidentally include sensitive
            # data in the log messages which might be part of the string
            # representation of the exception.
            exception_logger("... failed. Status: %s Message: %s", e.status, e.message)
            raise kopf.TemporaryError(delay=delay)
        else:
            return result


async def execute_sql_via_crate_control(
    *,
    namespace: str,
    name: str,
    sql: str,
    args: Optional[List[Any]],
    logger: logging.Logger,
):
    control_host = f"crate-control-{name}.{namespace}.svc.cluster.local"
    url = f"http://{control_host}:{CRATE_CONTROL_PORT}/exec"

    async with GlobalApiClient() as api_client:
        core = CoreV1Api(api_client)
        bootstrap_token = await resolve_secret_key_ref(
            core,
            namespace,
            {"name": f"crate-control-{name}", "key": "token"},
        )
        logger.info("Resolved crate-control bootstrap token")

    payload: Dict[str, Any] = {"stmt": sql}
    if args is not None:
        payload["args"] = args
    logger.info("Sending SQL to crate-control sidecar: %s", sql[:80])

    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.post(
            url,
            headers={"token": bootstrap_token},
            json=payload,
        )

    if resp.status_code >= 500:
        raise kopf.TemporaryError("Sidecar unavailable", delay=5)

    data = resp.json()
    if resp.status_code >= 400:
        logger.info("Sidecar returned %s: %s", resp.status_code, data)

    return data


async def execute_sql(
    *,
    namespace: str,
    name: str,
    pod_name: str,
    scheme: str,
    sql: str,
    args: list | None,
    logger: logging.Logger,
) -> SQLResult:
    if config.CLOUD_PROVIDER == CloudProvider.OPENSHIFT:
        logger.info("Using sidecar SQL execution")
        resp = await execute_sql_via_crate_control(
            namespace=namespace,
            name=name,
            sql=sql,
            args=args,
            logger=logger,
        )
        return normalize_crate_control(resp)
    else:
        logger.info("Using legacy pod_exec SQL execution")
        raw = await run_crash_command(
            namespace=namespace,
            pod_name=pod_name,
            scheme=scheme,
            command=sql,
            logger=logger,
        )
        return normalize_crash(raw)
