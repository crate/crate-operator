#!/usr/bin/env python3
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

import hmac
import json
import logging
import os
import signal
import socket
import ssl
import sys
from base64 import b64encode
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from bottle import Bottle, request, response
from waitress import serve

BOOTSTRAP_TOKEN = os.environ.get("BOOTSTRAP_TOKEN", "")
CRATE_HTTP_URL = os.environ.get("CRATE_HTTP_URL", "http://localhost:4200/_sql")
CRATE_USERNAME = os.environ.get("CRATE_USERNAME", "")
CRATE_PASSWORD = os.environ.get("CRATE_PASSWORD", "")
VERIFY_SSL = os.environ.get("VERIFY_SSL", "false").lower() == "true"
HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "30"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("crate-control")

SSL_CONTEXT = None if VERIFY_SSL else ssl._create_unverified_context()

app = Bottle()


def abort_json(status: int, payload: dict):
    """
    Return JSON error response.
    """
    response.status = status
    response.content_type = "application/json"
    return json.dumps(payload)


def verify_token():
    """
    Verify bootstrap token from request header.

    Uses hmac.compare_digest() for timing-safe comparison to prevent
    timing attacks that could leak the token value.
    """
    token = request.headers.get("Token") or ""
    if not hmac.compare_digest(token, BOOTSTRAP_TOKEN):
        logger.warning("unauthorized_request")
        return False
    return True


def crate_request(stmt: str, args):
    """
    Execute SQL request against CrateDB HTTP endpoint.

    Returns: (status_code, response_body_string)
    Raises: RuntimeError on connection failure
    """
    body = {"stmt": stmt}
    if args is not None:
        body["args"] = args

    data = json.dumps(body).encode("utf-8")

    req = Request(CRATE_HTTP_URL, data=data, method="POST")
    req.add_header("Content-Type", "application/json")

    if CRATE_USERNAME:
        auth = f"{CRATE_USERNAME}:{CRATE_PASSWORD}".encode()
        req.add_header("Authorization", "Basic " + b64encode(auth).decode())

    try:
        with urlopen(req, timeout=HTTP_TIMEOUT, context=SSL_CONTEXT) as resp:
            payload = resp.read().decode()
            return resp.status, payload
    except HTTPError as e:
        return e.code, e.read().decode()
    except (URLError, TimeoutError, socket.timeout) as e:
        raise RuntimeError(f"connection_failed: {e}")


@app.get("/health")
def health():
    """
    Health check endpoint.
    """
    return {"status": "ok"}


@app.post("/exec")
def exec_sql():
    """
    Execute SQL statement against CrateDB.
    """
    if not verify_token():
        return abort_json(401, {"detail": "Invalid or missing token"})

    try:
        payload = request.json
        if not payload or "stmt" not in payload:
            return abort_json(400, {"detail": "Missing stmt"})

        stmt = payload["stmt"]
        args = payload.get("args")

        logger.info("sql_execution_requested: %s", stmt[:80])

        status, body = crate_request(stmt, args)
        response.content_type = "application/json"

        if status >= 400:
            logger.error("query_failed status=%d", status)
            response.status = status
            return body

        logger.info("query_succeeded status=%d", status)
        return body
    except RuntimeError as e:
        logger.warning("crate_not_ready: %s", e)
        return abort_json(503, {"detail": str(e), "error_type": "connection_error"})
    except Exception as e:
        logger.exception("unexpected_error")
        return abort_json(500, {"detail": str(e), "error_type": "internal_error"})


def handle_signal(sig, frame):
    """
    Handle SIGTERM/SIGINT for graceful shutdown.
    """
    logger.info("received signal %s, shutting down...", sig)
    sys.exit(0)


signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)

if __name__ == "__main__":
    logger.info("starting crate-control on port 5050")
    logger.info("crate_url=%s verify_ssl=%s", CRATE_HTTP_URL, VERIFY_SSL)

    serve(app, host="0.0.0.0", port=5050, threads=2, cleanup_interval=30)
