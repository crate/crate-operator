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

import json
import logging
from typing import Any, Optional

import kopf

from crate.operator.config import config
from crate.operator.constants import KOPF_STATE_STORE_PREFIX, OperationType
from crate.operator.utils import crate
from crate.operator.utils.kopf import StateBasedSubHandler


class FinalRollbackSubHandler(StateBasedSubHandler):
    @crate.timeout(timeout=float(config.CLUSTER_UPDATE_TIMEOUT))
    async def handle(
        self,
        **kwargs: Any,
    ):
        annotations = kwargs["annotations"]
        logger = kwargs["logger"]

        logger.info("Evaluating rollback conditions...")

        failed_handlers = self._get_failed_dependent_handlers(annotations, logger)

        if failed_handlers:
            logger.info(f"Rollback triggered due to failed handlers: {failed_handlers}")
            await self.perform_rollback(**kwargs)
        else:
            logger.info("No rollback needed. All handlers succeeded.")

    async def perform_rollback(self, **kwargs):
        logger = kwargs["logger"]
        patch = kwargs["patch"]
        status = kwargs["status"]

        body = kwargs.get("body")
        old = kwargs.get("old")

        if not body or not old:
            logger.error("Missing required 'body' or 'old' in rollback context.")
            return

        logger.info(f"Determining rollback class for operation: {self.operation}")

        if self.operation is None:
            logger.info("Operation type is not set; cannot determine rollback handler.")
            return

        rollback_handler_class = self._get_rollback_handler_class(self.operation)

        if rollback_handler_class is None:
            logger.info(f"No rollback handler defined for operation: {self.operation}")
            return

        handler_name = rollback_handler_class.__name__

        logger.info(f"Starting rollback using handler: {handler_name}")

        rollback_handler = rollback_handler_class(
            namespace=self.namespace,
            name=self.name,
            body=body,
            patch=patch,
            logger=logger,
            ref=self.ref,
            context=self._context,
        )

        try:
            # manually invoke the rollback subhandler
            logger.info(f"[{handler_name}] Invoking rollback method...")
            result = await rollback_handler.rollback(**kwargs)

            key = kopf.AnnotationsProgressStorage(
                v1=False, prefix=KOPF_STATE_STORE_PREFIX
            ).make_v2_key(handler_name)
            logger.info(f"[{handler_name}] Setting success annotation: {key}")

            patch.setdefault("metadata", {}).setdefault("annotations", {})[key] = (
                json.dumps({"success": True, "failure": False})
            )
            patch.setdefault("status", {}).setdefault(handler_name, {}).update(
                {
                    "ref": self.ref,
                    "result": result,
                    "success": True,
                }
            )
            try:
                if "pendingPods" in status:
                    logger.info("Clearing pendingPods from status.")
                    patch.status["pendingPods"] = None
            except Exception as e:
                logger.warning(f"Failed to clear pendingPods during rollback: {e}")

            logger.info(f"Rollback {handler_name} completed successfully.")
        except Exception as e:
            logger.exception(f"Rollback handler {handler_name} failed: {e}")
            key = kopf.AnnotationsProgressStorage(
                v1=False, prefix=KOPF_STATE_STORE_PREFIX
            ).make_v2_key(handler_name)
            logger.info(f"[{handler_name}] Setting failure annotation: {key}")
            patch.setdefault("metadata", {}).setdefault("annotations", {})[key] = (
                json.dumps({"success": False, "failure": True})
            )
            patch.status[handler_name] = {
                "ref": self.ref,
                "error": str(e),
                "success": False,
            }

    def _get_rollback_handler_class(self, operation: OperationType):
        return {
            OperationType.UPGRADE: RollbackUpgradeSubHandler,
        }.get(operation)


class RollbackHandler:
    def __init__(
        self,
        namespace: str,
        name: str,
        body: kopf.Body,
        patch: kopf.Patch,
        logger: logging.Logger,
        ref: Optional[str] = None,
        context: Optional[dict] = None,
    ):
        self.namespace = namespace
        self.name = name
        self.body = body
        self.patch = patch
        self.logger = logger
        self.ref = ref
        self.context = context or {}

    def is_in_rollback(self) -> bool:
        return self.body.metadata.annotations.get(self.annotation_key()) == "true"

    def mark_rollback(self):
        key = self.annotation_key()
        self.logger.info(f"[{key}] Marking resource as IN rollback.")
        self.patch.metadata.setdefault("annotations", {})[key] = "true"

    def clear_rollback(self):
        key = self.annotation_key()
        self.logger.info(f"[{key}] Clearing rollback state.")
        self.patch.metadata.setdefault("annotations", {})[key] = "false"

    def annotation_key(self) -> str:
        return self.annotation_key_for(self.get_operation_type())

    @staticmethod
    def annotation_key_for(op: OperationType) -> str:
        return f"{KOPF_STATE_STORE_PREFIX}/rollback-{op.value}"

    async def rollback(self):
        raise NotImplementedError("Must be implemented by subclasses.")

    def get_operation_type(self) -> OperationType:
        raise NotImplementedError("Must be implemented by subclasses.")


class RollbackUpgradeSubHandler(RollbackHandler):
    def get_operation_type(self) -> OperationType:
        return OperationType.UPGRADE

    async def rollback(self, **kwargs):

        current_version = kwargs["body"].spec["cluster"]["version"]
        failed_from_version = kwargs["old"]["spec"]["cluster"]["version"]
        self.logger.info(
            f"[UpgradeRollback] Current version: {current_version}, "
            f"Target rollback version: {failed_from_version}"
        )
        if failed_from_version and current_version != failed_from_version:
            self.logger.warning(
                f"Rolling back version from {current_version} to {failed_from_version}"
            )
            self.mark_rollback()
            self.patch.setdefault("spec", {}).setdefault("cluster", {})[
                "version"
            ] = failed_from_version
        else:
            self.logger.info(
                "[UpgradeRollback] No rollback needed. Version already matches."
            )
