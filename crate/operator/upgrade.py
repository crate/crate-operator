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

import asyncio
import logging
import re
from typing import Any, Dict, List, Optional

import kopf
from kubernetes_asyncio.client import AppsV1Api, CoreV1Api

from crate.operator.config import config
from crate.operator.constants import INTERNAL_TABLES, LUCENE_MIN_VERSION_MAP
from crate.operator.cratedb import connection_factory
from crate.operator.operations import get_total_nodes_count
from crate.operator.scale import get_container
from crate.operator.utils import crate, quorum
from crate.operator.utils.k8s_api_client import GlobalApiClient
from crate.operator.utils.kopf import StateBasedSubHandler
from crate.operator.utils.kubeapi import get_host, get_system_user_password
from crate.operator.utils.version import CrateVersion
from crate.operator.webhooks import WebhookEvent, WebhookStatus, WebhookUpgradePayload


def upgrade_command_data_nodes(
    old_command: List[str], total_data_nodes: int
) -> List[str]:
    """
    Iterate through the ``old_command`` items and upgrade the setting's
    names where required in versions >= 4.7.

    Return the list making up the new CrateDB command.

    :param old_command: The command used to start-up CrateDB inside a
        Kubernetes container. This consists of the path to the Docker
        entrypoint script, the ``crate`` command argument and any additional
        settings.
    :param total_data_nodes: The number of data nodes that will be in the
        CrateDB cluster. From that, the quorum is derived as well.
    :return: The list forming the new CrateDB command.
    """
    new_command: List[str] = []
    for item in old_command:
        if item.startswith("-Cgateway.recover_after_nodes="):
            item = f"-Cgateway.recover_after_data_nodes={quorum(total_data_nodes)}"
        elif item.startswith("-Cgateway.expected_nodes="):
            item = f"-Cgateway.expected_data_nodes={total_data_nodes}"
        new_command.append(item)
    return new_command


def upgrade_command_jwt_auth(command: List[str]) -> List[str]:
    """
    Add the settings required for JWT authentication if they are not present yet.

    Return the list making up the new CrateDB command.

    :param old_command: The command used to start-up CrateDB inside a
        Kubernetes container. This consists of the path to the Docker
        entrypoint script, the ``crate`` command argument and any additional
        settings.
    :return: The list forming the new CrateDB command.
    """
    for c in [
        "-Cauth.host_based.config.98.method=jwt",
        "-Cauth.host_based.config.98.protocol=http",
        "-Cauth.host_based.config.98.ssl=on",
    ]:
        if c not in command:
            command.append(c)

    return command


def upgrade_command_global_jwt_config(
    command: List[str], name: str, cloud_settings: Dict[str, str]
) -> List[str]:
    global_jwt_config = {
        "-Cauth.host_based.jwt.iss": cloud_settings.get("jwkUrl"),
        "-Cauth.host_based.jwt.aud": name,
    }

    for key, value in global_jwt_config.items():
        if not any(item.startswith(key) for item in command):
            command.append(f"{key}={value}")

    return command


def upgrade_command_hostname_and_zone(old_command: List[str]) -> List[str]:
    """
    Replace old patterns using ``rev | cut`` with new awk-based equivalents.

    Return the list making up the new CrateDB command.

    :param old_command: The command used to start-up CrateDB inside a
        Kubernetes container. This consists of the path to the Docker
        entrypoint script, the ``crate`` command argument and any additional
        settings.
    :return: The list forming the new CrateDB command.
    """
    new_command = []
    for item in old_command:
        if item.startswith("-Cnode.name=") and "rev | cut -d- -f1 | rev" in item:
            # Replace rev-based extraction with awk.
            item = item.replace("rev | cut -d- -f1 | rev", "awk -F- '{print $NF}'")
        elif (
            item.startswith("-Cnode.attr.zone=")
            and "rev | cut -d '/' -f 1 | rev" in item
        ):
            item = item.replace(
                "rev | cut -d '/' -f 1 | rev", "awk -F'/' '{print $NF}'"
            )
        new_command.append(item)
    return new_command


async def update_statefulset(
    apps: AppsV1Api,
    namespace: str,
    sts_name: str,
    crate_image: str,
    old_version: str,
    new_version: str,
    data_nodes_count: int,
    name: str,
    cloud_settings: Optional[Dict[str, str]],
    logger: logging.Logger,
):
    await apps.patch_namespaced_stateful_set(
        namespace=namespace,
        name=sts_name,
        body={"spec": {"rollingUpdate": None, "updateStrategy": {"type": "OnDelete"}}},
    )
    body: Dict[str, Any] = {
        "spec": {
            "template": {
                "spec": {
                    "containers": [{"name": "crate", "image": crate_image}],
                    "initContainers": [
                        {"name": "mkdir-heapdump", "image": crate_image}
                    ],
                }
            }
        }
    }
    statefulset = await apps.read_namespaced_stateful_set(
        namespace=namespace, name=sts_name
    )
    crate_container = get_container(statefulset)
    command = crate_container.command

    if CrateVersion(old_version) < CrateVersion(
        config.GATEWAY_SETTINGS_DATA_NODES_VERSION
    ) and CrateVersion(new_version) >= CrateVersion(
        config.GATEWAY_SETTINGS_DATA_NODES_VERSION
    ):
        # upgrading to a version >= 4.7 requires changing the gateway settings
        # names and using the number of data nodes instead of total nodes.
        command = upgrade_command_data_nodes(command, data_nodes_count)
        logger.info("upgraded data nodes sts command: %s", command)

    if CrateVersion(old_version) < CrateVersion(
        config.CRATEDB_JWT_AUTH_VERSION
    ) and CrateVersion(new_version) >= CrateVersion(config.CRATEDB_JWT_AUTH_VERSION):
        # upgrading to a version >= 5.7.2 requires changing the auth config
        command = upgrade_command_jwt_auth(command)
        logger.info("upgraded jwt auth sts command: %s", command)

    if (
        CrateVersion(new_version)
        >= CrateVersion(config.CRATEDB_JWT_GLOBAL_CONFIG_VERSION)
        and cloud_settings
        and cloud_settings.get("jwkUrl")
    ):
        command = upgrade_command_global_jwt_config(command, name, cloud_settings)
        logger.info("upgraded jwt global jwt sts command: %s", command)

    if any("rev | cut" in item for item in command):
        # Apply the hostname and zone command upgrade if the old rev pattern is present.
        command = upgrade_command_hostname_and_zone(command)
        logger.info("upgraded hostname and zone sts command: %s", command)

    body["spec"]["template"]["spec"]["containers"][0]["command"] = command

    await apps.patch_namespaced_stateful_set(
        namespace=namespace,
        name=sts_name,
        body=body,
    )


async def upgrade_cluster(
    apps: AppsV1Api,
    namespace: str,
    name: str,
    body: kopf.Body,
    old: kopf.Body,
    logger: logging.Logger,
):
    """
    Update the Docker image in all StatefulSets for the cluster.

    For the changes to take affect, the cluster needs to be restarted.

    :param apps: An instance of the Kubernetes Apps V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The name for the ``CrateDB`` custom resource.
    :param body: The full body of the ``CrateDB`` custom resource per
        :class:`kopf.Body`.
    :param old: The old resource body. Required to get the old version.
    """
    old_version = old["spec"]["cluster"]["version"]
    crate_image = (
        body.spec["cluster"]["imageRegistry"] + ":" + body.spec["cluster"]["version"]
    )
    data_nodes_count = get_total_nodes_count(body.spec["nodes"], "data")

    updates = []
    if "master" in body.spec["nodes"]:
        updates.append(
            update_statefulset(
                apps,
                namespace,
                f"crate-master-{name}",
                crate_image,
                old_version,
                body.spec["cluster"]["version"],
                data_nodes_count,
                name,
                body.spec.get("grandCentral", {}),
                logger,
            )
        )
    updates.extend(
        [
            update_statefulset(
                apps,
                namespace,
                f"crate-data-{node_spec['name']}-{name}",
                crate_image,
                old_version,
                body.spec["cluster"]["version"],
                data_nodes_count,
                name,
                body.spec.get("grandCentral", {}),
                logger,
            )
            for node_spec in body.spec["nodes"]["data"]
        ]
    )

    await asyncio.gather(*updates)


def _get_major_or_error(version: CrateVersion) -> int:
    """Helper to safely get the major version or raise an error."""
    if version.major is None:
        raise kopf.PermanentError(f"Invalid CrateDB version: {version}")
    return version.major


async def check_reindexing_tables(
    core: CoreV1Api,
    namespace: str,
    name: str,
    body: kopf.Body,
    old: kopf.Body,
    logger: logging.Logger,
):
    """
    Check if there are any tables that need re-indexing before a
    major version upgrade.

    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The name for the ``CrateDB`` custom resource.
    :param body: The full body of the ``CrateDB`` custom resource per
        :class:`kopf.Body`.
    :param old: The old resource body. Required to get the old version.
    """
    old_version = CrateVersion(old["spec"]["cluster"]["version"])
    new_version = CrateVersion(body.spec["cluster"]["version"])

    old_major = _get_major_or_error(old_version)
    new_major = _get_major_or_error(new_version)

    if new_major > old_major:
        # Determine required Lucene version based on the target CrateDB version
        lucene_min_version = LUCENE_MIN_VERSION_MAP.get(new_major)
        if lucene_min_version is None:
            raise kopf.PermanentError(
                f"No Lucene version mapping found for target CrateDB {new_major}. "
            )
        host = await get_host(core, namespace, name)
        password = await get_system_user_password(core, namespace, name)
        conn_factory = connection_factory(host, password)
        connection = conn_factory()

        async with connection as conn:
            async with conn.cursor() as cursor:
                query = f"""
                    SELECT table_name
                    FROM (
                        SELECT table_name,
                            max(min_lucene_version LIKE '{lucene_min_version}')
                            AS needs_reindex
                        FROM sys.shards
                        GROUP BY table_name
                    ) t
                    WHERE needs_reindex = TRUE;
                    """
                await cursor.execute(query)
                rows = await cursor.fetchall()
                logger.info("Found tables that need re-indexing %s", rows)

                if rows:
                    tables = [row[0] for row in rows]
                    raise kopf.PermanentError(
                        f"Tables need re-indexing before upgrade: {', '.join(tables)}"
                    )


async def recreate_internal_tables(
    core: CoreV1Api,
    namespace: str,
    name: str,
    body: kopf.Body,
    old: kopf.Body,
    logger: logging.Logger,
):
    """
    Re-create internal tables that may have been created with an old
    CrateDB major version.

    :param core: An instance of the Kubernetes Core V1 API.
    :param namespace: The Kubernetes namespace for the CrateDB cluster.
    :param name: The name for the ``CrateDB`` custom resource.
    :param body: The full body of the ``CrateDB`` custom resource per
        :class:`kopf.Body`.
    :param old: The old resource body. Required to get the old version.
    """
    old_version = CrateVersion(old["spec"]["cluster"]["version"])
    new_version = CrateVersion(body.spec["cluster"]["version"])

    old_major = _get_major_or_error(old_version)
    new_major = _get_major_or_error(new_version)

    if new_major > old_major:
        host = await get_host(core, namespace, name)
        password = await get_system_user_password(core, namespace, name)
        conn_factory = connection_factory(host, password)
        connection = conn_factory()

        async with connection as conn:
            async with conn.cursor() as cursor:
                for full_table in INTERNAL_TABLES:
                    schema, table = full_table.split(".")

                    await cursor.execute(
                        """
                        SELECT COUNT(*)
                        FROM information_schema.tables
                        WHERE table_schema = %s AND table_name = %s
                        """,
                        (schema, table),
                    )
                    exists = (await cursor.fetchone())[0]

                    if not exists:
                        logger.info("Skipping missing table: %s", full_table)
                        continue

                    try:
                        tmp_table = f"{schema}.tmp_{table}"
                        logger.info("Recreating internal table: %s", full_table)

                        # Step 1: Fetch original CREATE TABLE statement and replace
                        # the table name with a temporary one.
                        await cursor.execute(f"SHOW CREATE TABLE {full_table}")
                        ddl = (await cursor.fetchone())[0]
                        ddl_tmp = re.sub(
                            r'CREATE TABLE IF NOT EXISTS\s+"([^"]+)"\."([^"]+)"',
                            f'CREATE TABLE IF NOT EXISTS "{schema}"."tmp_{table}"',
                            ddl,
                            count=1,
                        )
                        logger.info("Original DDL for %s: %s", full_table, ddl)
                        logger.info("Temporary DDL for %s: %s", tmp_table, ddl_tmp)

                        # Step 2: Create temporary table
                        logger.info("Creating temporary table: %s", tmp_table)
                        await cursor.execute(ddl_tmp)

                        # Step 3: Copy data into temporary table
                        logger.info("Copying data to %s", tmp_table)
                        await cursor.execute(
                            f'INSERT INTO "{schema}"."tmp_{table}" '
                            f'SELECT * FROM "{schema}"."{table}"'
                        )

                        # Step 4: Swap tables atomically
                        logger.info("Swapping %s -> %s", tmp_table, full_table)
                        await cursor.execute(
                            f'ALTER CLUSTER SWAP TABLE "{schema}"."tmp_{table}" '
                            f'TO "{schema}"."{table}"'
                        )

                        # Step 5: Drop temporary table
                        logger.info("Dropping obsolete temporary table: %s", tmp_table)
                        await cursor.execute(
                            f'DROP TABLE IF EXISTS "{schema}"."tmp_{table}"'
                        )
                    except Exception as e:
                        logger.error(
                            "Failed to re-create table %s: %s",
                            full_table,
                            e,
                            exc_info=True,
                        )
                        continue

                logger.info("Successfully re-created all internal tables.")


class UpgradeSubHandler(StateBasedSubHandler):
    @crate.on.error(error_handler=crate.send_update_failed_notification)
    @crate.timeout(timeout=float(config.CLUSTER_UPDATE_TIMEOUT))
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        body: kopf.Body,
        old: kopf.Body,
        logger: logging.Logger,
        **kwargs: Any,
    ):
        async with GlobalApiClient() as api_client:
            apps = AppsV1Api(api_client)
            await upgrade_cluster(apps, namespace, name, body, old, logger)

        await self.send_notification_now(
            logger,
            WebhookEvent.UPGRADE,
            WebhookUpgradePayload(
                old_registry=old["spec"]["cluster"]["imageRegistry"],
                new_registry=body.spec["cluster"]["imageRegistry"],
                old_version=old["spec"]["cluster"]["version"],
                new_version=body.spec["cluster"]["version"],
            ),
            WebhookStatus.IN_PROGRESS,
        )


class BeforeUpgradeSubHandler(StateBasedSubHandler):
    """
    A handler which checks if there are any resources created with
    an old crateDB version.
    """

    @crate.on.error(error_handler=crate.send_update_failed_notification)
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        body: kopf.Body,
        old: kopf.Body,
        logger: logging.Logger,
        **kwargs: Any,
    ):
        async with GlobalApiClient() as api_client:
            core = CoreV1Api(api_client)
            await check_reindexing_tables(core, namespace, name, body, old, logger)


class AfterUpgradeSubHandler(StateBasedSubHandler):
    """
    A handler which depends on ``upgrade`` and ``restart`` having finished
    successfully and sends a success notification of the upgrade process.
    """

    @crate.on.error(error_handler=crate.send_update_failed_notification)
    async def handle(  # type: ignore
        self,
        namespace: str,
        name: str,
        body: kopf.Body,
        old: kopf.Body,
        logger: logging.Logger,
        **kwargs: Any,
    ):
        async with GlobalApiClient() as api_client:
            core = CoreV1Api(api_client)
            await recreate_internal_tables(core, namespace, name, body, old, logger)

        self.schedule_notification(
            WebhookEvent.UPGRADE,
            WebhookUpgradePayload(
                old_registry=old["spec"]["cluster"]["imageRegistry"],
                new_registry=body.spec["cluster"]["imageRegistry"],
                old_version=old["spec"]["cluster"]["version"],
                new_version=body.spec["cluster"]["version"],
            ),
            WebhookStatus.SUCCESS,
        )
