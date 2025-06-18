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
import os
from typing import List, Optional

import bitmath
from kubernetes_asyncio.config.kube_config import ENV_KUBECONFIG_PATH_SEPARATOR

from crate.operator.constants import CloudProvider
from crate.operator.exceptions import ConfigurationError

UNDEFINED = object()


class Config:
    """
    The central configuration hub for the operator.

    To access the config from another module, import
    :data:`crate.operator.config.config` and access its attributes.
    """

    #: Time in seconds for which the operator will continue and wait to
    #: bootstrap a cluster. Once this threshold has passed, a bootstrapping is
    #: considered failed
    BOOTSTRAP_TIMEOUT: int = 1800

    #: Time in seconds between the retries when bootstrapping the cluster.
    #: This can be safely lowered if the k8s environment is quick to act and
    #: the pods are quick to start up.
    BOOTSTRAP_RETRY_DELAY: Optional[int] = 60

    #: Delay between health checks when waiting for a cluster to become ready.
    #: This can be safely lowered if the k8s environment is quick to act and
    #: the pods are quick to start up.
    HEALTH_CHECK_RETRY_DELAY: Optional[int] = 30

    #: When set, enable special handling for the defind cloud provider, e.g. on
    #: AWS pass the availability zone as a CrateDB node attribute.
    CLOUD_PROVIDER: Optional[CloudProvider] = None

    #: The Docker image that contains scripts to run cluster backups.
    CLUSTER_BACKUP_IMAGE: Optional[str] = None

    #: The volume size for the ``PersistentVolume`` that is used as a storage
    #: location for Java heap dumps.
    DEBUG_VOLUME_SIZE: bitmath.Byte = bitmath.GiB(64)

    #: The Kubernetes storage class name for the ``PersistentVolume`` that is
    #: used as a storage location for Java heap dumps.
    DEBUG_VOLUME_STORAGE_CLASS: str = "crate-standard"

    #: A list of image pull secrets. Separate names by ``,``.
    IMAGE_PULL_SECRETS: Optional[List[str]] = None

    #: JMX exporter version
    JMX_EXPORTER_VERSION: str

    #: The path the Kubernetes configuration to use.
    KUBECONFIG: Optional[str] = None

    #: The log level to use for all CrateDB operator related log messages.
    #: WARNING: Settings this to DEBUG or lower may print sensitive information
    #: (i.e. Kubernetes secrets) into the logs. DEBUG or below should not be used
    #: in production environments.
    LOG_LEVEL: str = "INFO"

    #: Time in seconds for which the operator will continue and wait to perform
    #: a rolling restart of a cluster. Once this threshold has passed, a
    #: restart is considered failed.
    ROLLING_RESTART_TIMEOUT = 3600 * 4

    #: Time in seconds for which the operator will continue and wait to scale a
    #: cluster up or down, including deallocating nodes before turning them
    #: off. Once the threshold has passed, a scaling operation is considered
    #: failed.
    SCALING_TIMEOUT = 3600 * 4

    #: Time in seconds for which the operator will continue and wait to perform
    #: an update of a cluster, either scaling a cluster up or down or upgrading
    #: a cluster. Once this threshold has passed, an update is considered
    #: failed.
    CLUSTER_UPDATE_TIMEOUT = 3600 * 4

    #: Time in seconds for which the operator will continue and wait to perform
    #: a check if volume expansion has finished successfully. Once this threshold
    #: has passed, volume expansion is considered failed or not supported by the
    #: StorageClass.
    EXPAND_VOLUME_TIMEOUT = 1800

    #: Time in seconds for which the operator will continue and wait to restore
    #: a snapshot. Once the threshold has passed, a restore operation is considered
    #: failed.
    RESTORE_BACKUP_TIMEOUT = 3600 * 24

    #: Time in seconds for which the operator will continue and wait to perform
    #: checks before and after a cluster update. Once the threshold has passed,
    #: an update is considered failed.
    BEFORE_UPDATE_TIMEOUT = 3600 * 24
    AFTER_UPDATE_TIMEOUT = 3600

    #: Do not scale down cluster when performing storage expansion.
    #: The underlying infrastructure must support this - i.e. Azure or AWS CSI volumes.
    NO_DOWNTIME_STORAGE_EXPANSION: bool = False

    #: Enable several testing behaviors, such as relaxed pod anti-affinity to
    #: allow for easier testing in smaller Kubernetes clusters.
    TESTING: bool = False

    #: Allows running tests in parallel. If enabled, filters the CrateDB resources
    #: by the PID in which they were created, allowing multiple operators to run
    #: in parallel.
    PARALLEL_TESTING: bool = False

    #: HTTP Basic Auth password for web requests made to :attr:`WEBHOOK_URL`.
    WEBHOOK_PASSWORD: Optional[str] = None

    #: Full URL where the operator will send HTTP POST requests to when certain
    #: events occured.
    WEBHOOK_URL: Optional[str] = None

    #: HTTP Basic Auth username for web requests made to :attr:`WEBHOOK_URL`.
    WEBHOOK_USERNAME: Optional[str] = None

    #: Which table are the running jobs stored in. This is only changed in tests.
    JOBS_TABLE: str = "sys.jobs"

    #: From which version onwards CrateDB gateway settings `expected_data_nodes`
    #: and `recover_after_data_nodes` must be used instead of `expected_nodes`
    #: and `recover_after_nodes`
    GATEWAY_SETTINGS_DATA_NODES_VERSION: str = "4.7.0"

    #: From which version onwards CrateDB supports JWT authentication
    CRATEDB_JWT_AUTH_VERSION: str = "5.7.2"

    #: From which version onwards CrateDB supports global JWT config
    CRATEDB_JWT_GLOBAL_CONFIG_VERSION: str = "5.10.1"

    #: Interval in seconds for which the operator will ping CrateDBs for their
    #: current health.
    CRATEDB_STATUS_CHECK_INTERVAL: Optional[int] = 60

    #: The port on which prometheus exposes metrics
    PROMETHEUS_PORT: int = 8080

    #: The sql_exporter image to use
    SQL_EXPORTER_IMAGE: str = "burningalchemist/sql_exporter:0.18.0"

    #: Name of the secret containing credentials to access the source
    #: backup when restoring a snapshot.
    RESTORE_BACKUP_SECRET_NAME = "restore-from-backup-{name}"

    #: Grand Central Sentry DSN
    GC_SENTRY_DSN: Optional[str] = None

    #: Delay before re-enabling cronjobs
    RE_ENABLING_CRONJOB_DELAY = 3600

    def __init__(self, *, prefix: str):
        self._prefix = prefix

    def load(self):
        bootstrap_timeout = self.env(
            "BOOTSTRAP_TIMEOUT", default=str(self.BOOTSTRAP_TIMEOUT)
        )
        try:
            self.BOOTSTRAP_TIMEOUT = int(bootstrap_timeout)
        except ValueError:
            raise ConfigurationError(
                f"Invalid {self._prefix}BOOTSTRAP_TIMEOUT="
                f"'{bootstrap_timeout}'. Needs to be a positive integer or 0."
            )
        if self.BOOTSTRAP_TIMEOUT < 0:
            raise ConfigurationError(
                f"Invalid {self._prefix}BOOTSTRAP_TIMEOUT="
                f"'{bootstrap_timeout}'. Needs to be a positive integer or 0."
            )

        bootstrap_delay = self.env(
            "BOOTSTRAP_RETRY_DELAY", default=str(self.BOOTSTRAP_RETRY_DELAY)
        )
        try:
            self.BOOTSTRAP_RETRY_DELAY = int(bootstrap_delay)
        except ValueError:
            raise ConfigurationError(
                f"Invalid {self._prefix}BOOTSTRAP_RETRY_DELAY="
                f"'{bootstrap_timeout}'. Needs to be a positive integer."
            )

        healthcheck_delay = self.env(
            "HEALTH_CHECK_RETRY_DELAY", default=str(self.HEALTH_CHECK_RETRY_DELAY)
        )
        try:
            self.HEALTH_CHECK_RETRY_DELAY = int(healthcheck_delay)
        except ValueError:
            raise ConfigurationError(
                f"Invalid {self._prefix}HEALTH_CHECK_RETRY_DELAY="
                f"'{bootstrap_timeout}'. Needs to be a positive integer."
            )

        cloud_provider = self.env("CLOUD_PROVIDER", default=self.CLOUD_PROVIDER)
        if cloud_provider is not None:
            try:
                self.CLOUD_PROVIDER = CloudProvider(cloud_provider)
            except ValueError:
                allowed = ", ".join(CloudProvider.__members__.values())
                raise ConfigurationError(
                    f"Invalid {self._prefix}CLOUD_PROVIDER="
                    f"'{cloud_provider}'. Needs to be of {allowed}."
                )

        self.CLUSTER_BACKUP_IMAGE = self.env(
            "CLUSTER_BACKUP_IMAGE", default=self.CLUSTER_BACKUP_IMAGE
        )

        debug_volume_size = self.env(
            "DEBUG_VOLUME_SIZE", default=str(self.DEBUG_VOLUME_SIZE)
        )
        try:
            self.DEBUG_VOLUME_SIZE = bitmath.parse_string(debug_volume_size)
        except ValueError:
            raise ConfigurationError(
                f"Invalid {self._prefix}DEBUG_VOLUME_SIZE='{debug_volume_size}'."
            )

        self.DEBUG_VOLUME_STORAGE_CLASS = self.env(
            "DEBUG_VOLUME_STORAGE_CLASS", default=self.DEBUG_VOLUME_STORAGE_CLASS
        )

        secrets = self.env("IMAGE_PULL_SECRETS", default=self.IMAGE_PULL_SECRETS)
        if secrets is not None:
            self.IMAGE_PULL_SECRETS = [
                s for s in (secret.strip() for secret in secrets.split(",")) if s
            ]

        self.JMX_EXPORTER_VERSION = self.env("JMX_EXPORTER_VERSION")

        self.KUBECONFIG = self.env("KUBECONFIG", default=self.KUBECONFIG)
        if self.KUBECONFIG is not None:
            # When the CRATEDB_OPERATOR_KUBECONFIG env var is set we need to
            # ensure that KUBECONFIG env var is set to the same value for
            # PyKube login of the Kopf framework to work correctly.
            os.environ["KUBECONFIG"] = self.KUBECONFIG
        else:
            self.KUBECONFIG = os.getenv("KUBECONFIG")
        if self.KUBECONFIG is not None:
            for path in self.KUBECONFIG.split(ENV_KUBECONFIG_PATH_SEPARATOR):
                if not os.path.exists(path):
                    raise ConfigurationError(
                        "The KUBECONFIG environment variable contains a path "
                        f"'{path}' that does not exist."
                    )

        self.LOG_LEVEL = self.env("LOG_LEVEL", default=self.LOG_LEVEL)
        level = logging.getLevelName(self.LOG_LEVEL)
        for logger_name in ("", "crate", "kopf", "kubernetes_asyncio"):
            logger = logging.getLogger(logger_name)
            logger.setLevel(level)

        rolling_restart_timeout = self.env(
            "ROLLING_RESTART_TIMEOUT", default=str(self.ROLLING_RESTART_TIMEOUT)
        )
        try:
            self.ROLLING_RESTART_TIMEOUT = int(rolling_restart_timeout)
        except ValueError:
            raise ConfigurationError(
                f"Invalid {self._prefix}ROLLING_RESTART_TIMEOUT="
                f"'{rolling_restart_timeout}'. Needs to be a positive integer or 0."
            )
        if self.ROLLING_RESTART_TIMEOUT < 0:
            raise ConfigurationError(
                f"Invalid {self._prefix}ROLLING_RESTART_TIMEOUT="
                f"'{rolling_restart_timeout}'. Needs to be a positive integer or 0."
            )

        scaling_timeout = self.env("SCALING_TIMEOUT", default=str(self.SCALING_TIMEOUT))
        try:
            self.SCALING_TIMEOUT = int(scaling_timeout)
        except ValueError:
            raise ConfigurationError(
                f"Invalid {self._prefix}SCALING_TIMEOUT="
                f"'{scaling_timeout}'. Needs to be a positive integer or 0."
            )
        if self.SCALING_TIMEOUT < 0:
            raise ConfigurationError(
                f"Invalid {self._prefix}SCALING_TIMEOUT="
                f"'{scaling_timeout}'. Needs to be a positive integer or 0."
            )

        before_update_timeout = self.env(
            "BEFORE_UPDATE_TIMEOUT", default=str(self.BEFORE_UPDATE_TIMEOUT)
        )
        try:
            self.BEFORE_UPDATE_TIMEOUT = int(before_update_timeout)
        except ValueError:
            raise ConfigurationError(
                f"Invalid {self._prefix}BEFORE_UPDATE_TIMEOUT="
                f"'{before_update_timeout}'. Needs to be a positive integer or 0."
            )
        if self.BEFORE_UPDATE_TIMEOUT < 0:
            raise ConfigurationError(
                f"Invalid {self._prefix}BEFORE_UPDATE_TIMEOUT="
                f"'{before_update_timeout}'. Needs to be a positive integer or 0."
            )

        after_update_timeout = self.env(
            "AFTER_UPDATE_TIMEOUT", default=str(self.AFTER_UPDATE_TIMEOUT)
        )
        try:
            self.AFTER_UPDATE_TIMEOUT = int(after_update_timeout)
        except ValueError:
            raise ConfigurationError(
                f"Invalid {self._prefix}AFTER_UPDATE_TIMEOUT="
                f"'{after_update_timeout}'. Needs to be a positive integer or 0."
            )
        if self.AFTER_UPDATE_TIMEOUT < 0:
            raise ConfigurationError(
                f"Invalid {self._prefix}AFTER_UPDATE_TIMEOUT="
                f"'{after_update_timeout}'. Needs to be a positive integer or 0."
            )

        re_enabling_cronjob_delay = self.env(
            "RE_ENABLING_CRONJOB_DELAY", default=str(self.RE_ENABLING_CRONJOB_DELAY)
        )
        try:
            self.RE_ENABLING_CRONJOB_DELAY = int(re_enabling_cronjob_delay)
        except ValueError:
            raise ConfigurationError(
                f"Invalid {self._prefix}RE_ENABLING_CRONJOB_DELAY="
                f"'{re_enabling_cronjob_delay}'. Needs to be a positive integer or 0."
            )
        if self.RE_ENABLING_CRONJOB_DELAY < 0:
            raise ConfigurationError(
                f"Invalid {self._prefix}RE_ENABLING_CRONJOB_DELAY="
                f"'{re_enabling_cronjob_delay}'. Needs to be a positive integer or 0."
            )

        testing = self.env("TESTING", default=str(self.TESTING))
        self.TESTING = testing.lower() == "true"

        testing = self.env("PARALLEL_TESTING", default=str(self.PARALLEL_TESTING))
        self.PARALLEL_TESTING = testing.lower() == "true"

        self.WEBHOOK_PASSWORD = self.env(
            "WEBHOOK_PASSWORD", default=self.WEBHOOK_PASSWORD
        )
        self.WEBHOOK_URL = self.env("WEBHOOK_URL", default=self.WEBHOOK_URL)
        self.WEBHOOK_USERNAME = self.env(
            "WEBHOOK_USERNAME", default=self.WEBHOOK_USERNAME
        )
        self.JOBS_TABLE = self.env("JOBS_TABLE", default=self.JOBS_TABLE)
        self.SQL_EXPORTER_IMAGE = self.env(
            "SQL_EXPORTER_IMAGE", default=self.SQL_EXPORTER_IMAGE
        )

        self.GC_SENTRY_DSN = self.env("GC_SENTRY_DSN", default=self.GC_SENTRY_DSN)

        cratedb_status_interval = int(
            self.env(
                "CRATEDB_STATUS_CHECK_INTERVAL",
                default=self.CRATEDB_STATUS_CHECK_INTERVAL,
            )
        )
        try:
            self.CRATEDB_STATUS_CHECK_INTERVAL = int(cratedb_status_interval)
        except ValueError:
            raise ConfigurationError(
                f"Invalid {self._prefix}CRATEDB_STATUS_CHECK_INTERVAL="
                f"'{cratedb_status_interval}'. Needs to be a positive integer or 0."
            )

        prometheus_port = int(
            self.env(
                "PROMETHEUS_PORT",
                default=self.PROMETHEUS_PORT,
            )
        )
        try:
            self.PROMETHEUS_PORT = int(prometheus_port)
        except ValueError:
            raise ConfigurationError(
                f"Invalid {self._prefix}PROMETHEUS_PORT="
                f"'{cratedb_status_interval}'. Needs to be a positive integer."
            )

        update_timeout = self.env(
            "CLUSTER_UPDATE_TIMEOUT", default=str(self.CLUSTER_UPDATE_TIMEOUT)
        )
        try:
            self.CLUSTER_UPDATE_TIMEOUT = int(update_timeout)
        except ValueError:
            raise ConfigurationError(
                f"Invalid {self._prefix}CLUSTER_UPDATE_TIMEOUT="
                f"'{update_timeout}'. Needs to be a positive integer or 0."
            )
        if self.CLUSTER_UPDATE_TIMEOUT < 0:
            raise ConfigurationError(
                f"Invalid {self._prefix}CLUSTER_UPDATE_TIMEOUT="
                f"'{update_timeout}'. Needs to be a positive integer or 0."
            )

        expand_volume_timeout = self.env(
            "EXPAND_VOLUME_TIMEOUT", default=str(self.EXPAND_VOLUME_TIMEOUT)
        )
        try:
            self.EXPAND_VOLUME_TIMEOUT = int(expand_volume_timeout)
        except ValueError:
            raise ConfigurationError(
                f"Invalid {self._prefix}EXPAND_VOLUME_TIMEOUT="
                f"'{expand_volume_timeout}'. Needs to be a positive integer or 0."
            )
        if self.EXPAND_VOLUME_TIMEOUT < 0:
            raise ConfigurationError(
                f"Invalid {self._prefix}EXPAND_VOLUME_TIMEOUT="
                f"'{expand_volume_timeout}'. Needs to be a positive integer or 0."
            )
        expansion = self.env(
            "NO_DOWNTIME_STORAGE_EXPANSION",
            default=str(self.NO_DOWNTIME_STORAGE_EXPANSION),
        )
        self.NO_DOWNTIME_STORAGE_EXPANSION = expansion.lower() == "true"

        self.RESTORE_BACKUP_SECRET_NAME = self.env(
            "RESTORE_BACKUP_SECRET_NAME", default=self.RESTORE_BACKUP_SECRET_NAME
        )

        restore_timeout = self.env(
            "RESTORE_BACKUP_TIMEOUT", default=str(self.RESTORE_BACKUP_TIMEOUT)
        )
        try:
            self.RESTORE_BACKUP_TIMEOUT = int(restore_timeout)
        except ValueError:
            raise ConfigurationError(
                f"Invalid {self._prefix}RESTORE_BACKUP_TIMEOUT="
                f"'{restore_timeout}'. Needs to be a positive integer or 0."
            )
        if self.RESTORE_BACKUP_TIMEOUT < 0:
            raise ConfigurationError(
                f"Invalid {self._prefix}RESTORE_BACKUP_TIMEOUT="
                f"'{restore_timeout}'. Needs to be a positive integer or 0."
            )

    def env(self, name: str, *, default=UNDEFINED) -> str:
        """
        Retrieve the environment variable ``name`` or fall-back to its default
        if provided. If no default is provided, a :exc:`~.ConfigurationError` is
        raised.
        """
        full_name = f"{self._prefix}{name}"
        try:
            return os.environ[full_name]
        except KeyError:
            if default is UNDEFINED:
                # raise from None - so that the traceback of the original
                # exception (KeyError) is not printed
                # https://docs.python.org/3.12/reference/simple_stmts.html#the-raise-statement
                raise ConfigurationError(
                    f"Required environment variable '{full_name}' is not set."
                ) from None
            return default


#: The global instance of the CrateDB operator config
config = Config(prefix="CRATEDB_OPERATOR_")
