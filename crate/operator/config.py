import logging
import os
from typing import List, Optional

import bitmath

from crate.operator.exceptions import ConfigurationError

UNDEFINED = object()


class Config:
    """
    The central configuration hub for the operator.

    To access the config from another module, import
    :data:`crate.operator.config.config` and access its attributes.
    """

    #: The Docker image that contians scripts to run cluster backups.
    CLUSTER_BACKUP_IMAGE: str

    #: The volume size for the ``PersistentVolume`` that is used as a storage
    #: location for Java heap dumps.
    DEBUG_VOLUME_SIZE: bitmath.Byte = bitmath.GiB(256)

    #: The Kubernetes storage class name for the ``PersistentVolume`` that is
    #: used as a storage location for Java heap dumps.
    DEBUG_VOLUME_STORAGE_CLASS: str = "crate-local"

    #: A list of image pull secrets. Separate names by ``,``.
    IMAGE_PULL_SECRETS: Optional[List[str]] = None

    #: JMX exporter version
    JMX_EXPORTER_VERSION: str

    #: The path the Kubernetes configuration to use.
    KUBECONFIG: Optional[str] = None

    #: The log level to use for all CrateDB operator related log messages.
    LOG_LEVEL: str = "INFO"

    #: Enable several testing behaviors, such as relaxed pod anti-affinity to
    #: allow for easier testing in smaller Kubernetes clusters.
    TESTING: bool = False

    def __init__(self, *, prefix: str):
        self._prefix = prefix

    def load(self):
        self.CLUSTER_BACKUP_IMAGE = self.env("CLUSTER_BACKUP_IMAGE")

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
        if self.KUBECONFIG is not None and not os.path.exists(self.KUBECONFIG):
            raise ConfigurationError(
                f"The Kubernetes config file '{self.KUBECONFIG}' does not exist."
            )

        self.LOG_LEVEL = self.env("LOG_LEVEL", default=self.LOG_LEVEL)
        log = logging.getLogger("crate")
        log.setLevel(logging.getLevelName(self.LOG_LEVEL))

        testing = self.env("TESTING", default=str(self.TESTING))
        self.TESTING = testing.lower() == "true"

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
                # https://docs.python.org/3.8/reference/simple_stmts.html#the-raise-statement
                raise ConfigurationError(
                    f"Required environment variable '{full_name}' is not set."
                ) from None
            return default


#: The global instance of the CrateDB operator config
config = Config(prefix="CRATEDB_OPERATOR_")
