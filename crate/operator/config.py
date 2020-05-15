import logging
import os
from typing import Optional

from crate.operator.exceptions import ConfigurationError

UNDEFINED = object()


class Config:
    """
    The central configuration hub for the operator.

    To access the config from another module, import
    :data:`crate.operator.config.config` and access its attributes.
    """

    #: The path the Kubernetes configuration to use.
    KUBECONFIG: Optional[str] = None

    #: The log level to use for all CrateDB operator related log messages.
    LOG_LEVEL: str = "INFO"

    def __init__(self, *, prefix: str):
        self._prefix = prefix

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

    def env(self, name: str, *, default=UNDEFINED) -> str:
        """
        Retrieve the environment variable ``name`` or fall-back to its default
        if provided. If no default is provided, a :exc:`~.ConfigurationError` is
        raised.
        """
        try:
            return os.environ[self._prefix + name]
        except KeyError as e:
            if default is UNDEFINED:
                # raise from None - so that the traceback of the original
                # exception (KeyError) is not printed
                # https://docs.python.org/3.8/reference/simple_stmts.html#the-raise-statement
                raise ConfigurationError(
                    f"Required environment variable '{e}' is not set."
                ) from None
            return default


#: The global instance of the CrateDB operator config
config = Config(prefix="CRATEDB_OPERATOR_")
