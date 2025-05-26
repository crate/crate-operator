from dataclasses import dataclass, field, fields
from typing import Type

from crate.operator.constants import (
    DEFAULT_BACKUP_STORAGE_PROVIDER,
    BackupStorageProvider,
)

REPOSITORY_TYPE_MAP = {
    BackupStorageProvider.AZURE_BLOB: "azure",
    BackupStorageProvider.AWS: "s3",
}


@dataclass
class AzureBackupRepositoryData:
    accountKey: str = field(metadata={"query_param": "key"})
    accountName: str = field(metadata={"query_param": "account"})
    container: str = field(metadata={"query_param": "container"})


@dataclass
class AwsBackupRepositoryData:
    accessKeyId: str = field(metadata={"query_param": "access_key"})
    basePath: str = field(metadata={"query_param": "base_path"})
    bucket: str = field(metadata={"query_param": "bucket"})
    secretAccessKey: str = field(metadata={"query_param": "secret_key"})


@dataclass
class BackupRepositoryData:
    data: AzureBackupRepositoryData | AwsBackupRepositoryData
    backup_provider: BackupStorageProvider = DEFAULT_BACKUP_STORAGE_PROVIDER

    # Validate that all fields are provided and are of string type
    def __post_init__(self):
        if not isinstance(self.backup_provider, BackupStorageProvider):
            raise ValueError("backup_provider must be a valide backup storage provider")

        if not isinstance(
            self.data, (AzureBackupRepositoryData, AwsBackupRepositoryData)
        ):
            raise ValueError(
                "data must be of type AzureBackupRepositoryData or "
                "AwsBackupRepositoryData"
            )
        for current_field in fields(self.data):
            value = getattr(self.data, current_field.name)
            if not isinstance(value, str) or not value:
                raise ValueError(
                    f"Field `{current_field.name}` must be a non-empty string"
                )

    @staticmethod
    def get_class_from_backup_provider(
        backup_provider: BackupStorageProvider,
    ) -> Type["AzureBackupRepositoryData"] | Type["AwsBackupRepositoryData"]:
        """
        Retrieve the backup repository data class corresponding
        to the given storage type. Use AWS S3 as a default value.
        """
        if backup_provider == BackupStorageProvider.AZURE_BLOB:
            return AzureBackupRepositoryData
        else:
            return AwsBackupRepositoryData

    @staticmethod
    def get_secrets_keys(backup_provider: BackupStorageProvider) -> list[str]:
        """
        Returns a list of all the secrets keys per provider.
        """
        cls = BackupRepositoryData.get_class_from_backup_provider(backup_provider)
        return [field.name for field in fields(cls)]

    @staticmethod
    def get_repository_type(backup_provider: BackupStorageProvider) -> str:
        """
        Returns the repository type per provider.
        """
        return REPOSITORY_TYPE_MAP[backup_provider]
