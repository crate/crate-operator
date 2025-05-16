from dataclasses import dataclass, field, fields
from typing import Type

from crate.operator.constants import DEFAULT_BACKUP_STORAGE_TYPE, BackupStorageType


@dataclass
class AzureBackupRepositoryData:
    accountKey: str = field(metadata={"query_param": "key"})
    accountName: str = field(metadata={"query_param": "account"})
    basePath: str = field(metadata={"query_param": "base_path"})
    container: str = field(metadata={"query_param": "container"})


@dataclass
class S3BackupRepositoryData:
    accessKeyId: str = field(metadata={"query_param": "access_key"})
    basePath: str = field(metadata={"query_param": "base_path"})
    bucket: str = field(metadata={"query_param": "bucket"})
    secretAccessKey: str = field(metadata={"query_param": "secret_key"})


@dataclass
class BackupRepositoryData:
    data: AzureBackupRepositoryData | S3BackupRepositoryData
    storage_type: str = DEFAULT_BACKUP_STORAGE_TYPE

    @staticmethod
    def get_class_from_storage_type(
        storage_type: BackupStorageType,
    ) -> Type["AzureBackupRepositoryData"] | Type["S3BackupRepositoryData"]:
        """
        Retrieve the backup repository data class corresponding
        to the given storage type.
        """
        if storage_type == BackupStorageType.AZURE:
            return AzureBackupRepositoryData
        else:
            return S3BackupRepositoryData

    @staticmethod
    def get_secrets_keys(storage_type: BackupStorageType) -> list[str]:
        """
        Returns a list of all the secrets keys per provider.
        """
        cls = BackupRepositoryData.get_class_from_storage_type(storage_type)
        return [field.name for field in fields(cls)]
