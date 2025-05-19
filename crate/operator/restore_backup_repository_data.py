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
    storage_type: BackupStorageType = DEFAULT_BACKUP_STORAGE_TYPE

    # Validate that all fields are provided and are of string type
    def __post_init__(self):
        if not isinstance(self.storage_type, BackupStorageType):
            raise ValueError("storage_type must be a valide backup storage provider")

        if not isinstance(
            self.data, (AzureBackupRepositoryData, S3BackupRepositoryData)
        ):
            raise ValueError(
                "data must be of type AzureBackupRepositoryData or "
                "S3BackupRepositoryData"
            )
        for current_field in fields(self.data):
            if not isinstance(getattr(self.data, current_field.name), str):
                raise ValueError(f"Field `{current_field.name}` must be of string type")

    @staticmethod
    def get_class_from_storage_type(
        storage_type: BackupStorageType,
    ) -> Type["AzureBackupRepositoryData"] | Type["S3BackupRepositoryData"]:
        """
        Retrieve the backup repository data class corresponding
        to the given storage type. Use S3 as a default value.
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
