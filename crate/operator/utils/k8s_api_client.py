import asyncio
from typing import List

from kubernetes_asyncio.client import ApiClient


class GlobalApiClient(ApiClient):

    _semaphore = asyncio.Semaphore(10)

    _instance_track: List["GlobalApiClient"] = []
    _instance_creation_track = 0
    _instance_removal_track = 0

    def __init__(self):
        super().__init__()

    @classmethod
    def get_instance_count(cls):
        return len(cls._instance_track)

    @classmethod
    def get_created_instances(cls):
        return cls._instance_creation_track

    @classmethod
    def get_removed_instances(cls):
        return cls._instance_removal_track

    async def __aenter__(self):
        await GlobalApiClient._semaphore.acquire()
        GlobalApiClient._instance_track.append(self)
        GlobalApiClient._instance_creation_track += 1
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        GlobalApiClient._semaphore.release()
        GlobalApiClient._instance_track.remove(self)
        GlobalApiClient._instance_removal_track += 1
        await super().__aexit__(exc_type, exc_value, traceback)
