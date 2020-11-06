from typing import Collection, Iterable, Optional

import kopf
from kopf.structs import dicts


class ReadManyWriteOneDiffBaseStorage(kopf.DiffBaseStorage):
    def __init__(self, storages: Collection[kopf.AnnotationsDiffBaseStorage]) -> None:
        super().__init__()
        self.storages = storages

    def build(
        self,
        *,
        body: kopf.Body,
        extra_fields: Optional[Iterable[dicts.FieldSpec]] = None,
    ) -> kopf.BodyEssence:
        essence = super().build(body=body, extra_fields=extra_fields)
        for storage in self.storages:
            # Let the individual stores to also clean the essence from their
            # own fields. For this, assume the the previous essence _is_ the
            # body (what's left of it).
            essence = storage.build(body=kopf.Body(essence), extra_fields=extra_fields)
        return essence

    def fetch(self, *, body: kopf.Body,) -> Optional[kopf.BodyEssence]:
        for storage in self.storages:
            content = storage.fetch(body=body)
            if content is not None:
                return content
        return None

    def store(
        self, *, body: kopf.Body, patch: kopf.Patch, essence: kopf.BodyEssence,
    ) -> None:
        storages_iter = iter(self.storages)
        first = next(storages_iter)
        first.store(body=body, patch=patch, essence=essence)
        # For all remaining storages, clear any of their annotations diff
        for storage in storages_iter:
            for full_key in storage.make_keys(storage.key):
                patch.metadata.annotations[full_key] = None


class ReadManyWriteOneProgressStorage(kopf.ProgressStorage):
    def __init__(self, storages: Collection[kopf.AnnotationsProgressStorage]) -> None:
        super().__init__()
        self.storages = storages

    def fetch(self, *, key: str, body: kopf.Body,) -> Optional[kopf.ProgressRecord]:
        for storage in self.storages:
            content = storage.fetch(key=key, body=body)
            if content is not None:
                return content
        return None

    def store(
        self,
        *,
        key: str,
        record: kopf.ProgressRecord,
        body: kopf.Body,
        patch: kopf.Patch,
    ) -> None:
        storages_iter = iter(self.storages)
        first = next(storages_iter)
        first.store(key=key, record=record, body=body, patch=patch)
        # Only store the progress in one annotation and clear all others
        for storage in storages_iter:
            storage.purge(key=key, body=body, patch=patch)

    def purge(self, *, key: str, body: kopf.Body, patch: kopf.Patch,) -> None:
        for storage in self.storages:
            storage.purge(key=key, body=body, patch=patch)

    def touch(
        self, *, body: kopf.Body, patch: kopf.Patch, value: Optional[str],
    ) -> None:
        storages_iter = iter(self.storages)
        first = next(storages_iter)
        first.touch(body=body, patch=patch, value=value)
        for storage in self.storages:
            storage.purge(key=storage.touch_key, body=body, patch=patch)

    def clear(self, *, essence: kopf.BodyEssence) -> kopf.BodyEssence:
        for storage in self.storages:
            essence = storage.clear(essence=essence)
        return essence
