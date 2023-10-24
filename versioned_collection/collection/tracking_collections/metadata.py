import dataclasses
from copy import copy
from typing import Optional

from pymongo.database import Database

from versioned_collection.collection.tracking_collections import (
    _BaseTrackerCollection,
)


class MetadataCollection(_BaseTrackerCollection):
    """Stores metadata about the current state of the target collection.

    This collection keeps track of the current version of the target
    versioned collection, i.e., its version and branch, and information about
    the 'head' branch pointer, which specifies whether unregistered changes
    were made since the last registered version of the collection and whether
    the head pointer is detached, i.e., we are not checked out to a branch (
    or more specifically, to the latest registered version on a branch).

    """

    _NAME_TEMPLATE = '__metadata_{}'

    @dataclasses.dataclass
    class SCHEMA:
        current_version: int
        current_branch: str
        detached: bool
        changed: bool
        has_stash: bool
        has_conflicts: bool

    def __init__(
        self, database: Database, parent_collection_name: str, **kwargs
    ) -> None:
        super().__init__(database, parent_collection_name, **kwargs)
        self._metadata: Optional[MetadataCollection.SCHEMA] = None

    @property
    def metadata(self) -> SCHEMA:
        if self._metadata is None:
            md = self.find_one({}, projection={'_id': False})
            self._metadata = self.SCHEMA(**md)
        return self._metadata

    @metadata.setter
    def metadata(self, metadata: SCHEMA):
        if self._metadata == metadata:
            return
        self._metadata = metadata
        self.find_one_and_replace(
            filter={}, replacement=self._metadata.__dict__
        )

    def set_metadata(
        self,
        current_version: Optional[int] = None,
        current_branch: Optional[str] = None,
        detached: Optional[bool] = None,
        changed: Optional[bool] = None,
        has_stash: Optional[bool] = None,
        has_conflicts: Optional[bool] = None,
    ) -> None:
        """Set some or all of the metadata attributes."""
        metadata = copy(self.metadata)

        if current_version is not None:
            metadata.current_version = current_version
        if current_branch is not None:
            metadata.current_branch = current_branch
        if detached is not None:
            metadata.detached = detached
        if changed is not None:
            metadata.changed = changed
        if has_stash is not None:
            metadata.has_stash = has_stash
        if has_conflicts is not None:
            metadata.has_conflicts = has_conflicts
        self.metadata = metadata

    def build(self) -> bool:
        """Build this collection on the database.

        :return: ``True`` if the collection was successfully built, ``False``
            otherwise.
        """
        if self.exists():
            return False

        self._metadata = self.SCHEMA(
            current_version=0,
            current_branch='main',
            detached=False,
            changed=False,
            has_stash=False,
            has_conflicts=False,
        )

        self.insert_one(self._metadata.__dict__)
        return True
