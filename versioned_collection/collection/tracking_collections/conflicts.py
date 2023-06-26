import dataclasses
from typing import Any, Dict

from pymongo.database import Database

from versioned_collection.collection.tracking_collections import (
    _BaseTrackerCollection,
)


class ConflictsCollection(_BaseTrackerCollection):
    """Stores the conflict information produced after merging a branch."""

    _NAME_TEMPLATE = '__conflicts_{}'

    @dataclasses.dataclass
    class SCHEMA:
        destination: Dict[str, Any]
        merged: Dict[str, Any]
        source: Dict[str, Any]
        destination_branch: str
        source_branch: str

    def __init__(
        self, database: Database, parent_collection_name: str, **kwargs
    ) -> None:
        super().__init__(database, parent_collection_name, **kwargs)
        self._exists = None
