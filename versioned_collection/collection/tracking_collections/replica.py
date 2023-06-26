from pymongo.database import Database

from versioned_collection.collection.tracking_collections import (
    _BaseTrackerCollection,
)


class ReplicaCollection(_BaseTrackerCollection):
    """A snapshot of the latest tracked version of the target collection.

    When the target collection is initialised for versioning, or when changes
    to a previous version of the target collection are tried to be made, i.e.,
    when branching explicitly or implicitly from a previous checked-out
    version, this collection will replicate the state of the target collection.

    The main purpose of a `ReplicaCollection` is to allow the user to
    efficiently perform queries to a tracked collection without any
    performance overhead, allowing to compute the deltas between the latest
    tracked version and the modified version of the target collection when a
    new version of it is registered.

    """

    _NAME_TEMPLATE = '__replica_{}'

    def __init__(
        self, database: Database, parent_collection_name: str, **kwargs
    ) -> None:
        super().__init__(database, parent_collection_name, **kwargs)

    def build(self):
        """Build this collection on the database.

        Upon the creation of this collection a snapshot of the target
        collection is created as well, i.e., this collection will be a
        replica of the target collection at the moment of initialisation.
        """
        self.create_snapshot()

    def create_snapshot(self) -> None:
        # Replicate the data from the target collection to the replica.
        self.database[self._target_collection_name].aggregate(
            [{"$match": {}}, {"$out": self.name}]
        )
