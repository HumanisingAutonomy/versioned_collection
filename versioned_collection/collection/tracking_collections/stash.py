from __future__ import annotations

from pymongo.collection import Collection
from pymongo.database import Database

from versioned_collection.collection.tracking_collections import (
    _BaseTrackerCollection,
    ModifiedCollection,
)


class StashCollection(_BaseTrackerCollection):
    """Stores the stash data of a collection."""

    _NAME_TEMPLATE = '__stash_{}'

    def __init__(
        self,
        database: Database,
        parent_collection_name: str,
        **kwargs,
    ) -> None:
        super().__init__(database, parent_collection_name, **kwargs)


class StashContainer:
    """Container class managing the stash area.

    When the state of the tracked collection is stashed, the modified
    documents of the tracked collection and the tracking documents from the
    corresponding :class:`ModifiedCollection` are backed-up in two
    :class:`StashCollection` collections. When the stashed data is restored,
    it is transferred back to the two corresponding collections and the stash
    area is cleared.

    """

    def __init__(
        self,
        database: Database,
        parent_collection_name: str,
        **kwargs,
    ) -> None:
        self.main_collection = StashCollection(
            database, parent_collection_name, **kwargs
        )
        self.modified_collection = StashCollection(
            database, f'modified_{parent_collection_name}', **kwargs
        )

    def drop(self):
        self.main_collection.drop()
        self.modified_collection.drop()

    def rename(self, new_name: str, *args, **kwargs):
        self.main_collection.rename(new_name, *args, **kwargs)
        self.modified_collection.rename(f'modified_{new_name}', *args, **kwargs)

    def exists(self) -> bool:
        main_exists = self.main_collection.exists()
        modified_exists = self.modified_collection.exists()
        assert main_exists == modified_exists
        return main_exists

    def stash(
        self,
        main_collection: Collection,
        modified_collection: ModifiedCollection,
    ) -> None:
        """Copy the modified documents and the trackers to the stashing space.

        .. note::
            This does not modify the original collections.

        .. warning::
            This overwrites the existing stashed collections.

        :param main_collection: The tracked versioned collection.
        :param modified_collection: The collection that tracks the ids of the
            modified documents, i.e., ``__modified_<tracked_collection_name>``.
        """
        self.modified_collection.drop()
        self.main_collection.drop()

        modified_collection.aggregate(
            [{"$match": {}}, {"$out": self.modified_collection.name}]
        )
        ids = modified_collection.get_unique_modified_document_ids()
        main_collection.aggregate([
            {"$match": {'_id': {"$in": ids}}},
            {"$out": self.main_collection.name},
        ])

    def stash_apply(
        self,
        main_collection: Collection,
        modified_collection: ModifiedCollection,
    ) -> None:
        """Apply the stash to restore the main collection.

        :param main_collection: The tracked versioned collection
        :param modified_collection: The collection that tracks the ids of the
            modified documents, i.e., ``__modified_<tracked_collection_name>``.
        """
        ids = next(
            self.modified_collection.aggregate([
                {"$group": {'_id': 0, 'ids': {"$addToSet": "$id"}}},
            ])
        )['ids']

        existing_ids = main_collection.find(
            {'_id': {"$in": ids}},
            projection={'_id': True},
        )
        existing_ids = [d['_id'] for d in existing_ids]
        if len(existing_ids):
            self.modified_collection.update_many(
                filter={'id': {"$in": existing_ids}, 'op': 'i'},
                update={"$set": {'op': 'u'}},
            )
        self.modified_collection.aggregate(
            [{"$match": {}}, {"$out": modified_collection.name}]
        )

        main_collection.delete_many({'_id': {"$in": ids}})
        main_collection.insert_many(list(self.main_collection.find({})))

        self.drop()
