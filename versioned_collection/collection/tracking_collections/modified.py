from __future__ import annotations

import dataclasses
from typing import Dict, List, TypedDict, Literal

from bson import ObjectId
from pymongo.database import Database

from versioned_collection.collection.tracking_collections import (
    _BaseTrackerCollection,
)


class ModifiedTracker(TypedDict):
    """Representation for a modified document and its list of trackers."""

    _id: ObjectId
    tracker_ids: List[ObjectId]


class ModifiedCollection(_BaseTrackerCollection):
    """Stores references to the documents that were modified and untracked.

    A temporary collection created upon the initialisation for tracking of a
    target collection. The documents in this collection store references to the
    documents that have been modified with respect to the latest registered
    version and the operation that modified the document. When a new version of
    the target collection is registered all the documents of this collection
    are removed.

    A tracking document is added to this collection each time a document in
    the target collection is modified. This allows correctly registering
    versions of the target collection when large volumes of unacknowledged
    writes are performed.

    The set of all tracking documents in this collection forms a total order,
    induced by the ordering property of ObjectIds. The documents that track the
    changes of a target document are ordered by the order of occurrence of the
    events that modified the target document, which can be restored by
    comparing the tracking document ids, i.e., the ``_id`` field.
    """

    _NAME_TEMPLATE = '__modified_{}'

    @dataclasses.dataclass
    class SCHEMA:
        # The id of document itself. This is needed to avoid
        # low probability races when registering new versions after a large
        # volume of unacknowledged write operations
        _id: ObjectId
        # The id of the modified document. This should technically be `Any`,
        # since all types are valid as id types.
        id: ObjectId
        # The operation that modified the document, such as insert ('i'),
        # update or replace ('u') or delete ('d')
        op: str

    def __init__(
        self, database: Database, parent_collection_name: str, **kwargs
    ) -> None:
        super().__init__(database, parent_collection_name, **kwargs)

    def has_changes(self) -> bool:
        return self.count_documents({}) > 0

    def get_modified_trackers(self) -> List[ModifiedTracker]:
        """Get the modified document ids and the ids of the trackers.

        :return: A list of documents containing the ids of the modified
            documents in the tracked collection and the ids of the trackers
            in this collection, grouped by the modified document ids.
        """
        docs = self.aggregate([
            {"$group": {'_id': "$id", 'tracker_ids': {"$push": "$_id"}}},
        ])
        return list(docs)

    def get_modified_document_ids_by_operation(
        self,
    ) -> Dict[Literal['i', 'd', 'u'], List[ObjectId]]:
        """Return the document ids grouped by the operation type.

        :return: The list of ids of the modified documents grouped by the
            type of the modifying operation. The valid types are ``'i'`` for
            inserts, ``'d'`` for deletes and ``'u'`` for updates and
            replacements.
        """
        docs = list(
            self.aggregate([
                {"$group": {'_id': "$op", 'ids': {"$addToSet": "$id"}}},
                {
                    "$replaceRoot": {
                        'newRoot': {
                            "$arrayToObject": [[{'k': "$_id", 'v': "$ids"}]]
                        }
                    }
                },
                # merge the results into a single document
                {
                    "$group": {
                        '_id': 0,
                        'aggregated_ops': {"$push": "$$ROOT"},
                    }
                },
                {
                    "$replaceRoot": {
                        'newRoot': {"$mergeObjects": "$aggregated_ops"}
                    }
                },
                {"$project": {'_id': False}},
            ])
        )
        return dict() if len(docs) == 0 else docs[0]

    def get_unique_modified_document_ids(self) -> List[ObjectId]:
        """Get the unique ids of the modified documents."""
        result = self.aggregate([
            {"$group": {'_id': 0, 'ids': {"$addToSet": "$id"}}},
        ])
        return next(result)['ids']

    def delete_modified(self, ids: List[ObjectId]) -> None:
        """Delete the tracked documents from this collection.

        .. note::
            This deletes only one of the trackers documents. A document
            from the tracked collection that has been modified multiple times
            has multiple trackers in this collection.

        :param ids: The ids of the tracker documents.
        """
        self.delete_many({'_id': {'$in': ids}})
