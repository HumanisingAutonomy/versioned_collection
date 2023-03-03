from __future__ import annotations

import dataclasses
from collections import defaultdict
from typing import Dict, List, Union

from bson import ObjectId
from pymongo.database import Database

from versioned_collection.collection.tracking_collections import \
    _BaseTrackerCollection


class ModifiedCollection(_BaseTrackerCollection):
    """ Stores references to the documents that were modified and untracked.

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

    def __init__(self,
                 database: Database,
                 parent_collection_name: str,
                 **kwargs
                 ) -> None:
        super().__init__(database, parent_collection_name, **kwargs)

    def has_changes(self) -> bool:
        return self.count_documents({}) > 0

    def find_modified_documents_ids(self) \
            -> List[Dict[str, Union[ObjectId, List[ObjectId]]]]:
        """ Returns the ids of the modified documents.

        :return: A list of documents containing the ids of the modified
            documents in the tracked collection and the ids of the trackers
            in this collection.
        """
        docs = self.aggregate([
            {"$match": {}},
            {"$group": {'_id': "$id", 'tracker_ids': {"$push": "$_id"}}}
        ])
        return list(docs)

    def get_modified_document_ids_by_operation(self) \
            -> Dict[str, List[ObjectId]]:
        """ Returns the document ids grouped by the operation type.

        :return: The list of ids of the modified documents grouped by the
            type of the modifying operation. The valid types are ``'i'`` for
            inserts, ``'d'`` for deletes and ``'u'`` for updates and
            replacements.
        """
        docs = list(self.find({}, projection={'_id': False}))
        docs_per_operation = defaultdict(list)
        for doc in docs:
            docs_per_operation[doc['op']].append(doc['id'])
        return dict(docs_per_operation)

    def delete_modified(self, ids: List[ObjectId]) -> None:
        """ Deletes the tracked documents from this collection.

        .. note::
            This deletes only one of the modification trackers. A document
            from the tracked collection that has been modified multiple times
            has multiple trackers in this collection.

        :param ids: The ids of the tracker documents.
        """
        self.delete_many({'_id': {'$in': ids}})
