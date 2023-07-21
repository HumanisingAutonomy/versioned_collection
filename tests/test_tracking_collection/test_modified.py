from typing import Literal
from unittest.mock import patch

import pymongo.collection
from bson import ObjectId

from tests.test_tracking_collection.in_memory_database import (
    InMemoryDatabaseSetup, build_and_destroy_collection
)
from versioned_collection.collection.tracking_collections import \
    ModifiedCollection


class TestModifiedCollection(InMemoryDatabaseSetup):
    def setUp(self) -> None:
        self.collection = ModifiedCollection(self.database, 'col')

    def test_has_changes_is_false_when_no_documents_in_collection(self):
        with patch.object(
            pymongo.collection.Collection,
            'count_documents'
        ) as mock:
            mock.return_value = 0
            self.assertFalse(self.collection.has_changes())
            mock.assert_called_once()

    def test_collection_has_changes_if_any_at_least_one_doc_in_collection(self):
        with patch.object(
            pymongo.collection.Collection,
            'count_documents'
        ) as mock:
            mock.return_value = 1
            self.assertTrue(self.collection.has_changes())
            mock.return_value = 2
            self.assertTrue(self.collection.has_changes())

    @build_and_destroy_collection
    def test_get_modified_trackers_for_a_document_with_a_single_tracker(self):
        doc_id = ObjectId()
        tracker_id = ObjectId()
        mod = ModifiedCollection.SCHEMA(
            _id=tracker_id,
            id=doc_id,
            op='i'
        )
        self.collection.insert_one(mod.__dict__)

        docs = self.collection.get_modified_trackers()
        self.assertEqual(1, len(docs))
        doc = docs[0]
        self.assertEqual(doc_id, doc['_id'])
        self.assertEqual(1, len(doc['tracker_ids']))
        self.assertEqual(tracker_id, doc['tracker_ids'][0])

    def _setup_one_doc_many_ops(self):
        doc_id = ObjectId()
        tracker_1 = ModifiedCollection.SCHEMA(ObjectId(), doc_id, op='i')
        tracker_2 = ModifiedCollection.SCHEMA(ObjectId(), doc_id, op='d')
        tracker_3 = ModifiedCollection.SCHEMA(ObjectId(), doc_id, op='i')
        tracker_4 = ModifiedCollection.SCHEMA(ObjectId(), doc_id, op='u')
        tracker_5 = ModifiedCollection.SCHEMA(ObjectId(), doc_id, op='u')
        trackers = [tracker_1, tracker_2, tracker_3, tracker_4, tracker_5]

        self.collection.insert_many(t.__dict__ for t in trackers)

        return doc_id, trackers

    @build_and_destroy_collection
    def test_get_modified_trackers_for_a_document_with_multiple_trackers(self):
        doc_id, trackers = self._setup_one_doc_many_ops()

        docs = self.collection.get_modified_trackers()
        self.assertEqual(1, len(docs))
        doc = docs[0]
        self.assertEqual(doc_id, doc['_id'])
        self.assertEqual(5, len(doc['tracker_ids']))
        # check that there is a 1 to 1 correspondence between that was
        # created and what was retrieved
        trackers_to_check = set(t._id for t in trackers)
        for tracker_id in doc['tracker_ids']:
            self.assertIn(tracker_id, trackers_to_check)
            trackers_to_check.remove(tracker_id)

    @build_and_destroy_collection
    def test_get_modified_trackers_for_multiple_documents(self):
        doc_id_1 = ObjectId()
        tracker_1_1 = ModifiedCollection.SCHEMA(ObjectId(), doc_id_1, op='d')

        doc_id_2 = ObjectId()
        tracker_2_1 = ModifiedCollection.SCHEMA(ObjectId(), doc_id_2, op='u')
        tracker_2_2 = ModifiedCollection.SCHEMA(ObjectId(), doc_id_2, op='u')

        expected = [
            {'_id': doc_id_1, 'tracker_ids': [tracker_1_1._id]},
            {'_id': doc_id_2, 'tracker_ids': [tracker_2_1._id, tracker_2_2._id]}
        ]

        self.collection.insert_many([
            tracker_1_1.__dict__, tracker_2_1.__dict__, tracker_2_2.__dict__
        ])

        docs = self.collection.get_modified_trackers()
        self.assertEqual(2, len(docs))
        if docs[0]['_id'] == doc_id_2:
            docs = docs[::-1]
        self.assertEqual(expected, docs)

    @build_and_destroy_collection
    def test_get_modified_document_ids_by_op_with_one_doc_and_one_op(self):
        doc_id = ObjectId()
        op: Literal['d'] = 'd'
        tracker = ModifiedCollection.SCHEMA(ObjectId(), doc_id, op)
        self.collection.insert_one(tracker.__dict__)

        result = self.collection.get_modified_document_ids_by_operation()
        self.assertEqual(1, len(result))
        self.assertIn(op, result)

        ids = result[op]
        self.assertEqual(1, len(ids))
        self.assertEqual(doc_id, ids[0])

    @build_and_destroy_collection
    def test_get_modified_document_ids_by_op_with_one_doc_and_many_ops(self):
        doc_id, _ = self._setup_one_doc_many_ops()

        result = self.collection.get_modified_document_ids_by_operation()
        self.assertEqual(3, len(result))
        self.assertEqual(1, len(result['i']))
        self.assertEqual(doc_id, result['i'][0])
        self.assertEqual(1, len(result['d']))
        self.assertEqual(doc_id, result['d'][0])
        self.assertEqual(1, len(result['u']))
        self.assertEqual(doc_id, result['u'][0])

    @build_and_destroy_collection
    def test_get_modified_document_ids_by_op_with_many_docs_and_many_ops(self):
        doc_id_1, _ = self._setup_one_doc_many_ops()
        doc_id_2, _ = self._setup_one_doc_many_ops()

        doc_id_3 = ObjectId()
        trackers_3 = [ModifiedCollection.SCHEMA(ObjectId(), doc_id_3, 'u')]
        self.collection.insert_one(trackers_3[0].__dict__)

        result = self.collection.get_modified_document_ids_by_operation()
        self.assertEqual(3, len(result))

        self.assertEqual(2, len(result['i']))
        self.assertEqual(2, len(result['d']))
        self.assertEqual(3, len(result['u']))

        doc_ids = {doc_id_1, doc_id_2}
        self.assertEqual(doc_ids, set(result['i']))
        self.assertEqual(doc_ids, set(result['d']))
        doc_ids.add(doc_id_3)
        self.assertEqual(doc_ids, set(result['u']))

    @build_and_destroy_collection
    def test_get_unique_modified_document_ids(self):
        doc_id_1, _ = self._setup_one_doc_many_ops()
        doc_id_2, _ = self._setup_one_doc_many_ops()

        result = set(self.collection.get_unique_modified_document_ids())
        self.assertEqual({doc_id_1, doc_id_2}, result)

    def test_delete_modified(self):
        ids_to_delete = [ObjectId() for _ in range(5)]
        with patch.object(pymongo.collection.Collection, 'delete_many') as mock:
            self.collection.delete_modified(ids_to_delete)
            mock.assert_called_once_with({'_id': {"$in": ids_to_delete}})
