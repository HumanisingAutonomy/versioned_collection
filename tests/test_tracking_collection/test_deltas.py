import datetime
from copy import deepcopy
from unittest.mock import patch

import deepdiff
import pymongo
from bson import ObjectId

import versioned_collection.collection.tracking_collections
from tests.test_tracking_collection.in_memory_database import \
    InMemoryDatabaseSetup
from versioned_collection.collection.tracking_collections import \
    DeltasCollection


class TestDeltasCollectionIntegration(InMemoryDatabaseSetup):

    def setUp(self) -> None:
        self.col = DeltasCollection(self.database, 'col')
        self.doc = {
            '_id': ObjectId(),
            'v': 0,
            'a_field': 'a_value'
        }

    def tearDown(self) -> None:
        self.col.drop()

    def test_deltas_build_returns_false_if_collection_already_exists(self):
        col = DeltasCollection(self.database, 'col')

        with patch.object(
            versioned_collection.collection.tracking_collections.DeltasCollection,
            'exists'
        ) as exists_mock:
            exists_mock.return_value = True
            self.assertFalse(col.build())

    def test_add_delta_for_an_unmodified_document_returns_none(self):
        delta_id = self.col.add_delta(
            document_new=self.doc,
            document_old=deepcopy(self.doc),
            document_id=self.doc['_id'],
            collection_version=1,
            branch='main',
            timestamp=datetime.datetime.utcnow(),
            branch_history=[]
        )
        self.assertIsNone(delta_id)

    def test_add_delta_for_the_first_time(self):
        # bson stores date times up to millisecond precision, so chop of the
        # nanoseconds
        timestamp = datetime.datetime.utcnow()
        timestamp = datetime.datetime.fromisoformat(
            timestamp.isoformat(timespec='milliseconds')
        )

        delta_id = self.col.add_delta(
            document_new=self.doc,
            document_old=dict(),
            document_id=self.doc['_id'],
            collection_version=1,
            branch='main',
            timestamp=timestamp,
            branch_history=[(0, 'main')]
        )
        self.assertIsNotNone(delta_id)

        deltas = list(self.col.find({}))

        self.assertEqual(1, len(deltas))
        delta = deltas[0]

        self.assertEqual(delta_id, delta['_id'])
        self.assertEqual(self.doc['_id'], delta['document_id'])
        self.assertEqual(1, delta['collection_version_id'])
        self.assertEqual('main', delta['branch'])
        self.assertEqual(timestamp, delta['timestamp'])
        self.assertIsNone(delta['prev'])
        self.assertEqual([], delta['next'])

        forward_delta = deepdiff.Delta(
            delta['forward'], safe_to_import={'bson.objectid.ObjectId'}
        )
        backward_delta = deepdiff.Delta(
            delta['backward'], safe_to_import={'bson.objectid.ObjectId'}
        )

        self.assertEqual(dict() + forward_delta, self.doc)
        self.assertEqual(self.doc + backward_delta, dict())

    def test_add_delta_with_existing_parent(self):
        pass

    def test_add_first_delta_on_a_new_branch(self):
        pass

    def test_add_delta_with_existing_parent_on_branch(self):
        pass

    def test_add_delta_on_branch_with_unconnected_delta_tree(self):
        # add a document on 2 different branches such that for both branches,
        # the deltas are added after the version corresponding to the
        # intersection of the branches, i.e., the LCA corresponding to the
        # versions for which the deltas are registered.
        pass


class TestDeltasCollectionUnitTests(InMemoryDatabaseSetup):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.col = DeltasCollection(cls.database, 'col')

    def setUp(self) -> None:
        self.doc = {
            '_id': ObjectId(),
            'v': 0,
            'a_field': 'a_value'
        }

    @staticmethod
    def _get_timestamp():
        timestamp = datetime.datetime.utcnow()
        timestamp = datetime.datetime.fromisoformat(
            timestamp.isoformat(timespec='milliseconds')
        )
        return timestamp

    @staticmethod
    def _update_doc(doc, _id=None):
        doc = deepcopy(doc)
        doc['v'] += 1
        if _id is not None:
            doc['_id'] = _id
        return doc

    @staticmethod
    def _get_forward_backward_deltas(doc_old, doc_new):
        forward = deepdiff.Delta(
            deepdiff.DeepDiff(
                doc_old,
                doc_new,
                ignore_order=False,
                report_repetition=False,
            )
        )
        backward = deepdiff.Delta(
            deepdiff.DeepDiff(
                doc_new,
                doc_old,
                ignore_order=False,
                report_repetition=False,
            )
        )
        return forward, backward

    @patch.object(pymongo.collection.Collection, 'find')
    @patch.object(pymongo.collection.Collection, 'update_one')
    @patch.object(pymongo.collection.Collection, 'insert_one')
    @patch.object(pymongo.collection.Collection, 'find_one_and_update')
    def test_add_delta(
        self,
        find_one_and_update_mock,
        insert_one_mock,
        update_one_mock,
        find_mock,
    ):
        find_mock.return_value = []

        delta_id = ObjectId()
        timestamp = self._get_timestamp()
        ret_delta_id = self.col.add_delta(
            document_new=self.doc,
            document_old=dict(),
            document_id=self.doc['_id'],
            collection_version=1,
            branch='main',
            timestamp=timestamp,
            branch_history=[(0, 'main')],
            with_id=delta_id,
        )
        self.assertEqual(delta_id, ret_delta_id)

        forward, backward = self._get_forward_backward_deltas(
            doc_old=dict(), doc_new=self.doc
        )

        delta_doc = dict(
            _id=delta_id,
            document_id=self.doc['_id'],
            collection_version_id=1,
            branch='main',
            timestamp=timestamp,
            forward=forward.dumps(),
            backward=backward.dumps(),
            prev=None,
            next=[],
        )

        find_one_and_update_mock.assert_not_called()
        update_one_mock.assert_not_called()
        insert_one_mock.assert_called_once_with(delta_doc)

    @patch.object(pymongo.collection.Collection, 'find')
    @patch.object(pymongo.collection.Collection, 'update_one')
    @patch.object(pymongo.collection.Collection, 'insert_one')
    @patch.object(pymongo.collection.Collection, 'find_one_and_update')
    def test_add_delta_with_existing_parent(
        self,
        find_one_and_update_mock,
        insert_one_mock,
        update_one_mock,
        find_mock,
    ):
        forward, backward = self._get_forward_backward_deltas(dict(), self.doc)

        parent_delta = dict(
            _id=ObjectId(),
            document_id=self.doc['_id'],
            collection_version_id=1,
            branch='main',
            timestamp=self._get_timestamp(),
            forward=forward.dumps(),
            backward=backward.dumps(),
            prev=None,
            next=[],
        )
        find_mock.return_value = [deepcopy(parent_delta)]

        doc_old = self.doc
        doc_new = self._update_doc(doc_old)

        delta_id = ObjectId()
        timestamp = self._get_timestamp()
        ret_delta_id = self.col.add_delta(
            document_new=doc_new,
            document_old=doc_old,
            document_id=doc_new['_id'],
            collection_version=2,
            branch='main',
            timestamp=timestamp,
            branch_history=[(1, 'main'), (0, 'main')],
            with_id=delta_id,
        )
        self.assertEqual(delta_id, ret_delta_id)

        update_one_mock.assert_not_called()

        forward, backward = self._get_forward_backward_deltas(doc_old, doc_new)
        delta_doc = dict(
            _id=delta_id,
            document_id=self.doc['_id'],
            collection_version_id=2,
            branch='main',
            timestamp=timestamp,
            forward=forward.dumps(),
            backward=backward.dumps(),
            prev=parent_delta['_id'],
            next=[],
        )
        insert_one_mock.assert_called_once_with(delta_doc)

        find_one_and_update_mock.assert_called_once_with(
            filter={'_id': parent_delta['_id']},
            update={"$set": {"next": [delta_id]}},
        )

    @patch.object(pymongo.collection.Collection, 'find')
    @patch.object(pymongo.collection.Collection, 'update_one')
    @patch.object(pymongo.collection.Collection, 'insert_one')
    @patch.object(pymongo.collection.Collection, 'find_one_and_update')
    def test_update_already_added_delta(
        self,
        find_one_and_update_mock,
        insert_one_mock,
        update_one_mock,
        find_mock,
    ):
        # This is a bit awkward, but it can happen in the rare situation the
        # change streams (and the listener) take too long to process the
        # modified documents -> better have some extra checks than producing
        # invalid collection states.
        # Remove this test after finding a permanent solution.

        forward, backward = self._get_forward_backward_deltas(dict(), self.doc)
        timestamp = self._get_timestamp()

        first_delta = dict(
            _id=ObjectId(),
            document_id=self.doc['_id'],
            collection_version_id=1,
            branch='main',
            timestamp=timestamp,
            forward=forward.dumps(),
            backward=backward.dumps(),
            prev=None,
            next=[],
        )
        find_mock.return_value = [deepcopy(first_delta)]

        # not a new version, but a new update before registering the doc
        doc_old = dict()
        doc_new = self._update_doc(self.doc)

        delta_id = ObjectId()
        ret_delta_id = self.col.add_delta(
            document_new=doc_new,
            document_old=doc_old,
            document_id=doc_new['_id'],
            collection_version=1,
            branch='main',
            timestamp=timestamp,
            branch_history=[(0, 'main')],
            with_id=delta_id,
        )
        self.assertEqual(first_delta['_id'], ret_delta_id)

        insert_one_mock.assert_not_called()
        find_one_and_update_mock.assert_not_called()

        forward2, backward2 = self._get_forward_backward_deltas(
            doc_old, doc_new
        )

        update_one_mock.assert_called_once_with(
            {'_id': first_delta['_id']},
            update={
                "$set": {
                    'forward': forward2.dumps(),
                    'backward': backward2.dumps(),
                }
            },
        )

    @patch.object(pymongo.collection.Collection, 'find')
    @patch.object(pymongo.collection.Collection, 'update_one')
    @patch.object(pymongo.collection.Collection, 'insert_one')
    @patch.object(pymongo.collection.Collection, 'find_one_and_update')
    def test_update_already_added_delta2(
        self,
        find_one_and_update_mock,
        insert_one_mock,
        update_one_mock,
        find_mock,
    ):
        first_delta_id = ObjectId()

        forward, backward = self._get_forward_backward_deltas(dict(), self.doc)
        parent_delta = dict(
            _id=ObjectId(),
            document_id=self.doc['_id'],
            collection_version_id=1,
            branch='main',
            timestamp=self._get_timestamp(),
            forward=forward.dumps(),
            backward=backward.dumps(),
            prev=None,
            next=[first_delta_id],
        )

        timestamp = self._get_timestamp()
        forward, backward = self._get_forward_backward_deltas(
            self.doc, self._update_doc(self.doc)
        )
        first_delta = dict(
            _id=first_delta_id,
            document_id=self.doc['_id'],
            collection_version_id=2,
            branch='main',
            timestamp=timestamp,
            forward=forward.dumps(),
            backward=backward.dumps(),
            prev=parent_delta['_id'],
            next=[],
        )

        find_mock.return_value = [deepcopy(parent_delta), deepcopy(first_delta)]

        doc_old = self.doc
        doc_new = self._update_doc(self._update_doc(self.doc))
        delta_id = ObjectId()
        ret_delta_id = self.col.add_delta(
            document_new=doc_new,
            document_old=doc_old,
            document_id=doc_new['_id'],
            collection_version=2,
            branch='main',
            timestamp=timestamp,
            branch_history=[(1, 'main'), (0, 'main')],
            with_id=delta_id,
        )
        self.assertEqual(first_delta_id, ret_delta_id)

        insert_one_mock.assert_not_called()
        find_one_and_update_mock.assert_not_called()

        forward2, backward2 = self._get_forward_backward_deltas(
            doc_old, doc_new
        )

        update_one_mock.assert_called_once_with(
            {'_id': first_delta['_id']},
            update={
                "$set": {
                    'forward': forward2.dumps(),
                    'backward': backward2.dumps(),
                }
            },
        )

    def test_add_first_delta_on_a_new_branch(self):
        pass

    def test_add_delta_with_existing_parent_on_branch(self):
        pass

    def test_add_delta_on_branch_with_unconnected_delta_tree(self):
        # add a document on 2 different branches such that for both branches,
        # the deltas are added after the version corresponding to the
        # intersection of the branches, i.e., the LCA corresponding to the
        # versions for which the deltas are registered.
        pass
