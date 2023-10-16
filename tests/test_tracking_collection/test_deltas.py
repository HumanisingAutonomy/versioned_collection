import datetime
from collections import defaultdict
from copy import deepcopy
from typing import Dict, List, Tuple
from unittest.mock import patch

import deepdiff
import pymongo
from bson import ObjectId

import versioned_collection.collection.tracking_collections
from tests.test_tracking_collection.in_memory_database import \
    InMemoryDatabaseSetup
from versioned_collection.collection.tracking_collections import \
    DeltasCollection


def _get_timestamp():
    # bson stores date times up to millisecond precision, so chop of the
    # nanoseconds
    timestamp = datetime.datetime.utcnow()
    timestamp = datetime.datetime.fromisoformat(
        timestamp.isoformat(timespec='milliseconds')
    )
    return timestamp


def _update_doc(doc, _id=None):
    doc = deepcopy(doc)
    doc['v'] += 1
    if _id is not None:
        doc['_id'] = _id
    return doc


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

    def assertEqualDeltaLists(
        self, l1: List[deepdiff.Delta], l2: List[deepdiff.Delta]
    ) -> None:
        # deepdiff.Delta does not implement equality, so compare the
        # underlying diffs.
        # Alternatively, we can compare the __dict__ repr of each delta,
        # but a delta is only a wrapper that applies a deepdiff.DeepDiff,
        # so that should be enough

        self.assertEqual(len(l1), len(l2))
        for d1, d2 in zip(l1, l2):
            self.assertEqual(d1.diff, d2.diff)

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
        timestamp = _get_timestamp()

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
        first_delta_id = self.col.add_delta(
            document_new=self.doc,
            document_old=dict(),
            document_id=self.doc['_id'],
            collection_version=1,
            branch='main',
            timestamp=_get_timestamp(),
            branch_history=[(0, 'main')]
        )

        new_doc = _update_doc(self.doc)
        second_delta_id = self.col.add_delta(
            document_new=new_doc,
            document_old=self.doc,
            document_id=self.doc['_id'],
            collection_version=2,
            branch='main',
            timestamp=_get_timestamp(),
            branch_history=[(1, 'main'), (0, 'main')]
        )

        parent_delta = self.col.find_one({'_id': first_delta_id})
        self.assertEqual([second_delta_id], parent_delta['next'])

        delta = self.col.find_one({'_id': second_delta_id})
        self.assertEqual(second_delta_id, delta['_id'])
        self.assertEqual(self.doc['_id'], delta['document_id'])
        self.assertEqual(2, delta['collection_version_id'])
        self.assertEqual('main', delta['branch'])
        self.assertEqual(first_delta_id, delta['prev'])
        self.assertEqual([], delta['next'])

    def test_add_delta_on_branch_with_unconnected_delta_tree(self):
        # add a document on 2 different branches such that for both branches,
        # the deltas are added after the version corresponding to the
        # intersection of the branches, i.e., the LCA corresponding to the
        # versions for which the deltas are registered.
        first_delta_id = self.col.add_delta(
            document_new=self.doc,
            document_old=dict(),
            document_id=self.doc['_id'],
            collection_version=2,
            branch='main',
            timestamp=_get_timestamp(),
            branch_history=[(1, 'main'), (0, 'main')]
        )

        second_delta_id = self.col.add_delta(
            document_new=self.doc,
            document_old=dict(),
            document_id=self.doc['_id'],
            collection_version=0,
            branch='branch',
            timestamp=_get_timestamp(),
            branch_history=[(0, 'branch'), (1, 'main'), (0, 'main')]
        )

        first_delta = self.col.find_one({'_id': first_delta_id})
        self.assertIsNone(first_delta['prev'])

        second_delta = self.col.find_one({'_id': second_delta_id})
        self.assertIsNone(second_delta['prev'])

    def _setup_1(self) -> Tuple[
        Dict[ObjectId, Dict[str, ObjectId]],
        Dict[ObjectId, Dict[str, Dict[str, deepdiff.Delta]]]
    ]:
        #
        #            0_m
        #              \
        #              1_m
        #            /   \
        #         0_b    2_m
        #                 \
        #                 3_m
        #
        d1_deltas = defaultdict(dict)

        d_old = dict()
        d_new = self.doc
        forward, backward = _get_forward_backward_deltas(d_old, d_new)
        d1_deltas['1_m']['f'] = forward
        d1_deltas['1_m']['b'] = backward
        d1_m = self.col.add_delta(
            document_new=d_new,
            document_old=d_old,
            document_id=self.doc['_id'],
            collection_version=1,
            branch='main',
            timestamp=_get_timestamp(),
            branch_history=[(0, 'main')]
        )

        d_old = self.doc
        d_new = _update_doc(self.doc)
        forward, backward = _get_forward_backward_deltas(d_old, d_new)
        d1_deltas['2_m']['f'] = forward
        d1_deltas['2_m']['b'] = backward
        d2_m = self.col.add_delta(
            document_new=d_new,
            document_old=d_old,
            document_id=self.doc['_id'],
            collection_version=2,
            branch='main',
            timestamp=_get_timestamp(),
            branch_history=[(1, 'main'), (0, 'main')]
        )

        d_old = d_new
        d_new = _update_doc(d_new)
        forward, backward = _get_forward_backward_deltas(d_old, d_new)
        d1_deltas['3_m']['f'] = forward
        d1_deltas['3_m']['b'] = backward
        d3_m = self.col.add_delta(
            document_new=d_new,
            document_old=d_old,
            document_id=self.doc['_id'],
            collection_version=3,
            branch='main',
            timestamp=_get_timestamp(),
            branch_history=[(2, 'main'), (1, 'main'), (0, 'main')]
        )
        d_old = dict()
        d_new = self.doc
        forward, backward = _get_forward_backward_deltas(d_old, d_new)
        d1_deltas['0_b']['f'] = forward
        d1_deltas['0_b']['b'] = backward
        d0_b = self.col.add_delta(
            document_new=self.doc,
            document_old=dict(),
            document_id=self.doc['_id'],
            collection_version=0,
            branch='b',
            timestamp=_get_timestamp(),
            branch_history=[(1, 'main'), (0, 'main')]
        )

        deep_deltas = dict()
        deep_deltas[self.doc['_id']] = d1_deltas

        delta_ids = dict()
        delta_ids[self.doc['_id']] = {
            '1_m': d1_m, '2_m': d2_m, '3_m': d3_m, '0_b': d0_b
        }
        return delta_ids, deep_deltas
    
    def test_get_delta_documents_in_path_forward(self):
        delta_ids, _ = self._setup_1()
        delta_ids = delta_ids[self.doc['_id']]
        deltas = list(self.col.get_delta_documents_in_path(
            {(0, 'main'): 1, (1, 'main'): 1, (2, 'main'): 1, (3, 'main'): 1}
        ))
        self.assertEqual(1, len(deltas))
        group = deltas[0]
        self.assertEqual(self.doc['_id'], group['_id'])

        ret_deltas_ids = [d['_id'] for d in group['deltas']]
        expected_deltas_ids = [
            delta_ids['1_m'], delta_ids['2_m'], delta_ids['3_m']
        ]
        self.assertEqual(expected_deltas_ids, ret_deltas_ids)

    def test_get_delta_documents_in_path_backward(self):
        delta_ids, _ = self._setup_1()
        delta_ids = delta_ids[self.doc['_id']]
        path = {
            (3, 'main'): -1, (2, 'main'): -1, (1, 'main'): -1, (0, 'main'): -1
        }
        deltas = list(self.col.get_delta_documents_in_path(
            path, sorting_order=pymongo.DESCENDING
        ))
        self.assertEqual(1, len(deltas))
        group = deltas[0]
        self.assertEqual(self.doc['_id'], group['_id'])

        ret_deltas_ids = [d['_id'] for d in group['deltas']]
        expected_deltas_ids = [
            delta_ids['3_m'], delta_ids['2_m'], delta_ids['1_m']
        ]
        self.assertEqual(expected_deltas_ids, ret_deltas_ids)

    def test_get_delta_documents_in_path_with_branch(self):
        delta_ids, _ = self._setup_1()
        delta_ids = delta_ids[self.doc['_id']]
        path = {
            (3, 'main'): -1, (2, 'main'): -1, (1, 'main'): 1, (0, 'b'): 1
        }
        deltas = list(self.col.get_delta_documents_in_path(
            path
        ))
        self.assertEqual(1, len(deltas))
        group = deltas[0]
        self.assertEqual(self.doc['_id'], group['_id'])

        ret_deltas_ids = {d['_id'] for d in group['deltas']}
        expected_deltas_ids = {
            delta_ids['3_m'], delta_ids['2_m'], delta_ids['1_m'],
            delta_ids['0_b']
        }
        self.assertEqual(expected_deltas_ids, ret_deltas_ids)

    def _setup_2(self):
        #
        #         self.doc          self.doc2
        #            [0_m]
        #              \             /    \
        #              1_m         0_c    1_m
        #             /  \                  \
        #          0_b   2_m                 \
        #                 \                   \
        #                 3_m                 3_m
        #
        delta_ids, deep_deltas = self._setup_1()

        self.doc2 = deepcopy(self.doc)
        self.doc2['_id'] = ObjectId()
        self.doc2['stop'] = 'hammer time'

        d2_deltas = defaultdict(dict)

        d_old = dict()
        d_new = self.doc2
        forward, backward = _get_forward_backward_deltas(d_old, d_new)
        d2_deltas['0_c']['f'] = forward
        d2_deltas['0_c']['b'] = backward
        d2_0_c = self.col.add_delta(
            document_new=d_new,
            document_old=d_old,
            document_id=self.doc2['_id'],
            collection_version=0,
            branch='c',
            timestamp=_get_timestamp(),
            branch_history=[(0, 'main')]
        )

        d_old = dict()
        d_new = self.doc2
        forward, backward = _get_forward_backward_deltas(d_old, d_new)
        d2_deltas['1_m']['f'] = forward
        d2_deltas['1_m']['b'] = backward
        d2_1_m = self.col.add_delta(
            document_new=d_new,
            document_old=d_old,
            document_id=self.doc2['_id'],
            collection_version=1,
            branch='main',
            timestamp=_get_timestamp(),
            branch_history=[(0, 'main')]
        )

        d_old = self.doc2
        d_new = _update_doc(self.doc2)
        forward, backward = _get_forward_backward_deltas(d_old, d_new)
        d2_deltas['3_m']['f'] = forward
        d2_deltas['3_m']['b'] = backward
        d2_3_m = self.col.add_delta(
            document_new=d_new,
            document_old=d_old,
            document_id=self.doc2['_id'],
            collection_version=3,
            branch='main',
            timestamp=_get_timestamp(),
            branch_history=[(2, 'main'), (1, 'main'), (0, 'main')]
        )

        delta_ids[self.doc2['_id']] = {
            '1_m': d2_1_m, '3_m': d2_3_m, '0_c': d2_0_c
        }
        deep_deltas[self.doc2['_id']] = d2_deltas

        return delta_ids, deep_deltas

    def test_get_deltas(self):
        _, deep_deltas = self._setup_2()
        path = {(3, 'main'): -1, (2, 'main'): -1, (1, 'main'): 0, (0, 'b'): 1}

        deltas = self.col.get_deltas(path)
        self.assertEqual(2, len(deltas))

        doc_1_id = self.doc['_id']
        expected_d1 = [
            deep_deltas[doc_1_id]['3_m']['b'],
            deep_deltas[doc_1_id]['2_m']['b'],
            deep_deltas[doc_1_id]['0_b']['f'],
        ]
        self.assertEqualDeltaLists(expected_d1, deltas[doc_1_id])

        doc_2_id = self.doc2['_id']
        expected_d2 = [
            deep_deltas[doc_2_id]['3_m']['b'],
        ]
        self.assertEqualDeltaLists(expected_d2, deltas[doc_2_id])


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

    def assertEqualDeltaLists(
        self, l1: List[deepdiff.Delta], l2: List[deepdiff.Delta]
    ) -> None:
        # deepdiff.Delta does not implement equality, so compare the
        # underlying diffs.
        # Alternatively, we can compare the __dict__ repr of each delta,
        # but a delta is only a wrapper that applies a deepdiff.DeepDiff,
        # so that should be enough

        self.assertEqual(len(l1), len(l2))
        for d1, d2 in zip(l1, l2):
            self.assertEqual(d1.diff, d2.diff)

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
        timestamp = _get_timestamp()
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

        forward, backward = _get_forward_backward_deltas(
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

    def _test_add_delta_with_existing_parent(
        self,
        find_one_and_update_mock,
        insert_one_mock,
        update_one_mock,
        find_mock,
        child_version: int,
        child_branch: str
    ):
        forward, backward = _get_forward_backward_deltas(dict(), self.doc)

        parent_delta = dict(
            _id=ObjectId(),
            document_id=self.doc['_id'],
            collection_version_id=1,
            branch='main',
            timestamp=_get_timestamp(),
            forward=forward.dumps(),
            backward=backward.dumps(),
            prev=None,
            next=[],
        )
        find_mock.return_value = [deepcopy(parent_delta)]

        doc_old = self.doc
        doc_new = _update_doc(doc_old)

        delta_id = ObjectId()
        timestamp = _get_timestamp()
        ret_delta_id = self.col.add_delta(
            document_new=doc_new,
            document_old=doc_old,
            document_id=doc_new['_id'],
            collection_version=child_version,
            branch=child_branch,
            timestamp=timestamp,
            branch_history=[(1, 'main'), (0, 'main')],
            with_id=delta_id,
        )
        self.assertEqual(delta_id, ret_delta_id)

        update_one_mock.assert_not_called()

        forward, backward = _get_forward_backward_deltas(doc_old, doc_new)
        delta_doc = dict(
            _id=delta_id,
            document_id=self.doc['_id'],
            collection_version_id=child_version,
            branch=child_branch,
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
    def test_add_delta_with_existing_parent(
        self,
        find_one_and_update_mock,
        insert_one_mock,
        update_one_mock,
        find_mock,
    ):
        self._test_add_delta_with_existing_parent(
            find_one_and_update_mock,
            insert_one_mock,
            update_one_mock,
            find_mock,
            child_version=2,
            child_branch='main'
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

        forward, backward = _get_forward_backward_deltas(dict(), self.doc)
        timestamp = _get_timestamp()

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
        doc_new = _update_doc(self.doc)

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

        forward2, backward2 = _get_forward_backward_deltas(
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

        forward, backward = _get_forward_backward_deltas(dict(), self.doc)
        parent_delta = dict(
            _id=ObjectId(),
            document_id=self.doc['_id'],
            collection_version_id=1,
            branch='main',
            timestamp=_get_timestamp(),
            forward=forward.dumps(),
            backward=backward.dumps(),
            prev=None,
            next=[first_delta_id],
        )

        timestamp = _get_timestamp()
        forward, backward = _get_forward_backward_deltas(
            self.doc, _update_doc(self.doc)
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
        doc_new = _update_doc(_update_doc(self.doc))
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

        forward2, backward2 = _get_forward_backward_deltas(
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
    def test_add_delta_with_existing_parent_on_a_branch(
        self,
        find_one_and_update_mock,
        insert_one_mock,
        update_one_mock,
        find_mock,
    ):
        self._test_add_delta_with_existing_parent(
            find_one_and_update_mock,
            insert_one_mock,
            update_one_mock,
            find_mock,
            child_version=0,
            child_branch='branch'
        )

    @patch.object(pymongo.collection.Collection, 'find')
    @patch.object(pymongo.collection.Collection, 'update_one')
    @patch.object(pymongo.collection.Collection, 'insert_one')
    @patch.object(pymongo.collection.Collection, 'find_one_and_update')
    def test_add_delta_on_branch_with_unconnected_delta_tree(
        self,
        find_one_and_update_mock,
        insert_one_mock,
        update_one_mock,
        find_mock,
    ):
        # add a document on 2 different branches such that for both branches,
        # the deltas are added after the version corresponding to the
        # intersection of the branches, i.e., the LCA corresponding to the
        # versions for which the deltas are registered.

        forward, backward = _get_forward_backward_deltas(dict(), self.doc)

        other_delta = dict(
            _id=ObjectId(),
            document_id=self.doc['_id'],
            collection_version_id=4,
            branch='main',
            timestamp=_get_timestamp(),
            forward=forward.dumps(),
            backward=backward.dumps(),
            prev=None,
            next=[],
        )
        find_mock.return_value = [deepcopy(other_delta)]

        delta_id = ObjectId()
        timestamp = _get_timestamp()
        ret_delta_id = self.col.add_delta(
            document_new=self.doc,
            document_old=dict(),
            document_id=self.doc['_id'],
            collection_version=1,
            branch='branch',
            timestamp=timestamp,
            branch_history=[(0, 'branch'), (1, 'main'), (0, 'main')],
            with_id=delta_id,
        )
        self.assertEqual(delta_id, ret_delta_id)

        update_one_mock.assert_not_called()
        find_one_and_update_mock.assert_not_called()

        delta_doc = dict(
            _id=delta_id,
            document_id=self.doc['_id'],
            collection_version_id=1,
            branch='branch',
            timestamp=timestamp,
            forward=forward.dumps(),
            backward=backward.dumps(),
            prev=None,
            next=[],
        )
        insert_one_mock.assert_called_once_with(delta_doc)

    @patch.object(pymongo.collection.Collection, 'insert_many')
    @patch.object(pymongo.collection.Collection, 'find_one_and_update')
    def test_insert_delta_docs(
        self,
        find_one_and_update_mock,
        insert_many_mock,
    ):
        d_id_1, d_id_2, d_id_3 = ObjectId(), ObjectId(), ObjectId()
        root_id = ObjectId()
        # fake
        forward, backward = _get_forward_backward_deltas(dict(), dict())

        delta_1 = dict(
            _id=d_id_1,
            document_id=ObjectId(),
            collection_version_id=1,
            branch='main',
            timestamp=_get_timestamp(),
            forward=forward.dumps(),
            backward=backward.dumps(),
            prev=root_id,
            next=[d_id_2, ObjectId(), ObjectId()],
        )
        delta_2 = dict(
            _id=d_id_2,
            document_id=ObjectId(),
            collection_version_id=2,
            branch='main',
            timestamp=_get_timestamp(),
            forward=forward.dumps(),
            backward=backward.dumps(),
            prev=d_id_1,
            next=[d_id_3, ObjectId()],
        )
        delta_3 = dict(
            _id=d_id_3,
            document_id=ObjectId(),
            collection_version_id=3,
            branch='main',
            timestamp=_get_timestamp(),
            forward=forward.dumps(),
            backward=backward.dumps(),
            prev=d_id_2,
            next=[],
        )

        deltas = [delta_1, delta_2, delta_3]
        self.col.insert_delta_docs(deltas)

        find_one_and_update_mock.assert_called_once_with(
            filter={'_id': root_id},
            update={"$push": {"next": d_id_1}},
        )

        delta_1['next'] = [d_id_2]
        delta_2['next'] = [d_id_3]
        insert_many_mock.assert_called_once_with(deltas)

    @patch.object(pymongo.collection.Collection, 'aggregate')
    def test_get_delta_documents_in_path_forward(self, aggregate_mock):
        # Path from version (0, 'm') to (2, 'm')
        path = {(0, 'm'): 1, (1, 'm'): 1, (2, 'm'): 1}

        self.col.get_delta_documents_in_path(path)

        # We don't care for delta (0, 'm') since that would move to (0, 'm'),
        # not from (0, 'm')
        cond = {"$or": [
            {'collection_version_id': 1, 'branch': 'm'},
            {'collection_version_id': 2, 'branch': 'm'},
        ]}
        aggregate_mock.assert_called_once_with([
            {"$match": cond},
            {"$group": {'_id': "$document_id", 'deltas': {"$push": "$$ROOT"}}}
        ],
            allowDiskUse=True
        )

    @patch.object(pymongo.collection.Collection, 'aggregate')
    def test_get_delta_documents_in_path_backward(self, aggregate_mock):
        # Path from version (2, 'm') to (0, 'm')
        path = {(2, 'm'): -1, (1, 'm'): -1, (0, 'm'): -1}

        self.col.get_delta_documents_in_path(path)

        # We don't care for delta (0, 'm') since that would move backward from
        # (0, 'm'), not to (0, 'm')
        cond = {"$or": [
            {'collection_version_id': 2, 'branch': 'm'},
            {'collection_version_id': 1, 'branch': 'm'},
        ]}
        aggregate_mock.assert_called_once_with([
            {"$match": cond},
            {"$group": {'_id': "$document_id", 'deltas': {"$push": "$$ROOT"}}}
        ],
            allowDiskUse=True
        )

    @patch.object(pymongo.collection.Collection, 'aggregate')
    def test_get_delta_documents_in_path_with_branches(self, aggregate_mock):
        path = {(1, 'm'): -1, (0, 'm'): 1, (1, 'b'): 1}

        self.col.get_delta_documents_in_path(path)

        cond = {"$or": [
            {'collection_version_id': 1, 'branch': 'm'},
            {'collection_version_id': 0, 'branch': 'm'},
            {'collection_version_id': 1, 'branch': 'b'},
        ]}
        aggregate_mock.assert_called_once_with([
            {"$match": cond},
            {"$group": {'_id': "$document_id", 'deltas': {"$push": "$$ROOT"}}}
        ],
            allowDiskUse=True
        )

    def _setup(self):
        """
            ::

                 self.doc          self.doc2
                    [0_m]
                      \\             /    \\
                      1_m         0_c     1_m
                     /  \\                  \\
                  0_b    2_m                \\
                          \\                  \\
                          3_m                 3_m
        """

        # self.doc
        delta_docs = dict()
        d1_ids = {v: ObjectId() for v in ['1_m', '2_m', '3_m', '0_b']}
        deep_deltas = dict()
        d1_deltas = defaultdict(dict)
        forward, backward = _get_forward_backward_deltas(dict(), self.doc)
        d1_deltas['1_m']['f'] = forward
        d1_deltas['1_m']['b'] = backward
        d1_1_m = dict(
            _id=d1_ids['1_m'],
            document_id=self.doc['_id'],
            collection_version_id=1,
            branch='main',
            timestamp=_get_timestamp(),
            forward=forward.dumps(),
            backward=backward.dumps(),
            prev=None,
            next=[d1_ids['2_m'], d1_ids['0_b']],
        )

        forward, backward = _get_forward_backward_deltas(
            self.doc, _update_doc(self.doc)
        )
        d1_deltas['2_m']['f'] = forward
        d1_deltas['2_m']['b'] = backward
        d1_2_m = dict(
            _id=d1_ids['2_m'],
            document_id=self.doc['_id'],
            collection_version_id=2,
            branch='main',
            timestamp=_get_timestamp(),
            forward=forward.dumps(),
            backward=backward.dumps(),
            prev=d1_ids['1_m'],
            next=[d1_ids['3_m']],
        )

        forward, backward = _get_forward_backward_deltas(
            _update_doc(self.doc), _update_doc(_update_doc(self.doc))
        )
        d1_deltas['3_m']['f'] = forward
        d1_deltas['3_m']['b'] = backward
        d1_3_m = dict(
            _id=d1_ids['3_m'],
            document_id=self.doc['_id'],
            collection_version_id=3,
            branch='main',
            timestamp=_get_timestamp(),
            forward=forward.dumps(),
            backward=backward.dumps(),
            prev=d1_ids['2_m'],
            next=[],
        )

        forward, backward = _get_forward_backward_deltas(
            self.doc, _update_doc(self.doc)
        )
        d1_deltas['0_b']['f'] = forward
        d1_deltas['0_b']['b'] = backward
        d1_0_b = dict(
            _id=d1_ids['0_b'],
            document_id=self.doc['_id'],
            collection_version_id=0,
            branch='b',
            timestamp=_get_timestamp(),
            forward=forward.dumps(),
            backward=backward.dumps(),
            prev=d1_ids['1_m'],
            next=[],
        )
        delta_docs[self.doc['_id']] = {
            '1_m': d1_1_m, '2_m': d1_2_m, '3_m': d1_3_m, '0_b': d1_0_b
        }
        deep_deltas[self.doc['_id']] = d1_deltas

        # self.doc2
        self.doc2 = deepcopy(self.doc)
        self.doc2['_id'] = ObjectId()
        self.doc2['stop'] = 'hammer time'
        d2_ids = {v: ObjectId() for v in ['1_m', '3_m', '0_c']}
        d2_deltas = defaultdict(dict)

        forward, backward = _get_forward_backward_deltas(dict(), self.doc2)
        d2_deltas['0_c']['f'] = forward
        d2_deltas['0_c']['b'] = backward
        d2_0_c = dict(
            _id=d2_ids['0_c'],
            document_id=self.doc2['_id'],
            collection_version_id=0,
            branch='c',
            timestamp=_get_timestamp(),
            forward=forward.dumps(),
            backward=backward.dumps(),
            prev=None,
            next=[],
        )

        forward, backward = _get_forward_backward_deltas(
            dict(), _update_doc(self.doc2)
        )
        d2_deltas['1_m']['f'] = forward
        d2_deltas['1_m']['b'] = backward
        d2_1_m = dict(
            _id=d2_ids['1_m'],
            document_id=self.doc2['_id'],
            collection_version_id=1,
            branch='main',
            timestamp=_get_timestamp(),
            forward=forward.dumps(),
            backward=backward.dumps(),
            prev=None,
            next=[d2_ids['3_m']],
        )

        forward, backward = _get_forward_backward_deltas(
            dict(), _update_doc(self.doc2)
        )
        d2_deltas['3_m']['f'] = forward
        d2_deltas['3_m']['b'] = backward
        d2_3_m = dict(
            _id=d2_ids['3_m'],
            document_id=self.doc2['_id'],
            collection_version_id=3,
            branch='main',
            timestamp=_get_timestamp(),
            forward=forward.dumps(),
            backward=backward.dumps(),
            prev=d2_ids['1_m'],
            next=[],
        )
        delta_docs[self.doc2['_id']] = {
            '1_m': d2_1_m, '3_m': d2_3_m, '0_c': d2_0_c
        }
        deep_deltas[self.doc2['_id']] = d2_deltas

        return delta_docs, deep_deltas

    @patch.object(pymongo.collection.Collection, 'aggregate')
    def test_get_deltas_linear_forward_1(self, aggregate_mock):
        delta_docs, deep_deltas = self._setup()

        aggregate_mock.return_value = [
            {'_id': doc_id, 'deltas': list(delta_docs[doc_id].values())}
            for doc_id in delta_docs.keys()
        ]

        path = {(0, 'main'): 1, (1, 'main'): 1, (2, 'main'): 1, (3, 'main'): 1}
        deltas = self.col.get_deltas(path)
        self.assertEqual(2, len(deltas))

        doc_1_id = self.doc['_id']
        doc_1_deltas = [
            deep_deltas[doc_1_id]['1_m']['f'],
            deep_deltas[doc_1_id]['2_m']['f'],
            deep_deltas[doc_1_id]['3_m']['f'],
        ]
        self.assertEqualDeltaLists(doc_1_deltas, deltas[doc_1_id])

        doc_2_id = self.doc2['_id']
        doc_2_deltas = [
            deep_deltas[doc_2_id]['1_m']['f'],
            deep_deltas[doc_2_id]['3_m']['f'],
        ]
        self.assertEqualDeltaLists(doc_2_deltas, deltas[doc_2_id])

    @patch.object(pymongo.collection.Collection, 'aggregate')
    def test_get_deltas_linear_forward_2(self, aggregate_mock):
        delta_docs, deep_deltas = self._setup()
        doc_1_id = self.doc['_id']
        doc_2_id = self.doc2['_id']

        aggregate_mock.return_value = [
            {'_id': doc_1_id,
             'deltas': [
                 delta_docs[doc_1_id]['2_m'], delta_docs[doc_1_id]['3_m']
             ]},
            {'_id': doc_2_id,
             'deltas': [delta_docs[doc_2_id]['3_m']]
             }
        ]
        path = {(1, 'main'): 1, (2, 'main'): 1, (3, 'main'): 1}
        deltas = self.col.get_deltas(path)
        self.assertEqual(2, len(deltas))

        doc_1_deltas = [
            deep_deltas[doc_1_id]['2_m']['f'],
            deep_deltas[doc_1_id]['3_m']['f'],
        ]
        self.assertEqualDeltaLists(doc_1_deltas, deltas[doc_1_id])

        doc_2_deltas = [
            deep_deltas[doc_2_id]['3_m']['f'],
        ]
        self.assertEqualDeltaLists(doc_2_deltas, deltas[doc_2_id])

    @patch.object(pymongo.collection.Collection, 'aggregate')
    def test_get_deltas_linear_backward(self, aggregate_mock):
        delta_docs, deep_deltas = self._setup()
        doc_1_id = self.doc['_id']
        doc_2_id = self.doc2['_id']

        aggregate_mock.return_value = [
            {'_id': doc_1_id,
             'deltas': [delta_docs[doc_1_id]['3_m']]},
            {'_id': doc_2_id,
             'deltas': [delta_docs[doc_2_id]['3_m']]
             }
        ]
        path = {(3, 'main'): -1, (2, 'main'): -1}
        deltas = self.col.get_deltas(path)
        self.assertEqual(2, len(deltas))

        doc_1_deltas = [
            deep_deltas[doc_1_id]['3_m']['b'],
        ]
        self.assertEqualDeltaLists(doc_1_deltas, deltas[doc_1_id])

        doc_2_deltas = [
            deep_deltas[doc_2_id]['3_m']['b'],
        ]
        self.assertEqualDeltaLists(doc_2_deltas, deltas[doc_2_id])

    @patch.object(pymongo.collection.Collection, 'aggregate')
    def test_get_deltas_branch_complete(self, aggregate_mock):
        delta_docs, deep_deltas = self._setup()
        doc_1_id = self.doc['_id']
        doc_2_id = self.doc2['_id']

        aggregate_mock.return_value = [{
            '_id': doc_1_id,
            'deltas': list(delta_docs[doc_1_id].values())
        }, {
            '_id': doc_2_id,
            'deltas': [delta_docs[doc_2_id]['3_m'], delta_docs[doc_2_id]['1_m']]
        }]

        path = {(3, 'main'): -1, (2, 'main'): -1, (1, 'main'): 0, (0, 'b'): 1}
        deltas = self.col.get_deltas(path)
        self.assertEqual(2, len(deltas))

        doc_1_deltas = [
            deep_deltas[doc_1_id]['3_m']['b'],
            deep_deltas[doc_1_id]['2_m']['b'],
            deep_deltas[doc_1_id]['0_b']['f'],
        ]
        self.assertEqualDeltaLists(doc_1_deltas, deltas[doc_1_id])

        doc_2_deltas = [
            deep_deltas[doc_2_id]['3_m']['b'],
        ]
        self.assertEqualDeltaLists(doc_2_deltas, deltas[doc_2_id])

    @patch.object(pymongo.collection.Collection, 'aggregate')
    def test_get_deltas_branch_partial(self, aggregate_mock):
        delta_docs, deep_deltas = self._setup()
        doc_1_id = self.doc['_id']
        doc_2_id = self.doc2['_id']

        aggregate_mock.return_value = [{
            '_id': doc_1_id,
            'deltas': [
                delta_docs[doc_1_id]['3_m'],
                delta_docs[doc_1_id]['2_m'],
                delta_docs[doc_1_id]['1_m'],
            ]
        }, {
            '_id': doc_2_id,
            'deltas': [
                delta_docs[doc_2_id]['3_m'],
                delta_docs[doc_2_id]['1_m'],
                delta_docs[doc_2_id]['0_c'],
            ]
        }]

        path = {
            (3, 'main'): -1,
            (2, 'main'): -1,
            (1, 'main'): -1,
            (0, 'main'): 0,
            (0, 'c'): 1,
        }
        deltas = self.col.get_deltas(path)
        self.assertEqual(2, len(deltas))

        doc_1_deltas = [
            deep_deltas[doc_1_id]['3_m']['b'],
            deep_deltas[doc_1_id]['2_m']['b'],
            deep_deltas[doc_1_id]['1_m']['b'],
        ]
        self.assertEqualDeltaLists(doc_1_deltas, deltas[doc_1_id])

        doc_2_deltas = [
            deep_deltas[doc_2_id]['3_m']['b'],
            deep_deltas[doc_2_id]['1_m']['b'],
            deep_deltas[doc_2_id]['0_c']['f'],
        ]
        self.assertEqualDeltaLists(doc_2_deltas, deltas[doc_2_id])