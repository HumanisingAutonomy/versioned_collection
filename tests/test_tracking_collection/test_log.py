import datetime
from copy import deepcopy
from typing import List, Dict, Any
from unittest import TestCase
from unittest.mock import patch

from bson import ObjectId

import versioned_collection.collection.tracking_collections
import versioned_collection.errors as vc_errors
from tests.test_tracking_collection.in_memory_database import \
    InMemoryDatabaseSetup
from versioned_collection.collection.tracking_collections import \
    LogsCollection


class TestLogSchema(TestCase):

    def setUp(self) -> None:
        self.entry = LogsCollection.SCHEMA(
            version=0,
            branch='main',
            timestamp=datetime.datetime.utcnow(),
            message='Some message',
            prev=None,
            next=[ObjectId(), ObjectId()]
        )

    def test_equal_log_entries(self):
        self.assertEqual(self.entry, self.entry)

    def test_log_entry_not_equal_to_none(self):
        self.assertNotEqual(self.entry, None)

    def test_log_entries_do_not_care_about_order_of_the_next_entries(self):
        entry_1 = self.entry
        entry_2 = deepcopy(self.entry)
        entry_2.next = entry_2.next[::-1]
        self.assertEqual(entry_1, entry_2)

    def test_log_entry_usually_does_not_have_id(self):
        self.assertIsNone(self.entry.id)

    def test_setting_the_entry_id(self):
        some_id = ObjectId()
        self.entry._id = some_id
        self.assertEqual(some_id, self.entry.id)

    def test_weak_equality_between_log_entries(self):
        entry_1 = self.entry
        entry_2 = deepcopy(self.entry)

        # equality => weak_equality
        self.assertTrue(entry_1.weakly_equals(entry_2))

        # weak equality does not care about the prev and next fields
        entry_2.prev = ObjectId()
        self.assertTrue(entry_1.weakly_equals(entry_2))

        entry_1.prev = ObjectId()
        self.assertTrue(entry_1.weakly_equals(entry_2))

        entry_2.next = []
        self.assertTrue(entry_1.weakly_equals(entry_2))

        entry_2.next = [ObjectId()]
        self.assertTrue(entry_1.weakly_equals(entry_2))

        # weak equality does not care about different message strings
        entry_2.message = 'long live rock n roll'
        self.assertTrue(entry_1.weakly_equals(entry_2))

        other_entry = type('other_type', (), {})()
        other_entry.version = entry_1.version
        other_entry.branch = entry_1.branch
        other_entry.timestamp = entry_1.timestamp
        other_entry.message = entry_1.message
        other_entry.prev = entry_1.prev
        other_entry.next = entry_1.next
        self.assertFalse(entry_1.weakly_equals(other_entry))  # type: ignore


class TestLogCollection(InMemoryDatabaseSetup):
    _parent_col_name = 'col'

    def _get_collection(self) -> LogsCollection:
        # this is not in a `setUp` since most of the tests require specific
        # data in the actual collection, and then the log tree would be built
        # twice.
        return LogsCollection(self.database, self._parent_col_name)

    def _initialise_database(self, docs: List[Dict[str, Any]]) -> None:
        col = self.database[LogsCollection.format_name(self._parent_col_name)]
        col.insert_many(docs)

    def _clean_up_database(self) -> None:
        self.database[LogsCollection.format_name(self._parent_col_name)].drop()

    def test_log_tree_not_built_if_collection_does_not_exist(self):
        col = self._get_collection()
        self.assertIsNone(col.log_tree)

    def test_log_build_returns_false_if_collection_already_exists(self):
        col = self._get_collection()

        with patch.object(
            versioned_collection.collection.tracking_collections.LogsCollection,
            'exists'
        ) as exists_mock:
            exists_mock.return_value = True
            self.assertFalse(col.build())

    def test_build_creates_a_log_entry(self):
        col = self._get_collection()
        timestamp = datetime.datetime.utcnow()
        message = 'Test initial version.'
        _id = ObjectId()

        res = col.build(
            message=message,
            timestamp=timestamp,
            with_id=_id,
        )
        self.assertTrue(res)

        log = col.get_log(branch='main', return_ids=True)
        self.assertEqual(1, len(log))

        entry = log[0]
        self.assertEqual(_id, entry.id)
        self.assertEqual(message, entry.message)
        self.assertEqual(timestamp, timestamp)

        self._clean_up_database()

    def test_initialisation_with_unconnected_components_raises_error(self):
        root_id = ObjectId()
        child_id = ObjectId()
        e1 = dict(
            _id=root_id,
            version=0,
            branch='main',
            timestamp=datetime.datetime.utcnow(),
            message='root',
            prev=None,
            next=[child_id]
        )
        e2 = dict(
            _id=child_id,
            version=1,
            branch='main',
            timestamp=datetime.datetime.utcnow(),
            message='v1',
            prev=root_id,
            next=[]
        )

        e3 = dict(
            version=2,
            branch='main',
            timestamp=datetime.datetime.utcnow(),
            message='v2',
            prev=None,
            next=[]
        )
        self._initialise_database([e1, e2, e3])

        # regardless of which root is document is picked up first, this error
        # must be raised because otherwise all documents could have been reached
        with self.assertRaisesRegexp(
            vc_errors.InvalidCollectionState, 'unconnected components'
        ):
            _col = self._get_collection()

        self._clean_up_database()

    def test_initialisation_with_logs_with_invalid_parents_raises_error(self):
        child_id = ObjectId()
        e1 = dict(
            version=0,
            branch='main',
            timestamp=datetime.datetime.utcnow(),
            message='root',
            prev=None,
            next=[child_id]
        )
        e2 = dict(
            _id=child_id,
            version=1,
            branch='main',
            timestamp=datetime.datetime.utcnow(),
            message='v1',
            prev=ObjectId(),
            next=[]
        )
        self._initialise_database([e1, e2])

        with self.assertRaisesRegexp(
            vc_errors.InvalidCollectionState,
            'parent does not exist'
        ):
            _col = self._get_collection()

        self._clean_up_database()

    def test_there_must_be_a_root_document(self):
        entry = dict(
            version=0,
            branch='main',
            timestamp=datetime.datetime.utcnow(),
            message='root',
            prev=ObjectId(),
            next=[]
        )
        self._initialise_database([entry])

        with self.assertRaisesRegexp(
            vc_errors.InvalidCollectionState,
            'No root entry'
        ):
            _col = self._get_collection()

        self._clean_up_database()

    def test_successful_initialisation(self):
        root_id = ObjectId()
        child_1_id = ObjectId()
        child_2_id = ObjectId()
        root = dict(
            _id=root_id,
            version=0,
            branch='main',
            timestamp=datetime.datetime.utcnow(),
            message='root',
            prev=None,
            next=[child_1_id, child_2_id]
        )
        child_1 = dict(
            _id=child_1_id,
            version=1,
            branch='main',
            timestamp=datetime.datetime.utcnow(),
            message='v1',
            prev=root_id,
            next=[]
        )
        child_2 = dict(
            _id=child_2_id,
            version=0,
            branch='branch',
            timestamp=datetime.datetime.utcnow(),
            message='other branch',
            prev=root_id,
            next=[]
        )
        self._initialise_database([root, child_1, child_2])
        root.pop('_id')
        child_1.pop('_id')
        child_2.pop('_id')

        child_id_to_doc = {
            child_1_id: child_1,
            child_2_id: child_2,
        }

        col = self._get_collection()

        self.assertEqual(3, len(col.log_tree))

        root_entry = col.get_log_entry(0, 'main')
        self.assertEqual(
            LogsCollection.SCHEMA(**root), root_entry
        )
        child_1_entry = col.get_log_entry(1, 'main')
        self.assertEqual(
            LogsCollection.SCHEMA(**child_1), child_1_entry
        )
        child_2_entry = col.get_log_entry(0, 'branch')
        self.assertEqual(
            LogsCollection.SCHEMA(**child_2), child_2_entry
        )

        log_tree_children = col.log_tree.children(col.log_tree.root)
        self.assertEqual(2, len(log_tree_children))

        first_child, second_child = log_tree_children

        self.assertEqual(
            first_child.data,
            LogsCollection.SCHEMA(**child_id_to_doc[first_child.tag])
        )
        self.assertEqual(
            second_child.data,
            LogsCollection.SCHEMA(**child_id_to_doc[second_child.tag])
        )
