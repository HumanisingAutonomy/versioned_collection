import datetime
from collections import OrderedDict
from copy import deepcopy
from typing import List, Dict, Any
from unittest import TestCase
from unittest.mock import patch, MagicMock

import pymongo.collection
from bson import ObjectId

import versioned_collection.collection.tracking_collections
import versioned_collection.errors as vc_errors
from tests.test_tracking_collection.in_memory_database import \
    InMemoryDatabaseSetup
from versioned_collection.collection.tracking_collections import \
    LogsCollection
from versioned_collection.utils.data_structures import hashabledict


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


class TestLogCollectionBasics(InMemoryDatabaseSetup):
    # mainly initialisation and collection building. the rest of the
    # functionality is tested separately to avoid duplication wrt setting up
    # and cleaning the database

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

        self.assertTrue(col.contains_version(version=0, branch='main'))

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
        with self.assertRaisesRegex(
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

        with self.assertRaisesRegex(
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

        with self.assertRaisesRegex(
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

        self._clean_up_database()

    def test_collection_reset_does_nothing_if_collection_does_not_exist(self):
        col = self._get_collection()
        with patch.object(
            versioned_collection.collection.tracking_collections.LogsCollection,
            'exists'
        ) as exists_mock:
            exists_mock.return_value = False
            self.assertFalse(col.reset())

    def test_collection_reset_drops_the_collection(self):
        col = self._get_collection()
        self.assertIsNone(col.log_tree)

        with patch.object(
            versioned_collection.collection.tracking_collections.LogsCollection,
            'exists'
        ) as exists_mock:
            exists_mock.return_value = True
            with patch.object(pymongo.collection.Collection, 'drop') as drop:
                self.assertTrue(col.reset())
                drop.assert_called_once()
                self.assertIsNotNone(col.log_tree)

    def test_uninitialised_collection_does_not_contain_any_versions(self):
        # if it contains any versions, one of them MUST be (0, 'main')
        col = self._get_collection()
        self.assertIsNone(col.log_tree)
        self.assertFalse(col.contains_version(0, 'main'))

    def test_get_log_entry_returns_none_if_entry_does_not_exist(self):
        col = self._get_collection()
        self.assertIsNone(col.log_tree)
        self.assertIsNone(col.get_log_entry(0, 'main'))

    def test_get_log_doc_id_returns_none_if_collection_not_initialised(self):
        col = self._get_collection()
        self.assertIsNone(col.log_tree)
        self.assertIsNone(col.get_log_doc_id(0, 'main'))

    def test_get_prev_version_and_branch_returns_none_if_col_not_init(self):
        col = self._get_collection()
        self.assertIsNone(col.log_tree)
        self.assertIsNone(col.get_previous_version_and_branch(1, 'main'))

    def test_log_is_empty_when_col_not_initialised(self):
        col = self._get_collection()
        self.assertIsNone(col.log_tree)
        self.assertEqual(0, len(col.get_log('main')))

    def test_cycle_detection_in_the_log_tree(self):
        root_id = ObjectId()
        child_id = ObjectId()
        root = dict(
            _id=root_id,
            version=0,
            branch='main',
            timestamp=datetime.datetime.utcnow(),
            message='root',
            prev=None,
            next=[child_id]
        )
        child = dict(
            _id=child_id,
            version=1,
            branch='main',
            timestamp=datetime.datetime.utcnow(),
            message='v1',
            prev=root_id,
            next=[root_id]
        )

        self._initialise_database([root, child])
        with self.assertRaisesRegex(
            vc_errors.InvalidCollectionState,
            "The log tree has cycles"
        ):
            _ = self._get_collection()

        self._clean_up_database()


class TestLogsCollection(InMemoryDatabaseSetup):

    @staticmethod
    def _get_setup_1() -> Dict[ObjectId, Dict[str, Any]]:
        """Setup the log tree.

        ::

                                   v0_main
                                      |
                                   __/\\__
                                 /        \\
                               /           \\
                           v1_main       v0_b1
                             |
                      ______/\\_____
                    /        |      \\
                   /         |       \\
               v2_main     v0_b2    v0_b3
                                     |
                                  __/\\__
                                /        \\
                             v1_b3      v0_b4

       """

        root_id = ObjectId()
        v1_main_id = ObjectId()
        v2_main_id = ObjectId()
        v0_b1_id = ObjectId()
        v0_b2_id = ObjectId()
        v0_b3_id = ObjectId()
        v1_b3_id = ObjectId()
        v0_b4_id = ObjectId()

        v0_main = dict(
            _id=root_id,
            version=0,
            branch='main',
            timestamp=datetime.datetime.utcnow(),
            message='root',
            prev=None,
            next=[v1_main_id, v0_b1_id]
        )
        v1_main = dict(
            _id=v1_main_id,
            version=1,
            branch='main',
            timestamp=datetime.datetime.utcnow(),
            message='v1',
            prev=root_id,
            next=[v2_main_id, v0_b2_id, v0_b3_id]
        )
        v2_main = dict(
            _id=v2_main_id,
            version=2,
            branch='main',
            timestamp=datetime.datetime.utcnow(),
            message='v2',
            prev=v1_main_id,
            next=[]
        )
        v0_b1 = dict(
            _id=v0_b1_id,
            version=0,
            branch='b1',
            timestamp=datetime.datetime.utcnow(),
            message='a branch',
            prev=root_id,
            next=[]
        )
        v0_b2 = dict(
            _id=v0_b2_id,
            version=0,
            branch='b2',
            timestamp=datetime.datetime.utcnow(),
            message='another branch',
            prev=v1_main_id,
            next=[]
        )
        v0_b3 = dict(
            _id=v0_b3_id,
            version=0,
            branch='b3',
            timestamp=datetime.datetime.utcnow(),
            message='yet another branch',
            prev=v1_main_id,
            next=[v1_b3_id, v0_b4_id]
        )
        v1_b3 = dict(
            _id=v1_b3_id,
            version=1,
            branch='b3',
            timestamp=datetime.datetime.utcnow(),
            message='some version on yet another branch',
            prev=v0_b3_id,
            next=[]
        )
        v0_b4 = dict(
            _id=v0_b4_id,
            version=0,
            branch='b4',
            timestamp=datetime.datetime.utcnow(),
            message='yet some other branch',
            prev=v0_b3_id,
            next=[]
        )

        docs = [v0_main, v1_main, v2_main, v0_b1, v0_b2, v0_b3, v1_b3, v0_b4]
        return {d['_id']: d for d in docs}

    def setUp(self) -> None:
        parent_collection_name = 'col'

        # database setup
        docs = self._get_setup_1()
        _col_name = LogsCollection.format_name(parent_collection_name)
        self.___raw_col = self.database[_col_name]
        self.___raw_col.insert_many(docs.values())

        self.col = LogsCollection(self.database, parent_collection_name)

        self.log_entries = dict()
        for _id, data in docs.items():
            data.pop('_id')
            self.log_entries[_id] = LogsCollection.SCHEMA(**data)

        self.named_log_entries = {
            f"v{e.version}_{e.branch}": e for e in self.log_entries.values()
        }
        self.named_versions_to_id = {
            f"v{e.version}_{e.branch}": _id
            for _id, e in self.log_entries.items()
        }

    def tearDown(self):
        self.col.reset()

    def test_get_log_entry_when_version_does_not_exist(self):
        self.assertIsNone(self.col.get_log_entry(-1, 'brr'))

    def test_get_log_entry(self):
        expected = self.named_log_entries['v0_b1']
        actual = self.col.get_log_entry(0, 'b1')
        self.assertEqual(expected, actual)

    def test_get_log_entry_id_when_version_does_not_exist(self):
        self.assertIsNone(self.col.get_log_doc_id(3, 'main'))

    def test_get_log_entry_id(self):
        expected = self.named_versions_to_id['v1_main']
        actual = self.col.get_log_doc_id(1, 'main')
        self.assertEqual(expected, actual)

    def test_the_main_branch_does_not_have_a_parent(self):
        self.assertIsNone(self.col.get_parent_branch('main'))

    def test_get_parent_of_non_existing_branch_raises_error(self):
        with self.assertRaises(vc_errors.BranchNotFound):
            self.col.get_parent_branch('brr')

    def test_get_parent_branch(self):
        self.assertEqual('main', self.col.get_parent_branch('b1'))
        self.assertEqual('main', self.col.get_parent_branch('b2'))
        self.assertEqual('main', self.col.get_parent_branch('b3'))
        self.assertEqual('main', self.col.get_parent_branch('b3'))
        self.assertEqual('b3', self.col.get_parent_branch('b4'))

    def test_get_parent_version_of_the_root_returns_none(self):
        self.assertIsNone(self.col.get_parent_version((0, 'main')))

    def test_get_parent_version_of_an_invalid_version_raises_error(self):
        with self.assertRaises(vc_errors.InvalidCollectionVersion):
            self.col.get_parent_version((15, 'beers please'))

    def test_get_parent_version_of_version(self):
        self.assertEqual((0, 'main'), self.col.get_parent_version((1, 'main')))
        self.assertEqual((0, 'main'), self.col.get_parent_version((0, 'b1')))
        self.assertEqual((1, 'main'), self.col.get_parent_version((0, 'b2')))
        self.assertEqual((1, 'main'), self.col.get_parent_version((0, 'b3')))
        self.assertEqual((0, 'b3'), self.col.get_parent_version((0, 'b4')))
        self.assertEqual((0, 'b3'), self.col.get_parent_version((1, 'b3')))

    def test_versions_of_all_branch_tips(self):
        leaf_versions = set(self.col.get_versions_of_branch_tips((0, 'main')))
        expected = {
            (e.version, e.branch) for e in self.log_entries.values()
            if e.next == []
        }
        self.assertEqual(expected, leaf_versions)

    def test_version_of_branch_tips_for_empty_subtree_returns_the_leaf(self):
        v = (0, 'b1')
        self.assertEqual([v], self.col.get_versions_of_branch_tips(v))
        v = (1, 'b3')
        self.assertEqual([v], self.col.get_versions_of_branch_tips(v))
        v = (2, 'main')
        self.assertEqual([v], self.col.get_versions_of_branch_tips(v))

    def test_get_version_of_branch_tips_in_subtree(self):
        self.assertEqual(
            {(1, 'b3'), (0, 'b4')},
            set(self.col.get_versions_of_branch_tips((0, 'b3')))
        )

    def test_contains_version(self):
        for e in self.log_entries.values():
            self.assertTrue(self.col.contains_version(e.version, e.branch))

    def test_get_prev_ver_and_branch_raises_error_if_called_with_version(self):
        with self.assertRaises(vc_errors.InvalidCollectionVersion):
            self.col.get_previous_version_and_branch(42, 'brr')

    def test_get_prev_ver_and_branch_called_with_root_version(self):
        self.assertEqual(
            (-1, 'main'),
            self.col.get_previous_version_and_branch(0, 'main')
        )

    def test_get_prev_ver_and_branch(self):
        self.assertEqual(
            (1, 'main'), self.col.get_previous_version_and_branch(0, 'b2')
        )
        self.assertEqual(
            (0, 'b3'), self.col.get_previous_version_and_branch(0, 'b4')
        )
        self.assertEqual(
            (0, 'b3'), self.col.get_previous_version_and_branch(1, 'b3')
        )

    def test_get_log_raises_error_if_given_non_existent_branch(self):
        with self.assertRaisesRegex(ValueError, 'Invalid branch name'):
            self.col.get_log(branch='brrr')

    def test_get_log_raises_error_if_called_with_non_existent_version(self):
        with self.assertRaisesRegex(ValueError, 'Invalid version'):
            # wrong branch name and version
            self.col.get_log(branch='brrr', version=0)

            # correct branch, wrong version
            self.col.get_log(branch='main', version=42)

    def test_get_log_called_with_the_branch_name_returns_the_whole_path(self):
        main_log = self.col.get_log(branch='main')
        self.assertEqual(3, len(main_log))

        # the log is in descending order
        self.assertEqual(self.named_log_entries['v2_main'], main_log[0])
        self.assertEqual(self.named_log_entries['v1_main'], main_log[1])
        self.assertEqual(self.named_log_entries['v0_main'], main_log[2])

        b4_log = self.col.get_log(branch='b4')
        self.assertEqual(4, len(b4_log))
        self.assertEqual(self.named_log_entries['v0_b4'], b4_log[0])
        self.assertEqual(self.named_log_entries['v0_b3'], b4_log[1])
        self.assertEqual(self.named_log_entries['v1_main'], b4_log[2])
        self.assertEqual(self.named_log_entries['v0_main'], b4_log[3])

        b1_log = self.col.get_log(branch='b1', return_ids=True)
        self.assertEqual(2, len(b1_log))
        self.assertEqual(self.named_versions_to_id['v0_b1'], b1_log[0].id)
        self.assertEqual(self.named_versions_to_id['v0_main'], b1_log[1].id)

    def test_get_log_from_a_specific_version(self):
        self.assertEqual([
            self.named_log_entries['v1_main'],
            self.named_log_entries['v0_main']
        ],
            self.col.get_log('main', version=1)
        )

        self.assertEqual([
            self.named_log_entries['v0_b3'],
            self.named_log_entries['v1_main'],
            self.named_log_entries['v0_main']
        ],
            self.col.get_log('b3', version=0)
        )

        self.assertEqual(
            [self.named_log_entries['v0_main']],
            self.col.get_log('main', version=0)
        )

    def test_delete_subtree_at_root_version(self):
        self.col.delete_subtree((0, 'main'))

        self.assertEqual(0, len(self.col.log_tree))
        self.assertIsNone(self.col.find_one({}))

    def test_delete_subtree_with_invalid_version_raises_error(self):
        with self.assertRaises(vc_errors.InvalidCollectionVersion):
            self.col.delete_subtree((42, 'brr'))

    @patch.object(pymongo.collection.Collection, 'delete_many')
    @patch.object(pymongo.collection.Collection, 'find_one_and_update')
    def test_delete_subtree_on_leaf_removes_the_leaf(
        self,
        find_one_and_replace_mock,
        delete_many_mock,

    ):
        version, branch = 0, 'b2'

        num_nodes = len(self.col.log_tree)
        self.col.delete_subtree((version, branch))

        # in memory representation
        self.assertEqual(num_nodes - 1, len(self.col.log_tree))

        parent_version_tree_identifier = hashabledict(
            {'version': 1, 'branch': 'main'}
        )
        children = self.col.log_tree.children(parent_version_tree_identifier)
        self.assertEqual({
            self.named_versions_to_id['v2_main'],
            self.named_versions_to_id['v0_b3']
        },
            {c.tag for c in children}
        )

        delete_many_mock.assert_called_once_with(
            {"$or": [{'version': version, 'branch': branch}]}
        )
        find_one_and_replace_mock.assert_called_once_with(
            filter={'_id': self.named_versions_to_id['v1_main']},
            update={
                "$set": {
                    "next": [
                        self.named_versions_to_id['v2_main'],
                        self.named_versions_to_id['v0_b3'],
                    ]}
            },
        )

    def test_delete_subtree(self):
        self.col.delete_subtree((1, 'main'))

        self.assertEqual(2, len(self.col.log_tree))

        children = self.col.log_tree.children(self.col.log_tree.root)
        self.assertEqual(
            {self.named_versions_to_id['v0_b1']},
            {c.tag for c in children}
        )

        nodes = list(self.col.find({}))
        self.assertEqual(2, len(nodes))

        nodes = {n['_id']: n for n in nodes}
        self.assertEqual({
            self.named_versions_to_id['v0_main'],
            self.named_versions_to_id['v0_b1']
        },
            set(nodes.keys())
        )

        self.assertEqual(
            [self.named_versions_to_id['v0_b1']],
            nodes[self.named_versions_to_id['v0_main']]['next']
        )

    def test_add_log_entry_raises_error_if_prev_version_does_not_exist(self):
        with self.assertRaises(vc_errors.InvalidCollectionVersion):
            self.col.add_log_entry(
                previous_version=1,
                previous_branch='b1',
                current_branch='b1',
                message='this will not be recorded',
                timestamp=datetime.datetime.utcnow()
            )

    @patch.object(pymongo.collection.Collection, 'insert_one')
    @patch.object(pymongo.collection.Collection, 'find_one_and_update')
    def test_add_version_on_a_new_branch(
        self,
        find_one_and_replace_mock,
        insert_one_mock,
    ):
        args = dict(
            previous_version=0,
            previous_branch='main',
            current_branch='b5',
            message='first version on b5',
            timestamp=datetime.datetime.utcnow(),
            with_id=ObjectId()
        )
        inserted_result_mock = MagicMock()
        inserted_result_mock.inserted_id = args['with_id']

        insert_one_mock.return_value = inserted_result_mock
        self.col.add_log_entry(**args)
        insert_one_mock.assert_called_once()
        find_one_and_replace_mock.assert_called_once()

        # something was added
        n_entries = len(self.log_entries)
        self.assertEqual(n_entries + 1, len(self.col.log_tree))

        # the version was added in the correct place
        children = self.col.log_tree.children(self.col.log_tree.root)
        self.assertEqual(3, len(children))
        self.assertIn(args['with_id'], {c.tag for c in children})

    @patch.object(pymongo.collection.Collection, 'insert_one')
    @patch.object(pymongo.collection.Collection, 'find_one_and_update')
    def test_add_root_version(
        self,
        find_one_and_replace_mock,
        insert_one_mock,
    ):
        self.col.reset()

        args = dict(
            previous_version=-1,
            previous_branch=None,
            current_branch='main',
            message='root',
            timestamp=datetime.datetime.utcnow(),
            with_id=ObjectId()
        )

        inserted_result_mock = MagicMock()
        inserted_result_mock.inserted_id = args['with_id']
        insert_one_mock.return_value = inserted_result_mock

        self.col.add_log_entry(**args)

        find_one_and_replace_mock.assert_not_called()

        insert_one_mock.assert_called_once_with({
            '_id': args['with_id'],
            'version': 0,
            'branch': args['current_branch'],
            'message': args['message'],
            'timestamp': args['timestamp'],
            'prev': None,
            'next': []
        })

    @patch.object(pymongo.collection.Collection, 'insert_one')
    def test_add_log_entry_with_on_the_same_branch(self, insert_one_mock):
        args = dict(
            previous_version=0,
            previous_branch=None,
            current_branch='b4',
            message='a version',
            timestamp=datetime.datetime.utcnow(),
            with_id=ObjectId()
        )
        inserted_result_mock = MagicMock()
        inserted_result_mock.inserted_id = args['with_id']
        insert_one_mock.return_value = inserted_result_mock
        self.col.add_log_entry(**args)

        insert_one_mock.assert_called_once_with({
            '_id': args['with_id'],
            'version': 1,
            'branch': args['current_branch'],
            'message': args['message'],
            'timestamp': args['timestamp'],
            'prev': self.named_versions_to_id['v0_b4'],
            'next': []
        })

    def test_get_path_between_non_existing_versions(self):
        # invalid source
        with self.assertRaises(vc_errors.InvalidCollectionVersion):
            self.col.get_path_between_versions((0, 'other'), (2, 'main'))

        # invalid target
        with self.assertRaises(vc_errors.InvalidCollectionVersion):
            self.col.get_path_between_versions((2, 'main'), (2, 'brr'))

        # invalid source and target
        with self.assertRaises(vc_errors.InvalidCollectionVersion):
            self.col.get_path_between_versions((0, 'other'), (2, 'brrrr'))

    def test_path_between_a_version_and_itself_is_empty(self):
        path = self.col.get_path_between_versions((1, 'main'), (1, 'main'))
        self.assertEqual(0, len(path))

    def _assertOrderedEqual(self, d1, d2):
        d1 = OrderedDict(d1)
        d2 = OrderedDict(d2)
        self.assertEqual(d1, d2)

    def test_get_path_between_versions_linear(self):
        # forward
        self._assertOrderedEqual(
            {(0, 'main'): 1, (1, 'main'): 1, (2, 'main'): 1},
            self.col.get_path_between_versions((0, 'main'), (2, 'main'))
        )

        # reverse
        self._assertOrderedEqual(
            {(2, 'main'): -1, (1, 'main'): -1, (0, 'main'): -1},
            self.col.get_path_between_versions((2, 'main'), (0, 'main'))
        )

        # path of length 1
        self._assertOrderedEqual(
            {(0, 'b3'): -1, (1, 'main'): -1},
            self.col.get_path_between_versions((0, 'b3'), (1, 'main'))
        )

    def test_get_path_between_version_same_level(self):
        self._assertOrderedEqual(
            {(2, 'main'): -1, (1, 'main'): 0, (0, 'b3'): 1},
            self.col.get_path_between_versions((2, 'main'), (0, 'b3'))
        )

    def test_get_path_between_versions(self):
        self._assertOrderedEqual(
            {(0, 'b4'): -1, (0, 'b3'): -1, (1, 'main'): 0, (2, 'main'): 1},
            self.col.get_path_between_versions((0, 'b4'), (2, 'main'))
        )
        self._assertOrderedEqual(
            {(2, 'main'): -1, (1, 'main'): 0, (0, 'b3'): 1, (0, 'b4'): 1},
            self.col.get_path_between_versions((2, 'main'), (0, 'b4'))
        )

    def test_rebranch_root_of_the_tree_raises_error(self):
        with self.assertRaises(ValueError):
            self.col.rebranch((0, 'main'), 'main_v2')

    def test_rebranch_leaf_with_one_version_per_branch(self):
        self.col.rebranch((0, 'b1'), 'new')
        old_version = hashabledict({'version': 0, 'branch': 'b1'})
        self.assertIsNone(self.col._log_tree.get_node(old_version))

        new_version = hashabledict({'version': 0, 'branch': 'new'})
        node = self.col._log_tree.get_node(new_version)
        self.assertEqual(node.data.branch, 'new')
        self.assertEqual(
            node.data.prev,
            self.named_versions_to_id['v0_main']
        )

    def test_rebranch_subtree(self):
        self.col.rebranch((0, 'b3'), 'new')
        node = self.col._log_tree.get_node(hashabledict(
            {'version': 0, 'branch': 'new'}
        ))
        self.assertEqual(node.data.branch, 'new')
        self.assertEqual(
            node.data.prev,
            self.named_versions_to_id['v1_main']
        )

        c1, c2 = self.col._log_tree.children(node.identifier)

        children = dict()
        if c1.data.branch == 'new':
            children['new'] = c1
            children['b4'] = c2
        else:
            children['new'] = c2
            children['b4'] = c1

        self.assertEqual(children['new'].data.branch, 'new')
        self.assertEqual(children['new'].data.version, 1)
        self.assertEqual(
            children['new'].data.prev,
            self.named_versions_to_id['v0_b3']
        )