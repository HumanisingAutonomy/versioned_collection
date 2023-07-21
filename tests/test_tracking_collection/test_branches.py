from unittest import TestCase
from unittest.mock import patch

import pymongo

import versioned_collection.errors as vc_errors
from tests.test_tracking_collection.in_memory_database import \
    InMemoryDatabaseSetup
from versioned_collection.collection.tracking_collections import \
    BranchesCollection


class TestBranchesCollectionSchema(TestCase):
    @staticmethod
    def _get_main_args():
        return {
            'name': 'main',
            'points_to_collection_version': 0,
            'points_to_branch': 'main'
        }

    def test_equal_branches(self):
        args = self._get_main_args()
        b1 = BranchesCollection.SCHEMA(**args)
        b2 = BranchesCollection.SCHEMA(**args)
        self.assertEqual(b1, b2)

    def test_branches_are_equal_with_themselves(self):
        b = BranchesCollection.SCHEMA(**self._get_main_args())
        self.assertEqual(b, b)

    def test_different_branches_are_not_equal(self):
        args = self._get_main_args()
        b1 = BranchesCollection.SCHEMA(**args)
        args['name'] = 'some_branch'
        b2 = BranchesCollection.SCHEMA(**args)
        self.assertNotEqual(b1, b2)

    def test_branches_not_equal_with_none(self):
        b = BranchesCollection.SCHEMA(**self._get_main_args())
        self.assertNotEqual(b, None)
        self.assertNotEqual(None, b)

    def test_branches_are_hashable(self):
        b = BranchesCollection.SCHEMA(**self._get_main_args())
        self.assertIsNotNone(hash(b))
        some_set = set()
        some_set.add(b)


class TestBranchesCollection(InMemoryDatabaseSetup):

    def setUp(self) -> None:
        self.collection = BranchesCollection(self.database, 'col')
        self.collection.build()

    def tearDown(self) -> None:
        self.collection.drop()

    def test_building_the_collection_creates_the_main_branch(self):
        col = BranchesCollection(self.database, 'fresh_collection')
        with patch.object(pymongo.collection.Collection, 'insert_one') as mock:
            self.assertTrue(col.build())
            mock.assert_called_with({
                'name': 'main',
                'points_to_collection_version': 0,
                'points_to_branch': 'main'
            })
        col.drop()

    def test_building_the_collection_creates_the_main_branch_integration(self):
        col = BranchesCollection(self.database, 'fresh_collection')
        self.assertTrue(col.build())
        self.assertTrue(col.has_branch(branch_name='main'))
        col.drop()

    def test_building_an_existing_collection_does_nothing(self):
        self.assertFalse(self.collection.build())

    def test_creating_an_existing_branch_not_allowed(self):
        with self.assertRaisesRegex(ValueError, 'main already exists'):
            self.collection.create_branch(
                branch='main',
                pointing_to_collection_version=0,
                pointing_to_branch='main'
            )

    def test_creating_a_branch_adds_it_to_the_database(self):
        with patch.object(pymongo.collection.Collection, 'insert_one') as mock:
            self.collection.create_branch(
                branch='branch',
                pointing_to_collection_version=0,
                pointing_to_branch='main'
            )
            mock.assert_called_once_with({
                'name': 'branch',
                'points_to_collection_version': 0,
                'points_to_branch': 'main'
            })

    def test_has_branch_with_existing_branch(self):
        with patch.object(pymongo.collection.Collection, 'find_one') as mock:
            mock.return_value = object()
            self.assertTrue(self.collection.has_branch('main'))
            mock.assert_called_once_with({'name': 'main'})

    def test_has_branch_with_non_existing_branch(self):
        with patch.object(pymongo.collection.Collection, 'find_one') as mock:
            mock.return_value = None
            self.assertFalse(self.collection.has_branch('brr'))
            mock.assert_called_once_with({'name': 'brr'})

    def test_get_branch_with_non_existing_branch_raises_exception(self):
        with patch.object(pymongo.collection.Collection, 'find_one') as mock:
            mock.return_value = None
            with self.assertRaises(vc_errors.BranchNotFound):
                self.collection.get_branch('random')

    def test_get_branch_returns_the_branch_object(self):
        main_br = dict(
            name='main',
            points_to_collection_version=0,
            points_to_branch='main',
        )
        with patch.object(pymongo.collection.Collection, 'find_one') as mock:
            mock.return_value = main_br
            mock.return_value['_id'] = 0
            branch = self.collection.get_branch('main')
            self.assertEqual(BranchesCollection.SCHEMA(**main_br), branch)

    def test_update_non_existing_branch_raises_error(self):
        with patch.object(BranchesCollection, 'has_branch') as mock:
            mock.return_value = False
            with self.assertRaises(vc_errors.BranchNotFound):
                self.collection.update_branch('branch', 0, '')
            mock.assert_called_once_with('branch')

    @patch.object(pymongo.collection.Collection, 'find_one_and_replace')
    def test_update_branch_info_but_keeping_the_name(self, mock):
        data = {
            'points_to_collection_version': 1,
            'points_to_branch': 'main'
        }
        self.collection.update_branch(
            branch='main',
            pointing_to_collection_version=data['points_to_collection_version'],
            pointing_to_branch=data['points_to_branch'],
        )
        mock.assert_called_once_with(
            filter={'name': 'main'},
            replacement={
                'name': 'main',
                **data
            }
        )

    @patch.object(BranchesCollection, 'has_branch')
    @patch.object(pymongo.collection.Collection, 'find_one_and_replace')
    def test_rename_branch(self, find_one_and_replace, has_branch):
        has_branch.return_value = True
        self.collection.update_branch(
            branch='new_main',
            pointing_to_collection_version=0,
            pointing_to_branch='main',
        )

        has_branch.assert_called_once_with('new_main')
        find_one_and_replace.assert_called_once_with(
            filter={'name': 'new_main'},
            replacement={
                'name': 'new_main',
                'points_to_collection_version': 0,
                'points_to_branch': 'main'
            }
        )

    def test_delete_branch(self):
        with patch.object(pymongo.collection.Collection, 'delete_one') as mock:
            self.collection.delete_branch('main')
            mock.assert_called_once_with({'name': 'main'})

    def test_delete_branches(self):
        branches_to_delete = ['main', 'other', 'yet_another_one']
        with patch.object(pymongo.collection.Collection, 'delete_many') as mock:
            self.collection.delete_branches(branches_to_delete)
            mock.assert_called_once_with({'name': {'$in': branches_to_delete}})

    def test_get_empty_branches_when_none_exist_returns_and_empty_set(self):
        empty_branches = self.collection.get_empty_branches()
        self.assertEqual(0, len(empty_branches))

    def test_get_empty_branches(self):
        args = {
            'pointing_to_collection_version': 0,
            'pointing_to_branch': 'main'
        }
        self.collection.create_branch(
            branch='empty_1',
            **args
        )

        branches = self.collection.get_empty_branches()
        self.assertEqual(1, len(branches))
        branch = branches.pop()
        self.assertEqual(branch.name, 'empty_1')

        self.collection.create_branch(
            branch='empty_2',
            **args
        )
        branches = self.collection.get_empty_branches()
        self.assertEqual(2, len(branches))

    def test_get_empty_child_branches_when_none_exist_returns_empty_list(self):
        branches = self.collection.get_empty_child_branches('main')
        self.assertEqual(0, len(branches))

    def _setup_empty_branches1(self):
        self.collection.create_branch(
            branch='empty_1',
            pointing_to_branch='main',
            pointing_to_collection_version=0
        )

        self.collection.create_branch(
            branch='empty_2',
            pointing_to_branch='main',
            pointing_to_collection_version=3
        )

    def test_get_all_empty_child_branches(self):
        self._setup_empty_branches1()

        branches = self.collection.get_empty_child_branches('main')
        self.assertEqual(2, len(branches))

    def test_get_empty_branches_after_a_version(self):
        self._setup_empty_branches1()
        branches = self.collection.get_empty_child_branches(
            'main', after_version=1
        )
        self.assertEqual(1, len(branches))
        branch = branches[0]
        self.assertEqual(branch.name, 'empty_2')

    def test_get_empty_child_branches_returns_only_children(self):
        self._setup_empty_branches1()
        self.collection.create_branch(
            branch='empty_2_0',
            pointing_to_branch='empty_2',
            pointing_to_collection_version=0
        )
        self.collection.create_branch(
            branch='empty_2_1',
            pointing_to_branch='empty_2',
            pointing_to_collection_version=1
        )

        branches = self.collection.get_empty_child_branches(branch='empty_2')
        self.assertEqual(2, len(branches))
        b0, b1 = sorted(branches, key=lambda b: b.name)
        self.assertEqual('empty_2_0', b0.name)
        self.assertEqual('empty_2_1', b1.name)

    def test_get_branch_names(self):
        mock_branches = ['b1', 'b2', 'b3']
        with patch.object(pymongo.collection.Collection, 'distinct') as mock:
            mock.return_value = mock_branches
            branches = self.collection.get_branch_names()
            self.assertEqual(set(mock_branches), branches)

    def test_get_branch_names_integration(self):
        expected_branches = ['b1', 'b2', 'b3']
        for b in expected_branches:
            self.collection.create_branch(b, 0, 'main')
        expected_branches.append('main')

        actual_branches = self.collection.get_branch_names()
        self.assertEqual(set(expected_branches), actual_branches)
