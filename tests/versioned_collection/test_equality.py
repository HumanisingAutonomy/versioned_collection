from copy import deepcopy
from time import sleep
from typing import List
from unittest import TestCase

import pymongo
from bson import ObjectId
from pymongo import MongoClient

from versioned_collection import VersionedCollection
from versioned_collection.errors import (
    CollectionAlreadyInitialised,
    InvalidOperation,
    InvalidCollectionVersion,
    BranchNotFound,
    InvalidCollectionState,
    AutoMergeFailedError,
)
from versioned_collection.utils.serialization import stringify_object_id

from .common import _BaseTest, SLEEP_TIME, _RemoteBaseTest


class TestVersionCollectionEquality(_RemoteBaseTest):

    def test_untracked_collections_are_equal_if_they_have_the_same_name(self):
        self.assertEqual(self.local, self.remote)

    def test_tracked_and_untracked_collections_are_not_equal(self):
        self.local.init()
        self.assertNotEqual(self.local, self.remote)

    def test_collections_with_diverging_roots_are_not_equal(self):
        self.local.init()
        self.remote.init()
        self.assertNotEqual(self.local, self.remote)

    def test_empty_collections_are_equal(self):
        self.local.init()
        self.local.push(self.remote)
        self.assertEqual(self.local, self.remote)
        self.assertTrue(self.local <= self.remote)
        self.assertTrue(self.local >= self.remote)

    def test_collection_not_equal_to_none(self):
        self.assertNotEqual(self.local, None)

    def test_versioned_collections_type_check_equality(self):
        other = pymongo.collection.Collection(self.db_local, 'other')
        self.assertNotEqual(self.local, other)
        other.drop()

    def test_collection_equal_to_self(self):
        self.assertEqual(self.local, self.local)

    def test_collections_not_equal_if_names_not_equal(self):
        other = VersionedCollection(self.db_local, 'User')
        self.assertNotEqual(self.local, other)
        other.drop()

    def test_collections_not_equal_if_different_number_of_branches(self):
        self.local.init()
        self.remote.init()
        self.local.create_branch('brr')
        self.assertNotEqual(self.local, self.remote)

    def test_less_with_different_collection_types(self):
        other = pymongo.collection.Collection(self.db_local, 'User')
        with self.assertRaisesRegex(
            TypeError, "not supported between instances of"
        ):
            _ = self.local < other
        other.drop()

    def test_collection_less_than_self(self):
        self.assertFalse(self.local < self.local)
        self.assertTrue(self.local <= self.local)

    def test_comparing_with_less_throws_error_when_names_different(self):
        other = VersionedCollection(self.db_local, 'User')
        with self.assertRaisesRegex(ValueError, "different names"):
            _ = self.local < other
        other.drop()

    def test_untracked_collections_are_less_then_each_other(self):
        self.assertFalse(self.remote.is_tracked())
        self.assertFalse(self.local.is_tracked())
        self.assertTrue(self.remote < self.local)

    def test_less_with_one_tracked_one_untracked(self):
        self.remote.init()
        self.assertFalse(self.local < self.remote)
        self.remote.drop()

        self.local.init()
        self.assertFalse(self.remote.is_tracked())
        self.assertFalse(self.local < self.remote)

    def test_less_with_different_number_of_branches(self):
        self.local.init()
        self.remote.init()
        self.local.create_branch('brr')
        self.assertFalse(self.local < self.remote)
        self.assertTrue(self.local > self.remote)

    def test_less(self):
        self.local.init()
        self.assertTrue(self.local.push(self.remote))
        self.local.insert_one(self.DOCUMENT)
        self.assertTrue(self.local.register('v1'))

        self.assertTrue(self.local != self.remote)
        self.assertTrue(self.remote < self.local)
        self.assertTrue(self.remote <= self.local)

    def test_less_with_more_branches(self):
        self.local.init()
        self.local.push(self.remote)
        self.local.create_branch('branch')

        self.assertTrue(self.local != self.remote)
        self.assertTrue(self.remote < self.local)

    def _setup_local_remote(self):
        self.local.init('0_m')
        self.local.insert_one(self.DOCUMENT)
        self.assertTrue(self.local.register('1_m'))
        self.local.checkout(0)
        self.local.create_branch('b')
        self.local.insert_one(self.DOCUMENT2)
        self.assertTrue(self.local.register('0_b'))

        self.assertTrue(self.local.push(self.remote, 'main'))
        self.assertTrue(self.local.push(self.remote, 'b'))

        self.assertEqual(self.local, self.remote)

    def test_less_tree_subset(self):
        self._setup_local_remote()

        self.local.insert_one(self.DOCUMENT3)
        self.assertTrue(self.local.register('1_b'))
        self.local.checkout(1, branch='main')
        self.assertTrue(1, self.local.version)
        self.assertTrue('main', self.local.branch)
        self.assertFalse(self.local.is_detached())
        self.assertFalse(self.local.has_changes())

        self.local.insert_one(self.DOCUMENT3)
        self.assertTrue(self.local.register('2_m'))

        self.assertTrue(self.local != self.remote)
        self.assertTrue(self.local > self.remote)
        self.assertTrue(self.local >= self.remote)

    def test_different_branch_length(self):
        self._setup_local_remote()

        self.local.checkout(1, 'main')
        self.local.insert_one(self.DOCUMENT3)
        self.assertTrue(self.local.register('2_main'))

        self.assertTrue(self.remote < self.local)

        self.remote.checkout(branch='b')
        self.remote.insert_one(self.DOCUMENT3)
        self.assertTrue(self.remote.register('1_b'))

        self.assertNotEqual(self.local, self.remote)
        self.assertFalse(self.local < self.remote)
        self.assertFalse(self.remote < self.local)

    def test_different_set_of_empty_branches(self):
        self.local.init()
        self.local.push(self.remote)
        self.assertEqual(self.local, self.remote)

        self.local.create_branch('b_local')
        self.remote.create_branch('b_remote')

        self.assertNotEqual(self.local, self.remote)
        self.assertFalse(self.local < self.remote)
        self.assertFalse(self.local <= self.remote)
