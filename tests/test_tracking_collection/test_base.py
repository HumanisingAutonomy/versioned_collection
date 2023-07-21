from unittest.mock import patch

import pymongo
import pymongo.database

from tests.test_tracking_collection.in_memory_database import \
    InMemoryDatabaseSetup
from versioned_collection.collection.tracking_collections import \
    _BaseTrackerCollection  # noqa


class SomeCollection(_BaseTrackerCollection):
    _NAME_TEMPLATE = '__some_prefix_{}'


class TestBaseCollection(InMemoryDatabaseSetup):

    def setUp(self) -> None:
        self.collection = SomeCollection(self.database, 'someName')

    def tearDown(self) -> None:
        self.collection.drop()

    def test_collections_are_equal_if_they_have_the_same_name(self):
        self.assertEqual(
            self.collection,
            SomeCollection(self.database, 'someName')
        )

        self.assertNotEqual(
            self.collection,
            SomeCollection(self.database, 'someOtherName')
        )

    def test_building_a_collection_physically_creates_it(self):
        self.assertFalse(self.collection.exists())
        self.collection.build()
        self.assertTrue(self.collection.exists())

    def test_rebuilding_an_existing_collection_drops_it(self):
        self.collection.build()
        with patch.object(pymongo.collection.Collection, 'drop') as drop_mock:
            with patch.object(
                pymongo.database.Database,
                'create_collection'
            ) as create_collection_mock:
                self.collection.build()
                drop_mock.assert_called_once()
                create_collection_mock.assert_called_once_with(
                    self.collection.name
                )

    def test_collection_created_with_the_correct_name(self):
        self.assertEqual('__some_prefix_someName', self.collection.name)

    def test_rename_collection_changes_its_name_in_the_database(self):
        self.collection.build()
        self.assertTrue(self.collection.rename('someNewName'))
        same_collection_new_name = SomeCollection(
            self.database, name='someNewName'
        )
        self.assertTrue(same_collection_new_name.exists())

    def test_rename_does_nothing_if_collection_not_built(self):
        old_name = self.collection.name
        self.assertFalse(self.collection.rename('someNewName'))
        self.assertEqual(old_name, self.collection.name)
