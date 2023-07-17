from unittest.mock import patch

import pymongo.collection

from tests.tracking_collection.in_memory_database import InMemoryDatabaseSetup
from versioned_collection.collection.tracking_collections import \
    MetadataCollection


class TestMetadataCollection(InMemoryDatabaseSetup):

    def setUp(self) -> None:
        self.collection = MetadataCollection(self.database, 'col')
        self.collection.build()
        self._original_metadata = MetadataCollection.SCHEMA(
            current_version=0,
            current_branch='main',
            detached=False,
            changed=False,
            has_stash=False,
            has_conflicts=False,
        )

    def tearDown(self) -> None:
        self.collection.drop()

    def test_metadata_build_returns_false_if_collection_exists(self):
        self.assertFalse(self.collection.build())

    def test_metadata_build_creates_the_original_metadata_pointer(self):
        # new collection to capture the pymongo calls
        col = MetadataCollection(self.database, 'other')
        with patch.object(pymongo.collection.Collection, 'insert_one') as mock:
            self.assertTrue(col.build())
            mock.assert_called_once_with(self._original_metadata.__dict__)

    def test_retrieving_cached_metadata_does_not_query_the_database(self):
        with patch.object(pymongo.collection.Collection, 'find_one') as mock:
            metadata = self.collection.metadata
            mock.assert_not_called()
            self.assertEqual(self._original_metadata, metadata)

    def test_retrieving_metadata_for_a_new_object_queries_the_database(self):
        col = MetadataCollection(self.database, 'col')
        with patch.object(pymongo.collection.Collection, 'find_one') as mock:
            mock.return_value = self._original_metadata.__dict__
            metadata = col.metadata
            mock.assert_called_with({}, projection={'_id': False})
            self.assertEqual(self._original_metadata, metadata)

    def test_set_metadata_with_the_same_values_does_not_change_database(self):
        with patch.object(
            pymongo.collection.Collection,
            'find_one_and_replace'
        ) as mock:
            self.collection.set_metadata()
            self.collection.set_metadata(
                current_version=self._original_metadata.current_version,
                current_branch=self._original_metadata.current_branch,
                detached=self._original_metadata.detached,
                changed=self._original_metadata.changed,
                has_stash=self._original_metadata.has_stash,
                has_conflicts=self._original_metadata.has_conflicts,
            )
            mock.assert_not_called()

    def test_setting_metadata_replaces_the_metadata_document(self):
        new_metadata = dict(
            current_version=10,
            current_branch='brr',
            detached=True,
            changed=True,
            has_stash=False,
            has_conflicts=True,
        )
        with patch.object(
            pymongo.collection.Collection,
            'find_one_and_replace'
        ) as mock:
            self.collection.set_metadata(**new_metadata)
            mock.assert_called_once()
            args = mock.call_args[1]
            self.assertEqual(args['filter'], {})

            args['replacement'].pop('_id')
            self.assertEqual(new_metadata, args['replacement'])