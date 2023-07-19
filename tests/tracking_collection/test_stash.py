from typing import List, Dict, Any
from unittest.mock import create_autospec, MagicMock, call, ANY

import pymongo.collection
from bson import ObjectId

import versioned_collection.collection.tracking_collections
from tests.tracking_collection.in_memory_database import (
    InMemoryDatabaseSetup
)
from versioned_collection.collection.tracking_collections import (
    StashContainer, StashCollection
)


class TestStashCollection(InMemoryDatabaseSetup):
    def setUp(self) -> None:
        self.collection_name = 'col'
        self.container = StashContainer(self.database, self.collection_name)

        self.main_collection = create_autospec(StashCollection)(
            self.database, self.collection_name
        )
        self.main_collection.name = f'__stash_{self.collection_name}'

        self.mod_name = f'modified_{self.collection_name}'
        self.modified_collection = create_autospec(StashCollection)(
            self.database, self.mod_name
        )
        self.modified_collection.name = f'__stash_{self.mod_name}'

        self.container.main_collection = self.main_collection
        self.container.modified_collection = self.modified_collection

    def test_dropping_the_stash_container_drops_all_stash_collection(self):
        self.container.drop()
        self.main_collection.drop.assert_called_once()
        self.modified_collection.drop.assert_called_once()

    def test_renaming_the_container_renames_the_stash_collections(self):
        new_col_name = 'edison_copied_tesla'
        self.container.rename(new_col_name)
        self.main_collection.rename.assert_called_once_with(new_col_name)
        self.modified_collection.rename.assert_called_once_with(
            f'modified_{new_col_name}'
        )

    def test_if_only_one_stash_collection_exits_raises_assertion_error(self):
        self.main_collection.exists.return_value = True
        self.modified_collection.exists.return_value = False
        with self.assertRaises(AssertionError):
            self.container.exists()

        self.main_collection.exists.return_value = False
        self.modified_collection.exists.return_value = True
        with self.assertRaises(AssertionError):
            self.container.exists()

    def test_exists_normal_behaviour(self):
        self.main_collection.exists.return_value = False
        self.modified_collection.exists.return_value = False
        self.assertFalse(self.container.exists())

        self.main_collection.exists.return_value = True
        self.modified_collection.exists.return_value = True
        self.assertTrue(self.container.exists())

    def _build_source_collections(self):
        main_collection = create_autospec(pymongo.collection.Collection)(
            self.database, self.collection_name
        )
        modified_collection = create_autospec(
            versioned_collection.collection.tracking_collections.ModifiedCollection
        )(self.database, self.collection_name)
        return main_collection, modified_collection

    def test_stashing_data_drops_the_current_stashed_data(self):
        main_collection, modified_collection = self._build_source_collections()
        order = MagicMock()
        order.attach_mock(self.modified_collection, 'self_modified_col')
        order.attach_mock(self.main_collection, 'self_main_col')
        order.attach_mock(main_collection, 'main_col')
        order.attach_mock(modified_collection, 'modified_col')

        self.container.stash(main_collection, modified_collection)
        self.main_collection.drop.assert_called_once()
        self.modified_collection.drop.assert_called_once()

        calls = order.mock_calls
        self.assertEqual(call.self_modified_col.drop(), calls[0])
        # first drop, then do stuff
        self.assertEqual(call.self_main_col.drop(), calls[1])
        self.assertEqual(call.self_main_col.drop(), calls[1])
        self.assertEqual(call.modified_col.aggregate(ANY), calls[2])
        self.assertEqual(call.main_col.aggregate(ANY), calls[4])

    def test_stash(self):
        main_col, modified_col = self._build_source_collections()

        mod_doc_ids = [ObjectId() for _ in range(5)]
        modified_col.get_unique_modified_document_ids.return_value = mod_doc_ids

        self.container.stash(main_col, modified_col)

        # we want the `modified_collection` to be cloned
        modified_col.aggregate.assert_called_once_with([
            {"$match": {}},
            {"$out": self.modified_collection.name},
        ])

        # we want to clone only the modified documents from the main collection
        main_col.aggregate.assert_called_once_with([
            {"$match": {'_id': {"$in": mod_doc_ids}}},
            {"$out": self.main_collection.name},
        ])

    def _test_stash_apply(self, existing_ids: List[Dict[str, Any]]):
        main_col, modified_col = self._build_source_collections()
        modified_col.name = '__modified_col'

        modified_doc_ids = [{'ids': [ObjectId() for _ in range(3)]}]
        self.modified_collection.aggregate.return_value = iter(modified_doc_ids)

        main_col.find.return_value = existing_ids
        main_docs = [{'_id': 1}, {'_id': 2}, {'_id': 3}]
        self.main_collection.find.return_value = iter(main_docs)

        self.container.stash_apply(main_col, modified_col)

        if len(existing_ids) == 0:
            self.modified_collection.update_many.assert_not_called()
        else:
            _ex_ids = [d['_id'] for d in existing_ids]
            self.modified_collection.update_many.assert_called_once_with(
                filter={'id': {"$in": _ex_ids}, 'op': 'i'},
                update={"$set": {'op': 'u'}},
            )

        main_col.delete_many.assert_called_once_with(
            {'_id': {"$in": modified_doc_ids[0]['ids']}}
        )
        main_col.insert_many.assert_called_once_with(main_docs)

        self.assertEqual(
            call([{'$match': {}}, {'$out': '__modified_col'}]),
            self.modified_collection.aggregate.mock_calls[-1]
        )
        self.main_collection.drop.assert_called_once()
        self.modified_collection.drop.assert_called_once()

        self.assertEqual(call.drop(), self.main_collection.mock_calls[-1])
        self.assertEqual(call.drop(), self.modified_collection.mock_calls[-1])

    def test_stash_apply_without_existent_ids_in_target(self):
        self._test_stash_apply(existing_ids=[])

    def test_stash_apply_with_existing_ids_in_target(self):
        self._test_stash_apply(existing_ids=[{'_id': ObjectId()}])
