from unittest.mock import create_autospec

from tests.tracking_collection.in_memory_database import (
    InMemoryDatabaseSetup
)
from versioned_collection.collection.tracking_collections import (
    StashContainer, StashCollection
)


class TestStashCollection(InMemoryDatabaseSetup):
    def setUp(self) -> None:
        self._collection_name = 'col'
        self.container = StashContainer(self.database, self._collection_name)

        self.main_collection = create_autospec(StashCollection)(
            self.database, self._collection_name
        )
        self.main_collection.name = f'__stash_{self._collection_name}'

        mod_name = f'modified_{self._collection_name}'
        self.modified_collection = create_autospec(StashCollection)(
            self.database, mod_name
        )
        self.modified_collection.name = f'__stash_{mod_name}'

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

