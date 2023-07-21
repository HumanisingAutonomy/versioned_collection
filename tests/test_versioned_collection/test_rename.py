from tests.test_versioned_collection.common import _BaseTest


class TestVersionedCollectionRename(_BaseTest):

    def test_renaming_an_untracked_collection_renames_it(self):
        original_name = self.user_collection.name
        self.user_collection.insert_one(self.DOCUMENT)
        current_collections = self.database.list_collection_names()
        self.assertIn(original_name, current_collections)

        new_name = 'USERS'
        self.user_collection = self.user_collection.rename(new_name=new_name)
        self.assertEqual(new_name, self.user_collection.name)
        current_collections = self.database.list_collection_names()
        self.assertIn(new_name, current_collections)
        self.assertNotIn(original_name, current_collections)

    def test_renaming_a_tracked_collection_renames_the_tracking_cols(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init()
        old_coll_names = [
            coll.name for coll in self.user_collection._tracking_collections
        ]
        old_coll_names.append(self.user_collection.name)

        new_name = 'USERS'
        self.user_collection = self.user_collection.rename(new_name)
        new_coll_names = [
            coll.name for coll in self.user_collection._tracking_collections
        ]
        new_coll_names.append(new_name)

        current_collections = self.database.list_collection_names()
        self.assertEqual(len(current_collections) - 1, len(new_coll_names))
        for coll in new_coll_names:
            self.assertIn(coll, current_collections)
