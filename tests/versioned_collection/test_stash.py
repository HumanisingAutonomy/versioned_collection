from versioned_collection.errors import (
    InvalidOperation,
)

from .common import _BaseTest


class TestVersionedCollectionStash(_BaseTest):

    def test_stash_untracked_collection(self):
        self.assertFalse(self.user_collection.stash())

    def test_stashing_when_no_changes_exist_returns_false(self):
        self.user_collection.init()
        self.assertFalse(self.user_collection.has_changes())
        self.assertFalse(self.user_collection.stash())

    def test_stashing_clears_changes(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        self.assertTrue(self.user_collection.stash())

        self.assertFalse(self.user_collection.has_changes())
        self.assertEqual(0, self.user_collection.count_documents({}))

    def test_applying_the_stash_restores_the_documents(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.stash()
        self.assertTrue(self.user_collection.has_stash())

        self.assertEqual(0, self.user_collection.count_documents({}))
        self.assertTrue(self.user_collection.stash_apply())
        self.assertFalse(self.user_collection.has_stash())

        self.assertEqual(1, self.user_collection.count_documents({}))
        self.assertEqual(self.DOCUMENT, self.user_collection.find_one({}))
        self.assertTrue(self.user_collection.has_changes())

    def test_applying_the_stash_restores_the_trackers(self):
        self.user_collection.init('v0')
        self.user_collection.insert_one(self.DOCUMENT)
        self.assertTrue(self.user_collection.stash())
        self.assertFalse(self.user_collection._modified_collection.exists())
        self.assertTrue(self.user_collection.stash_apply())
        self.assertTrue(self.user_collection._modified_collection.exists())

    def test_stash_handles_deleted_documents(self):
        # stashing and applying the stash restores the collection
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT2)
        self.user_collection.delete_one({'_id': self.DOCUMENT['_id']})

        self.user_collection.stash()
        self.assertEqual(1, self.user_collection.count_documents({}))
        self.assertEqual(self.DOCUMENT, self.user_collection.find_one({}))

        self.user_collection.stash_apply()
        self.assertEqual(1, self.user_collection.count_documents({}))
        self.assertEqual(self.DOCUMENT2, self.user_collection.find_one({}))

        self.user_collection.register('v1')
        self.user_collection.checkout(0)
        self.assertEqual(1, self.user_collection.count_documents({}))
        self.assertEqual(self.DOCUMENT, self.user_collection.find_one({}))

    def test_cannot_run_apply_stash_if_changes_exist(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.stash()
        self.user_collection.insert_one(self.DOCUMENT)
        with self.assertRaisesRegex(
            InvalidOperation,
            "Cannot apply stashed data because the collection has changes",
        ):
            self.user_collection.stash_apply()

    def test_discarding_stashed_data_when_does_not_exist_returns_false(self):
        self.assertFalse(self.user_collection.stash_discard())
        self.user_collection.init()
        self.assertFalse(self.user_collection.stash_discard())

    def test_discarding_stashed_data_clears_the_stash_area(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.stash()
        self.assertEqual(0, self.user_collection.count_documents({}))
        self.assertTrue(self.user_collection.has_stash())
        self.assertTrue(self.user_collection.stash_discard())
        self.assertFalse(self.user_collection.has_stash())
