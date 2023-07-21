from tests.test_versioned_collection.common import _BaseTest


class TestVersionedCollectionDrop(_BaseTest):

    def tearDown(self) -> None:
        super(_BaseTest, self).tearDown()

    def test_dropping_an_untracked_collection_removes_it(self):
        self.assertEqual(len(self.database.list_collection_names()), 0)
        self.user_collection.insert_one(self.DOCUMENT)
        self.assertEqual(len(self.database.list_collection_names()), 1)
        self.user_collection.drop()
        self.assertEqual(len(self.database.list_collection_names()), 0)

    def test_dropping_a_tracked_collection_removes_all_tracking_data(self):
        self.assertEqual(len(self.database.list_collection_names()), 0)
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init()
        self.assertEqual(
            len(self.database.list_collection_names()),
            len(self.user_collection._tracking_collections) + 2,
        )
        self.user_collection.drop()
        self.assertEqual(len(self.database.list_collection_names()), 0)

    def test_dropping_a_collection_stops_the_listeners(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init()
        self.assertTrue(self.user_collection._listener.is_listening())
        self.user_collection.drop()
        self.assertFalse(self.user_collection._listener.is_listening())
