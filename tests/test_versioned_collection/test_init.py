from time import sleep

from tests.test_versioned_collection.common import _BaseTest, SLEEP_TIME, User
from versioned_collection.errors import (
    CollectionAlreadyInitialised,
)


class TestVersionedCollectionInit(_BaseTest):

    def test_right_collections_are_created_at_initialisation(self):
        current_collections = self.database.list_collection_names()
        self.assertEqual(len(current_collections), 0)

        self.user_collection.init()
        current_collections = self.database.list_collection_names()
        for col in self.user_collection._tracking_collections:
            self.assertIn(col.name, current_collections)

    def test_tracking_collections_correctly_reloaded(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init()
        del self.user_collection
        # reloading
        self.user_collection = User(self.database)
        self.assertTrue(self.user_collection._tracked)
        current_collections = self.database.list_collection_names()
        self.assertTrue(len(current_collections) > 0)
        for col in self.user_collection._tracking_collections:
            self.assertIn(col.name, current_collections)

    def test_initialising_twice_causes_error(self):
        self.user_collection.init()
        self.assertTrue(self.user_collection._tracked)

        with self.assertRaises(CollectionAlreadyInitialised):
            self.user_collection.init()

    def test_init_correctly_starts_listeners(self):
        # to test this we can just add something to a tracked collection and
        # make sure that the correct data is added to the modified collection

        self.user_collection.init()

        _id = self.user_collection.insert_one(self.DOCUMENT).inserted_id
        sleep(SLEEP_TIME)
        doc_id = self.user_collection._modified_collection.find_one({})['id']
        self.assertEqual(_id, doc_id)

    def test_replica_in_correct_state_after_initialisation(self):
        # insert into collection
        self.user_collection.insert_one(self.DOCUMENT)
        # start tracking the collection
        self.user_collection.init()

        doc = self.user_collection.find_one({})  # the inserted document
        replica_doc = self.user_collection._replica_collection.find_one({})
        self.assertEqual(doc, replica_doc)

    def test_logs_collection_correctly_built(self):
        message = "test init"
        self.user_collection.init(message)

        logs = list(self.user_collection._log_collection.find({}))
        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0]['message'], message)

    def test_metadata_collection_correctly_built(self):
        self.user_collection.init()
        metadata = list(self.user_collection._meta_collection.find({}))
        self.assertEqual(len(metadata), 1)
        self.assertEqual(metadata[0]['current_version'], 0)
        self.assertEqual(metadata[0]['detached'], False)

    def test_init_creates_main_branch(self):
        self.user_collection.init()
        self.assertEqual(
            self.user_collection._branches_collection.count_documents({}), 1
        )
        data = self.user_collection._branches_collection.get_branch('main')
        self.assertIsNotNone(data)
