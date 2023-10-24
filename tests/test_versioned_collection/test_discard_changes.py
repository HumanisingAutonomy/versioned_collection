from copy import deepcopy

from tests.test_versioned_collection.common import _BaseTest
from versioned_collection.errors import (
    InvalidOperation,
)


class TestVersionedCollectionDiscardChanges(_BaseTest):

    def test_discarding_changes_of_untracked_collections(self):
        self.assertFalse(self.user_collection.discard_changes())

    def test_discarding_changes_allows_checking_out(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        with self.assertRaises(InvalidOperation):
            self.user_collection.checkout(0)

        self.user_collection.discard_changes()
        self.user_collection.checkout(0)

    def test_discarding_changes_rolls_collection_back(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.insert_one(self.DOCUMENT2)
        self.user_collection.insert_one({'name': 'Gauss', 'emails': []})
        original_doc = deepcopy(self.DOCUMENT)
        self.user_collection.init()
        self.user_collection.find_one_and_update(
            filter={'name': 'Goethe'}, update={"$set": {'name': 'GOETHE'}}
        )
        self.user_collection.delete_one({'name': 'Euler'})
        self.assertEqual(2, self.user_collection.count_documents({}))
        self.user_collection.discard_changes()
        self.assertEqual(3, self.user_collection.count_documents({}))
        doc = self.user_collection.find_one({'name': 'Goethe'})
        self.assertEqual(original_doc, doc)
        doc = self.user_collection.find_one({'name': 'Euler'})
        self.assertEqual(self.DOCUMENT2, doc)
        doc = self.user_collection.find_one({'name': 'Gauss'})
        self.assertEqual(3, len(doc))
        self.assertEqual([], doc['emails'])

    def test_discarding_changes_handles_inserts(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT2)
        self.assertEqual(2, self.user_collection.count_documents({}))
        self.user_collection.discard_changes()
        self.assertEqual(1, self.user_collection.count_documents({}))

    def test_discarding_changes_handles_deletes(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.insert_one(self.DOCUMENT2)
        self.user_collection.init()
        self.assertEqual(2, self.user_collection.count_documents({}))
        self.user_collection.delete_one({'name': 'Euler'})
        self.assertEqual(1, self.user_collection.count_documents({}))
        self.user_collection.discard_changes()
        self.assertEqual(2, self.user_collection.count_documents({}))

    def test_discarding_changes_handles_upserts(self):
        self.user_collection.init()
        self.user_collection.find_one_and_update(
            filter={'name': 'Goethe'},
            update={"$set": {'name': 'Goethe'}},
            upsert=True,
        )
        self.assertTrue(self.user_collection.has_changes())
        self.assertTrue(self.user_collection.discard_changes())
        self.assertFalse(self.user_collection.has_changes())
        self.assertEqual(0, self.user_collection.count_documents({}))

    def test_discard_changes_handles_upserts2(self):
        self.user_collection.init()
        self.user_collection.find_one_and_replace(
            filter={'name': 'Goethe'}, replacement=self.DOCUMENT, upsert=True
        )
        self.assertTrue(self.user_collection.has_changes())
        self.assertTrue(self.user_collection.discard_changes())
        self.assertFalse(self.user_collection.has_changes())
        self.assertEqual(0, self.user_collection.count_documents({}))

    def test_discard_changes_handles_upserts3(self):
        self.user_collection.init()
        self.user_collection.update_one(
            filter={'name': 'Goethe'},
            update={"$set": {'name': 'Goethe'}},
            upsert=True,
        )
        self.assertTrue(self.user_collection.has_changes())
        self.assertTrue(self.user_collection.discard_changes())
        self.assertFalse(self.user_collection.has_changes())
        self.assertEqual(0, self.user_collection.count_documents({}))

    def test_order_of_operations(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.delete_one({'_id': self.DOCUMENT['_id']})
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.update_one(
            {'_id': self.DOCUMENT['_id']}, {"$set": {'age': 69}}
        )
        self.user_collection.delete_one({'_id': self.DOCUMENT['_id']})
        self.assertEqual(0, self.user_collection.count_documents({}))
        self.user_collection.discard_changes()
        self.assertEqual(0, self.user_collection.count_documents({}))

    def test_order_of_operations_2(self):
        self.user_collection.insert_one(self.DOCUMENT)
        original_doc = deepcopy(self.DOCUMENT)
        self.user_collection.init('v0')
        self.user_collection.update_one(
            {'_id': self.DOCUMENT['_id']}, {"$set": {'age': 69}}
        )
        self.user_collection.delete_one({'_id': self.DOCUMENT['_id']})
        self.user_collection.discard_changes()
        self.assertEqual(1, self.user_collection.count_documents({}))
        self.assertEqual(original_doc, self.user_collection.find_one({}))
