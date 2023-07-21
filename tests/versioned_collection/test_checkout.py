from time import sleep

from versioned_collection.errors import (
    InvalidOperation,
    InvalidCollectionVersion,
    InvalidCollectionState,
)
from .common import _BaseTest, SLEEP_TIME


class TestVersionedCollectionCheckout(_BaseTest):

    def test_checkout_on_untracked_collection(self):
        self.assertFalse(self.user_collection.checkout(0))

    def test_checkout_called_with_no_params_throws_error(self):
        self.user_collection.init()
        with self.assertRaises(ValueError):
            self.user_collection.checkout()

    def test_checking_out_the_same_version_does_nothing(self):
        self.user_collection.init()
        self.assertEqual(self.user_collection.version, 0)
        self.assertTrue(self.user_collection.checkout(0))

    def test_checkout_when_changes_are_made(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        with self.assertRaises(InvalidOperation):
            self.user_collection.checkout(1)

    def test_checking_out_invalid_versions_not_allowed(self):
        self.user_collection.init()
        with self.assertRaises(InvalidCollectionVersion):
            self.user_collection.checkout(1)

    def test_checkout_on_invalid_state_raises_error(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        sleep(SLEEP_TIME)

        self.user_collection.register('v1')
        self.user_collection._deltas_collection.drop()

        with self.assertRaises(InvalidCollectionState):
            self.user_collection.checkout(0)

    def _checkout_setup(self):
        # v0
        self.user_collection.init('v0')

        # v1
        self.user_collection.insert_one(self.DOCUMENT)
        sleep(SLEEP_TIME)
        self.user_collection.register('v1')

        # v2
        self.user_collection.insert_one(self.DOCUMENT2)
        self.user_collection.find_one_and_update(
            {'name': 'Goethe'}, {'$set': {'book': 'Faust'}}
        )
        sleep(SLEEP_TIME)

        self.user_collection.register('v2')

    def test_checkout_when_documents_are_deleted(self):
        self._checkout_setup()
        self.user_collection.delete_one({'name': 'Goethe'})
        self.user_collection.register('v3')
        self.assertEqual(1, self.user_collection.count_documents({}))
        self.user_collection.checkout(2)
        self.assertEqual(2, self.user_collection.count_documents({}))
        self.user_collection.checkout(3)
        self.assertEqual(1, self.user_collection.count_documents({}))

    def test_checkout_in_both_directions(self):
        self._checkout_setup()

        self.assertEqual(self.user_collection.count_documents({}), 2)
        self.user_collection.checkout(1)
        self.assertEqual(self.user_collection.count_documents({}), 1)

        doc = self.user_collection.find_one({})
        self.assertTrue(self.DOCUMENT == doc)

        self.user_collection.checkout(0)
        self.assertEqual(self.user_collection.count_documents({}), 0)

        self.user_collection.checkout(2)
        self.assertEqual(self.user_collection.count_documents({}), 2)

        doc1 = self.user_collection.find_one({'name': 'Goethe'})
        doc2 = self.user_collection.find_one({'name': 'Euler'})
        self.DOCUMENT['book'] = 'Faust'
        self.assertEqual(self.DOCUMENT, doc1)
        self.assertEqual(self.DOCUMENT2, doc2)

    def test_transitivity_of_checkouts_backwards(self):
        self.user_collection.insert_one({'name': 'Gauss', 'emails': []})
        self._checkout_setup()

        # v2 -> v0
        self.user_collection.checkout(0)
        collection_state1 = list(self.user_collection.find({}))

        # v2 -> v1 -> v0
        self.user_collection.checkout(2)
        self.user_collection.checkout(1)
        self.user_collection.checkout(0)
        collection_state2 = list(self.user_collection.find({}))

        self.assertEqual(collection_state1, collection_state2)

    def test_transitivity_of_checkouts_forward(self):
        self._checkout_setup()

        # v0 -> v2
        self.user_collection.checkout(0)
        self.user_collection.checkout(2)
        collection_state1 = list(self.user_collection.find({}))

        # v0 -> v1 -> v2
        self.user_collection.checkout(0)
        self.user_collection.checkout(1)
        self.user_collection.checkout(2)
        collection_state2 = list(self.user_collection.find({}))
        self.assertEqual(len(collection_state1), len(collection_state2))

        # fix the order
        if collection_state1[0]['name'] != collection_state2[0]['name']:
            collection_state2 = collection_state2[::-1]

        self.assertEqual(collection_state1, collection_state2)
