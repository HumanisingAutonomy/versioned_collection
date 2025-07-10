import os
from unittest import mock

import pymongo

from tests.test_versioned_collection.common import _BaseTest
from versioned_collection import VersionedCollection


class TestVersionedCollectionBasics(_BaseTest):

    def test_find_one_and_replace(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init()
        self.assertFalse(self.user_collection.has_changes())
        self.user_collection.find_one_and_replace(
            {'name': 'Goethe'}, self.DOCUMENT2
        )
        self.assertTrue(self.user_collection.has_changes())

    def test_find_one_and_update(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init()
        self.assertFalse(self.user_collection.has_changes())
        self.user_collection.find_one_and_update(
            {}, {"$set": {'name': 'GOETHE'}}
        )
        self.assertTrue(self.user_collection.has_changes())

    def test_find_one_and_delete(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init()
        self.user_collection.find_one_and_delete({})
        self.assertTrue(self.user_collection.has_changes())

    def test_delete_many(self):
        self.user_collection.insert_many([self.DOCUMENT, self.DOCUMENT2])
        ids = [self.DOCUMENT['_id'], self.DOCUMENT2['_id']]
        self.user_collection.init()
        self.user_collection.delete_many({'_id': {"$in": ids}})
        self.assertTrue(self.user_collection.has_changes())

    def test_delete_one(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init()
        self.user_collection.delete_one({})
        self.assertTrue(self.user_collection.has_changes())

    def test_update_one(self):
        self.user_collection.init()
        self.user_collection.update_one(
            {'name': 'Goethe'}, {"$set": self.DOCUMENT}
        )
        self.assertFalse(self.user_collection.has_changes())
        self.user_collection.update_one(
            {'name': 'Goethe'}, {"$set": self.DOCUMENT}, upsert=True
        )
        self.assertTrue(self.user_collection.has_changes())

        self.user_collection.register('v1')
        self.assertFalse(self.user_collection.has_changes())
        self.user_collection.update_one({}, {"$set": {'name': 'GOETHE'}})
        self.assertTrue(self.user_collection.has_changes())

    def test_update_many(self):
        self.user_collection.insert_many([self.DOCUMENT, self.DOCUMENT2])
        self.user_collection.init()
        self.user_collection.update_many({}, {"$set": {'new_filed': True}})
        self.assertTrue(self.user_collection.has_changes())

    def test_replace_one(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init()
        self.user_collection.replace_one({}, self.DOCUMENT2)
        self.assertTrue(self.user_collection.has_changes())

    def test_insert_one(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        self.assertTrue(self.user_collection.has_changes())

    def test_insert_many(self):
        self.user_collection.init()
        self.user_collection.insert_many([self.DOCUMENT, self.DOCUMENT2])
        self.assertTrue(self.user_collection.has_changes())

    def test_bulk_write(self):
        self.user_collection.init()
        self.user_collection.bulk_write([
            pymongo.InsertOne(self.DOCUMENT),
            pymongo.DeleteOne(self.DOCUMENT),
            pymongo.InsertOne(self.DOCUMENT2),
        ])
        self.assertTrue(self.user_collection.has_changes())

    def test_aggregate_with_no_changes(self):
        self.user_collection.init()
        _ = self.user_collection.aggregate([{'$match': {}}])
        self.assertFalse(self.user_collection.has_changes())

    def test_aggregate_with_no_changes_empty_pipeline(self):
        self.user_collection.init()
        _ = self.user_collection.aggregate([])
        self.assertFalse(self.user_collection.has_changes())

    def test_aggregation_with_modifying_pipeline(self):
        # The behaviour here should be just to mark the collection as being
        # potentially modified. The other parts of `VersionedCollection` will
        # check if there are actually any changes during the basic operations
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init()

        self.user_collection.aggregate(
            [{'$match': {}}, {'$out': self.user_collection.name}]
        )
        self.assertTrue(self.user_collection.has_changes())

    def test_aggregation_with_modifying_pipeline2(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init()

        self.user_collection.aggregate([
            {'$match': {}},
            {
                '$out': {
                    'db': self._database_name,
                    'coll': self.user_collection.name,
                }
            },
        ])
        self.assertTrue(self.user_collection.has_changes())

    def test_aggregate_raw_batches(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init()
        out = self.user_collection.aggregate_raw_batches([{'$match': {}}])
        self.assertFalse(self.user_collection.has_changes())
        self.assertEqual(1, len(list(out)))  # this is 1 batch


class AuthTest(_BaseTest):

    @mock.patch.dict(
        os.environ,
        {
            "VC_MONGO_USER": "mongo",
            "VC_MONGO_PASSWORD": "pswd",
        },
    )
    def test_uses_env_variables_if_defined(self):
        collection = VersionedCollection(self.database, "User")

        # FIXME: forgive me lord for i have sinned
        user, password = collection.__dict__[
            '_VersionedCollection__credentials'
        ]
        self.assertEqual(user, "mongo")
        self.assertEqual(password, "pswd")
