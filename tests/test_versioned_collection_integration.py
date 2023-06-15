from copy import deepcopy
from time import sleep
from typing import List
from unittest import TestCase

import pymongo
from bson import ObjectId
from pymongo import MongoClient

from versioned_collection import VersionedCollection
from versioned_collection.errors import CollectionAlreadyInitialised, \
    InvalidOperation, InvalidCollectionVersion, BranchNotFound, \
    InvalidCollectionState, AutoMergeFailedError
from versioned_collection.utils.serialization import stringify_object_id


class User(VersionedCollection):
    SCHEMA = {
        'name': str,
        'emails': List[str]
    }


def debug():
    import pdb
    pdb.set_trace()


SLEEP_TIME = 0.12


class _BaseTest(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super(_BaseTest, cls).setUpClass()
        conn_str = "mongodb://localhost:27017"
        connection = MongoClient(conn_str)
        _database_name = "__test__db"
        cls.database = connection[_database_name]
        cls._database_name = _database_name

    def setUp(self) -> None:
        self.user_collection = User(self.database)

        self.DOCUMENT = {
            'name': 'Goethe',
            'emails': ['oh_my@goethe.com']
        }

        self.DOCUMENT2 = {
            'name': 'Euler',
            'emails': ['euler@mathsclub.ch']
        }

    def tearDown(self) -> None:
        self.user_collection.drop()


class TestVersionedCollectionBasics(_BaseTest):

    def test_find_one_and_replace(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init()
        self.assertFalse(self.user_collection.has_changes())
        self.user_collection.find_one_and_replace({'name': 'Goethe'},
                                                  self.DOCUMENT2)
        self.assertTrue(self.user_collection.has_changes())

    def test_find_one_and_update(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init()
        self.assertFalse(self.user_collection.has_changes())
        self.user_collection.find_one_and_update(
            {}, {"$set": {'name': 'GOETHE'}})
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
            pymongo.InsertOne(self.DOCUMENT2)
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

        self.user_collection.aggregate([
            {'$match': {}}, {'$out': self.user_collection.name}
        ])
        self.assertTrue(self.user_collection.has_changes())

    def test_aggregation_with_modifying_pipeline2(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init()

        self.user_collection.aggregate([
            {'$match': {}},
            {'$out': {
                'db': self._database_name,
                'coll': self.user_collection.name
            }}
        ])
        self.assertTrue(self.user_collection.has_changes())

    def test_aggregate_raw_batches(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init()
        out = self.user_collection.aggregate_raw_batches([{'$match': {}}])
        self.assertFalse(self.user_collection.has_changes())
        self.assertEqual(1, len(list(out)))  # this is 1 batch


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
            self.user_collection._branches_collection.count_documents({}), 1)
        data = self.user_collection._branches_collection.get_branch('main')
        self.assertIsNotNone(data)


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
        self.assertEqual(len(self.database.list_collection_names()),
                         len(self.user_collection._tracking_collections) + 2)
        self.user_collection.drop()
        self.assertEqual(len(self.database.list_collection_names()), 0)

    def test_dropping_a_collection_stops_the_listeners(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init()
        self.assertTrue(self.user_collection._listener.is_listening())
        self.user_collection.drop()
        self.assertFalse(self.user_collection._listener.is_listening())


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
        old_coll_names = [coll.name for coll in
                          self.user_collection._tracking_collections]
        old_coll_names.append(self.user_collection.name)

        new_name = 'USERS'
        self.user_collection = self.user_collection.rename(new_name)
        new_coll_names = [coll.name for coll in
                          self.user_collection._tracking_collections]
        new_coll_names.append(new_name)

        current_collections = self.database.list_collection_names()
        self.assertEqual(len(current_collections) - 1, len(new_coll_names))
        for coll in new_coll_names:
            self.assertIn(coll, current_collections)


class TestVersionedCollectionRegister(_BaseTest):

    def test_register_twice(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        res = self.user_collection.register("register once")
        self.assertTrue(res)
        res = self.user_collection.register("register twice")
        self.assertFalse(res)

    def test_registering_in_detached_head_mode_creates_a_new_branch(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        sleep(SLEEP_TIME)
        self.assertTrue(self.user_collection.register("v1"))
        self.user_collection.checkout(0)
        self.user_collection.insert_one(self.DOCUMENT2)

        self.user_collection.register('message', branch_name='branch')

    def test_registering_in_detached_head_mode_requires_branch_name(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        self.assertTrue(self.user_collection.register("v1"))

        self.user_collection.checkout(0)
        self.user_collection.insert_one(self.DOCUMENT2)

        with self.assertRaises(ValueError):
            self.user_collection.register('message')

        self.user_collection.register('message', 'new_branch')

    def test_registering_updates_the_replica(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        sleep(SLEEP_TIME)  # something funny happens when the whole suite is run
        self.user_collection.register("v1")
        coll_docs = list(self.user_collection.find({}))
        replica_docs = list(self.user_collection._replica_collection.find({}))
        self.assertEqual(1, len(coll_docs))
        self.assertEqual(1, len(replica_docs))
        self.assertTrue(coll_docs[0] == replica_docs[0])

    def test_registering_updates_the_logs(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        message = "V1 of Users collection."
        res = self.user_collection.register(message)
        self.assertTrue(res)
        logs = self.user_collection.get_log()
        self.assertEqual(len(logs), 2)  # init and first registered version
        self.assertEqual(logs[0].message, message)

    def test_multiple_updates_to_doc_before_register(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.update_one({'name': self.DOCUMENT['name']},
                                        {"$set": {'age': 99}})
        sleep(SLEEP_TIME)
        self.user_collection.register('v1')

    def test_multiple_updates_to_doc_before_register_with_more_versions(self):
        self.user_collection.init('v0')
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.register('v1')
        self.user_collection.update_one({'name': self.DOCUMENT['name']},
                                        {"$set": {'age': 99}})
        self.user_collection.update_one({'name': self.DOCUMENT['name']},
                                        {"$set": {'is_wizard': False}})
        sleep(SLEEP_TIME)
        self.user_collection.register('v2')

    def test_multiple_updates_to_doc_before_register_with_more_versions2(self):
        self.user_collection.init('v0')
        self.user_collection.insert_one(self.DOCUMENT2)
        self.assertTrue(self.user_collection.register('v1'))
        self.user_collection.insert_one(self.DOCUMENT)
        self.assertTrue(self.user_collection.register('v2'))
        self.user_collection.update_one({'name': self.DOCUMENT['name']},
                                        {"$set": {'age': 199}})
        self.user_collection.update_one({'name': self.DOCUMENT['name']},
                                        {"$set": {'is_wizard': False}})
        sleep(SLEEP_TIME)
        self.assertTrue(self.user_collection.register('v3'))
        self.assertEqual(
            3, self.user_collection._deltas_collection.count_documents({}))

    def test_register_with_multiple_updates_from_empty_branch(self):
        self.user_collection.init('v0')
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.register('v1')
        self.user_collection.create_branch('b')
        self.user_collection.update_one({'_id': self.DOCUMENT['_id']},
                                        {"$set": {'name': 'GOETHE'}})
        self.user_collection.update_one({'_id': self.DOCUMENT['_id']},
                                        {"$set": {'is_wizard': False}})
        sleep(SLEEP_TIME)
        self.user_collection.register('b_v0')
        self.assertEqual(
            2, self.user_collection._deltas_collection.count_documents({}))

    def test_register_after_insert_and_delete(self):
        self.user_collection.init('v0')
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.delete_one({'_id': self.DOCUMENT['_id']})
        self.assertTrue(self.user_collection.has_changes())
        self.assertFalse(self.user_collection.register('v1'))
        self.assertEqual(0, self.user_collection.version)
        self.assertEqual(1, len(self.user_collection.get_log()))

    def test_nothing_registered_if_collection_not_actually_modified(self):
        self.user_collection.init()
        # Artificially set the state to 'changed'. This can naturally happen
        # under some circumstances with aggregation pipelines
        self.user_collection._has_changed()  # noqa
        self.assertFalse(self.user_collection.register('v1?'))
        self.assertFalse(self.user_collection.has_changes())


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
            filter={'name': 'Goethe'}, update={"$set": {'name': 'GOETHE'}})
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
            upsert=True
        )
        self.assertTrue(self.user_collection.has_changes())
        self.assertTrue(self.user_collection.discard_changes())
        self.assertFalse(self.user_collection.has_changes())
        self.assertEqual(0, self.user_collection.count_documents({}))

    def test_discard_changes_handles_upserts2(self):
        self.user_collection.init()
        self.user_collection.find_one_and_replace(
            filter={'name': 'Goethe'},
            replacement=self.DOCUMENT,
            upsert=True
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
            upsert=True
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
        self.user_collection.update_one({'_id': self.DOCUMENT['_id']},
                                        {"$set": {'age': 69}})
        self.user_collection.delete_one({'_id': self.DOCUMENT['_id']})
        self.assertEqual(0, self.user_collection.count_documents({}))
        self.user_collection.discard_changes()
        self.assertEqual(0, self.user_collection.count_documents({}))

    def test_order_of_operations_2(self):
        self.user_collection.insert_one(self.DOCUMENT)
        original_doc = deepcopy(self.DOCUMENT)
        self.user_collection.init('v0')
        self.user_collection.update_one({'_id': self.DOCUMENT['_id']},
                                        {"$set": {'age': 69}})
        self.user_collection.delete_one({'_id': self.DOCUMENT['_id']})
        self.user_collection.discard_changes()
        self.assertEqual(1, self.user_collection.count_documents({}))
        self.assertEqual(original_doc, self.user_collection.find_one({}))


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
                "Cannot apply stashed data because the collection has changes"
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


class TestVersionedCollectionDiff(_BaseTest):

    def test_diffs_for_untracked_collection(self):
        self.assertIsNone(self.user_collection.diff())
        self.assertIsNone(self.user_collection.diff(1))
        self.assertIsNone(self.user_collection.diff(1, 'main'))
        self.assertIsNone(self.user_collection.diff(1, 'other'))
        self.assertIsNone(self.user_collection.diff(branch='other'))

    def test_diffs_for_unchanged_collection(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init()
        self.assertEqual({}, self.user_collection.diff())
        self.assertEqual({}, self.user_collection.diff(deep=False))

    def test_invalid_versions_throw_error(self):
        self.user_collection.init()
        with self.assertRaises(InvalidCollectionVersion):
            self.user_collection.diff(version=42)
        with self.assertRaises(InvalidCollectionVersion):
            self.user_collection.diff(version=0, branch='honolulu')

    def test_diffs_when_unregistered_changes_were_made(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.insert_one(self.DOCUMENT2)
        self.user_collection.init()
        self.user_collection.update_one({'name': 'Goethe'},
                                        {"$set": {'name': 'GOETHE'}})
        self.user_collection.delete_one({'name': 'Euler'})
        sleep(SLEEP_TIME)
        diffs = self.user_collection.diff()
        self.assertEqual(2, len(diffs))
        diffs = self.user_collection.diff(deep=False)
        self.assertEqual(2, len(diffs))
        ids = set(diffs.keys())
        doc_ids = {
            stringify_object_id(self.DOCUMENT['_id']),
            stringify_object_id(self.DOCUMENT2['_id'])
        }
        self.assertEqual(ids, doc_ids)

    def test_deep_diff(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init()
        self.user_collection.update_one(
            {'name': 'Goethe'},
            {"$set": {'name': 'GOETHE'}}
        )
        sleep(SLEEP_TIME)
        diff = self.user_collection.diff(deep=True)[self.DOCUMENT['_id']]
        print(diff)
        self.assertEqual(
            diff['values_changed']["root['name']"]['new_value'], "GOETHE"
        )

    def test_diffs_between_two_registered_versions(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.insert_one(self.DOCUMENT2)
        self.user_collection.register('v1')
        diffs = self.user_collection.diff(0)
        self.assertEqual(2, len(diffs))
        diffs = self.user_collection.diff(0, deep=False)
        self.assertEqual(2, len(diffs))

    def test_diffs_with_unregister_changes2(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        diffs = self.user_collection.diff(branch='main')
        self.assertEqual(1, len(diffs))
        diffs = self.user_collection.diff(branch='main', deep=False)
        self.assertEqual(1, len(diffs))

    def test_diffs_with_no_changes(self):
        self.user_collection.init()
        diff = self.user_collection.diff(branch='main')
        self.assertEqual(0, len(diff))
        diff = self.user_collection.diff(branch='main', deep=False)
        self.assertEqual(0, len(diff))

    def test_diffs_between_versions_with_untracked_changes(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        self.assertTrue(self.user_collection.register('v1'))
        self.user_collection.insert_one(self.DOCUMENT2)
        diffs = self.user_collection.diff(0, 'main')
        self.assertEqual(2, len(diffs))
        diffs = self.user_collection.diff(0, 'main', deep=False)
        self.assertEqual(2, len(diffs))

    def test_diff_ignores_no_ops(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.delete_one({'_id': self.DOCUMENT['_id']})
        diff = self.user_collection.diff(0, 'main')
        self.assertEqual(0, len(diff))
        diff = self.user_collection.diff(0, 'main', deep=False)
        self.assertEqual(0, len(diff))

    def tests_diffs_forward_and_backward(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.register('v1')
        self.user_collection.insert_one(self.DOCUMENT2)
        self.user_collection.register('v2')

        diff_backward_2 = self.user_collection.diff(0)
        self.assertEqual(2, len(diff_backward_2))

        self.user_collection.checkout(1)
        diff_backward_1 = self.user_collection.diff(0)
        self.assertEqual(1, len(diff_backward_1))

        self.user_collection.checkout(0)
        diff_forward_1 = self.user_collection.diff(1)
        self.assertEqual(1, len(diff_forward_1))
        diff_forward_2 = self.user_collection.diff(2)
        self.assertEqual(2, len(diff_forward_2))

        pairs = [
            (diff_forward_1, diff_backward_1), (diff_forward_2, diff_backward_2)
        ]
        for diff_forward, diff_backward in pairs:
            for obj_id in diff_forward.keys():
                diff_f = diff_forward[obj_id]
                diff_b = diff_backward[obj_id]

                self.assertIn('dictionary_item_removed', diff_f)
                self.assertIn('dictionary_item_added', diff_b)
                self.assertEqual(
                    diff_f['dictionary_item_removed'],  # noqa
                    diff_b['dictionary_item_added']  # noqa
                )


class TestVersionedCollectionLog(_BaseTest):

    def test_correct_logs_for_empty_branches(self):
        self.user_collection.init('v0')
        self.user_collection.get_log()
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.register('v1')
        log1 = self.user_collection.get_log('main')
        self.user_collection.create_branch('b')
        log2 = self.user_collection.get_log()
        self.assertEqual(log1, log2)


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

    def _checkout_setup(self):
        # v0
        self.user_collection.init('v0')

        # v1
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.register('v1')

        # v2
        self.user_collection.insert_one(self.DOCUMENT2)
        self.user_collection.find_one_and_update(
            {'name': 'Goethe'}, {'$set': {'book': 'Faust'}})
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


class TestVersionedCollectionBranching(_BaseTest):

    def test_illegal_branch_name(self):
        self.user_collection.init()
        with self.assertRaisesRegex(
                ValueError,
                "Branch names cannot start with '__'"
        ):
            self.user_collection.create_branch('__super_secret_branch')

    def test_create_branch_when_detached(self):
        self.user_collection.init()

        self.user_collection.insert_one(self.DOCUMENT)
        sleep(SLEEP_TIME)
        self.assertTrue(self.user_collection.register("v1"))

        self.user_collection.checkout(0)
        self.assertEqual(0, self.user_collection.version)
        self.assertEqual('main', self.user_collection.branch)

        old_version = self.user_collection.create_branch('branch')
        self.assertEqual((0, 'main'), old_version)
        self.assertEqual(-1, self.user_collection.version)
        self.assertEqual('branch', self.user_collection.branch)

        new_br = self.user_collection._branches_collection.get_branch('branch')
        self.assertIsNotNone(new_br)
        self.assertEqual('main', new_br.points_to_branch)
        self.assertEqual(0, new_br.points_to_collection_version)

        self.user_collection.insert_one(self.DOCUMENT2)
        self.user_collection.register('v2')

        n_branches = (self.user_collection
                      ._branches_collection
                      .count_documents({}))
        self.assertEqual(n_branches, 2)

        new_br = self.user_collection._branches_collection.get_branch('branch')
        self.assertIsNotNone(new_br)
        self.assertEqual(new_br.points_to_branch, 'branch')
        self.assertEqual(new_br.points_to_collection_version, 0)

        self.assertEqual(self.user_collection.count_documents({}), 1)
        self.assertEqual(self.user_collection.find_one({}), self.DOCUMENT2)

    def test_create_branch_when_attached(self):
        self.user_collection.init()

        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.register("v1")

        self.user_collection.create_branch('branch')
        self.user_collection.insert_one(self.DOCUMENT2)
        self.user_collection.register('v2')

        n_branches = (self.user_collection
                      ._branches_collection
                      .count_documents({}))
        self.assertEqual(n_branches, 2)

        branch = self.user_collection._branches_collection.get_branch('branch')
        self.assertIsNotNone(branch)
        self.assertEqual(branch.points_to_branch, 'branch')
        self.assertEqual(branch.points_to_collection_version, 0)

    def test_checking_out_detaches_the_head(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT2)
        self.user_collection.register('1')
        self.user_collection.checkout(0)
        metadata = self.user_collection._meta_collection.metadata
        self.assertTrue(metadata.detached)
        self.assertFalse(metadata.changed)

    def test_branching_when_registering_when_head_attached(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.register('added doc', 'other')

        with self.assertRaises(BranchNotFound):
            self.user_collection._branches_collection.get_branch('other')

        branch = self.user_collection._branches_collection.get_branch('main')
        self.assertEqual(branch.points_to_collection_version, 1)

    def test_checking_out_attaches_the_head(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)

        metadata = self.user_collection._meta_collection.metadata
        self.assertFalse(metadata.detached)
        self.assertTrue(metadata.changed)

        self.user_collection.register('v1')
        metadata = self.user_collection._meta_collection.metadata
        self.assertFalse(metadata.detached)
        self.assertFalse(metadata.changed)

        self.user_collection.checkout(0)
        metadata = self.user_collection._meta_collection.metadata
        self.assertTrue(metadata.detached)
        self.assertFalse(metadata.changed)

        self.user_collection.checkout(1)
        metadata = self.user_collection._meta_collection.metadata
        self.assertFalse(metadata.detached)
        self.assertFalse(metadata.changed)

    def test_correctly_checkout_from_empty_branches(self):
        self.user_collection.init("Initial version")

        self.user_collection.insert_one(self.DOCUMENT)
        self.assertTrue(self.user_collection.register('v1'))

        self.user_collection.create_branch('branch')
        self.assertEqual(-1, self.user_collection.version)
        self.assertEqual('branch', self.user_collection.branch)
        self.assertEqual(self.DOCUMENT, self.user_collection.find_one({}))

        with self.assertRaises(InvalidCollectionVersion):
            self.assertTrue(self.user_collection.checkout(0))

        self.assertTrue(self.user_collection.checkout(branch='branch'))
        self.assertEqual(-1, self.user_collection.version)
        self.assertEqual('branch', self.user_collection.branch)
        self.assertTrue(self.user_collection.checkout(1, 'main'))
        self.assertEqual(self.DOCUMENT, self.user_collection.find_one({}))

    def _branching_setup(self):
        """
        Branching structure
                  0_m
                  /
                1_m
              /     \
            2_m     0_b2
            / \\       \
          3_m 0_b1     1_b2
                        \
                        2_b2
                          \
                          3_b2

        self.DOCUMENT is updated in all versions except in '0_m;
        self.DOCUMENT2 is updated only in 1_m, 2_m and 0_b2
        """

        def _increase_v_and_update(doc):
            doc['v'] += 1
            self.user_collection.find_one_and_replace(
                {'_id': doc['_id']}, doc
            )

        self.user_collection.init("0_m")
        self.DOCUMENT['v'] = 1
        self.DOCUMENT2['v'] = 1

        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.insert_one(self.DOCUMENT2)
        self.user_collection.register("1_m")

        _increase_v_and_update(self.DOCUMENT)
        _increase_v_and_update(self.DOCUMENT2)
        self.user_collection.register("2_m")

        _increase_v_and_update(self.DOCUMENT)
        self.user_collection.register("3_m")

        self.user_collection.checkout(2)
        _increase_v_and_update(self.DOCUMENT)
        self.user_collection.register('0_b1', 'b1')

        self.user_collection.checkout(1, 'main')
        _increase_v_and_update(self.DOCUMENT)
        _increase_v_and_update(self.DOCUMENT2)
        self.user_collection.register('0_b2', 'b2')

        for i in range(1, 4):
            _increase_v_and_update(self.DOCUMENT)
            self.user_collection.register(f"{i}_b2")

    def test_checkout_across_branches(self):
        def _assert_version(doc, version):
            d = self.user_collection.find_one({"_id": doc['_id']})
            self.assertEqual(d['v'], version)

        self._branching_setup()
        # currently, checked out at 3_b2
        _assert_version(self.DOCUMENT, 8)
        _assert_version(self.DOCUMENT2, 3)

        self.user_collection.checkout(3, 'main')
        _assert_version(self.DOCUMENT, 3)
        _assert_version(self.DOCUMENT2, 2)

        self.user_collection.checkout(0, 'b1')
        _assert_version(self.DOCUMENT, 4)
        _assert_version(self.DOCUMENT2, 2)

        self.user_collection.checkout(2, 'b2')
        _assert_version(self.DOCUMENT, 7)
        _assert_version(self.DOCUMENT2, 3)

        self.user_collection.checkout(2, 'main')
        _assert_version(self.DOCUMENT, 2)
        _assert_version(self.DOCUMENT2, 2)

        self.user_collection.checkout(1, 'b2')
        _assert_version(self.DOCUMENT, 6)
        _assert_version(self.DOCUMENT2, 3)

        self.user_collection.checkout(0, 'b2')
        _assert_version(self.DOCUMENT, 5)
        _assert_version(self.DOCUMENT2, 3)

        self.user_collection.checkout(3, 'main')
        _assert_version(self.DOCUMENT, 3)
        _assert_version(self.DOCUMENT2, 2)

    def test_create_branch_and_checkout_back(self):
        self.user_collection.init()
        self.user_collection.create_branch('other')
        self.assertEqual('other', self.user_collection.branch)
        # -1 since no version was registered on ``other`` yet.
        self.assertEqual(-1, self.user_collection.version)

        self.user_collection.checkout(0, 'main')
        self.assertEqual('main', self.user_collection.branch)
        self.assertEqual(0, self.user_collection.version)

    def test_create_multiple_branches(self):
        self.user_collection.init()
        self.assertEqual('main', self.user_collection.branch)
        self.assertEqual(0, self.user_collection.version)

        self.user_collection.create_branch('b1')
        self.assertEqual('b1', self.user_collection.branch)
        self.assertEqual(-1, self.user_collection.version)

        self.user_collection.create_branch('b2')
        self.assertEqual('b2', self.user_collection.branch)
        self.assertEqual(-1, self.user_collection.version)

        self.user_collection.checkout(branch='b1')
        self.assertEqual('b1', self.user_collection.branch)
        self.assertEqual(-1, self.user_collection.version)

        self.user_collection.checkout(branch='b2')
        self.assertEqual('b2', self.user_collection.branch)
        self.assertEqual(-1, self.user_collection.version)

        self.user_collection.checkout(0, 'main')
        self.assertEqual('main', self.user_collection.branch)
        self.assertEqual(0, self.user_collection.version)

    def test_checking_out_between_empty_branches(self):
        def _assert_version(doc, version):
            d = self.user_collection.find_one({"_id": doc['_id']})
            self.assertEqual(d['v'], version)

        self._branching_setup()
        _assert_version(self.DOCUMENT, 8)
        # currently, checked out at 3_b2

        # create two empty branches pointing at 3_b2
        self.user_collection.create_branch('e1')
        self.user_collection.create_branch('e2')
        _assert_version(self.DOCUMENT, 8)

        # go to 2_m and create another branch
        self.user_collection.checkout(2, 'main')
        self.user_collection.create_branch('e3')
        _assert_version(self.DOCUMENT, 2)

        self.user_collection.checkout(branch='e2')
        _assert_version(self.DOCUMENT, 8)
        self.user_collection.checkout(branch='e1')
        _assert_version(self.DOCUMENT, 8)

        self.user_collection.checkout(branch='e3')
        _assert_version(self.DOCUMENT, 2)

        self.user_collection.checkout(2, branch='b2')
        _assert_version(self.DOCUMENT, 7)

    def test_checkout_on_empty_main_branch(self):
        self.user_collection.init('v0')
        self.user_collection.create_branch('b')
        self.assertTrue(not self.user_collection.is_detached())
        self.assertEqual('b', self.user_collection.branch)
        self.user_collection.checkout(branch='main')
        self.assertTrue(not self.user_collection.is_detached())
        self.assertEqual('main', self.user_collection.branch)
        self.user_collection.checkout(branch='b')
        self.assertTrue(not self.user_collection.is_detached())
        self.assertEqual('b', self.user_collection.branch)

    def test_branching_from_v0_with_init_state(self):
        def _increase_v_and_update(doc):
            doc['v'] += 1
            self.user_collection.find_one_and_replace(
                {'_id': doc['_id']}, doc
            )

        self.DOCUMENT['v'] = 1
        self.DOCUMENT['_id'] = ObjectId()
        original_doc = deepcopy(self.DOCUMENT)

        # 0_m
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init('v0')

        # 1_m
        _increase_v_and_update(self.DOCUMENT)
        self.assertTrue(self.user_collection.register('v1'))

        # 2_m
        _increase_v_and_update(self.DOCUMENT)
        self.assertTrue(self.user_collection.register('2_m'))

        # 0_b
        self.user_collection.checkout(0, 'main')
        _increase_v_and_update(self.DOCUMENT)
        self.assertTrue(self.user_collection.register('0_b', branch_name='b'))

        self.assertEqual(0, self.user_collection.version)
        self.assertEqual('b', self.user_collection.branch)

        # 1_b
        self.DOCUMENT['new_field'] = {'data': True}
        _increase_v_and_update(self.DOCUMENT)
        self.assertTrue(self.user_collection.register('1_b'))

        self.assertEqual(5, self.user_collection.find_one({})['v'])

        self.user_collection.checkout(branch='main')
        original_doc['v'] = 3
        self.assertEqual(original_doc, self.user_collection.find_one({}))

    def test_same_document_added_on_two_branches(self):
        def _increase_v_and_update(doc):
            doc['v'] += 1
            self.user_collection.find_one_and_replace(
                {'_id': doc['_id']}, doc
            )

        self.DOCUMENT['v'] = 1
        self.DOCUMENT2['v'] = 1

        _id = ObjectId()
        self.DOCUMENT2['_id'] = _id

        # 0_m
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init('v0')

        # 1_m
        _increase_v_and_update(self.DOCUMENT)
        self.assertTrue(self.user_collection.register('v1'))

        # 2_m
        _increase_v_and_update(self.DOCUMENT)
        self.user_collection.insert_one(self.DOCUMENT2)
        self.assertTrue(self.user_collection.register('2_m'))

        # 0_b
        self.user_collection.checkout(0, 'main')
        self.assertEqual(1, self.user_collection.count_documents({}))
        _increase_v_and_update(self.DOCUMENT)
        self.assertTrue(self.user_collection.register('0_b', branch_name='b'))

        self.assertEqual(0, self.user_collection.version)
        self.assertEqual('b', self.user_collection.branch)

        # 1_b
        _increase_v_and_update(self.DOCUMENT)
        self.user_collection.insert_one(self.DOCUMENT2)
        _increase_v_and_update(self.DOCUMENT2)
        self.assertTrue(self.user_collection.register('1_b'))
        self.assertEqual(2, self.user_collection.find_one({'_id': _id})['v'])

        self.user_collection.checkout(2, 'main')
        self.assertEqual(2, self.user_collection.count_documents({}))
        self.assertEqual(1, self.user_collection.find_one({'_id': _id})['v'])


class TestVersionedCollectionDeleteSubtree(_BaseTest):

    def test_deleting_does_nothing_for_untracked_collections(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.assertFalse(self.user_collection.delete_version_subtree(0))

    def test_deleting_the_root_of_the_version_tree(self):
        self.user_collection.init('v0')
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.register('v1')
        self.assertTrue(self.user_collection.delete_version_subtree(0))
        self.assertEqual({'tracked': False}, self.user_collection.status())
        self.assertIsNone(self.user_collection.find_one())

    def test_deleting_the_last_version(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init('v0')
        self.user_collection.insert_one(self.DOCUMENT2)
        sleep(SLEEP_TIME)
        self.assertTrue(self.user_collection.register('v1'))
        self.assertEqual(2, len(self.user_collection.get_log()))
        self.assertEqual(2, self.user_collection.count_documents({}))
        self.assertEqual(1, self.user_collection.version)

        self.user_collection.delete_version_subtree(1)
        self.assertEqual(1, len(self.user_collection.get_log()))
        self.assertEqual(1, self.user_collection.count_documents({}))
        self.assertEqual(0, self.user_collection.version)
        self.assertFalse(self.user_collection.is_detached())

    def _build_structure(self):
        self.user_collection.init('v0')
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.register('v1')
        self.user_collection.create_branch('branch')
        self.user_collection.insert_one(self.DOCUMENT2)
        self.user_collection.register('v0_branch')
        self.user_collection.update_one(
            {'_id': self.DOCUMENT['_id']}, {"$set": {'height': None}}
        )
        self.user_collection.register('v1_branch')

    def test_delete_a_whole_branch(self):
        self._build_structure()
        self.user_collection.delete_version_subtree(0)
        self.assertEqual(1, self.user_collection.version)
        self.assertEqual('main', self.user_collection.branch)
        self.assertEqual(1, self.user_collection.count_documents({}))

    def test_deleting_a_version_from_the_parent_branch(self):
        self._build_structure()
        self.user_collection.delete_version_subtree(1, 'main')
        self.assertEqual(0, self.user_collection.version)
        self.assertEqual('main', self.user_collection.branch)
        self.assertEqual(0, self.user_collection.count_documents({}))

    def test_delete_a_multi_branch_subtree(self):
        self._build_structure()
        self.user_collection.checkout(0)
        self.user_collection.create_branch('other_branch')
        self.user_collection.delete_one(self.DOCUMENT)
        self.user_collection.register('v0_other_branch')

        self.user_collection.checkout(1, 'main')
        self.user_collection.delete_version_subtree(1, 'main')
        self.assertEqual(0, self.user_collection.version)
        self.assertEqual('main', self.user_collection.branch)
        self.assertEqual(0, self.user_collection.count_documents({}))

    def test_deleting_next_versions_on_branch_reattaches_the_head(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        self.assertTrue(self.user_collection.register('v1'))
        self.user_collection.checkout(0)
        self.assertTrue(self.user_collection.is_detached())
        self.assertTrue(self.user_collection.delete_version_subtree(1))
        self.assertEqual(0, self.user_collection.version)
        self.assertFalse(self.user_collection.is_detached())

    def test_delete_called_from_empty_branch(self):
        self.user_collection.init('v0_m')
        self.user_collection.insert_one(self.DOCUMENT)
        self.assertTrue(self.user_collection.register('v1_m'))
        self.assertTrue(self.user_collection.checkout(0))
        self.user_collection.insert_one(self.DOCUMENT2)
        self.user_collection.register('v0_b', branch_name='b')
        self.user_collection.checkout(branch='main')
        self.user_collection.create_branch('empty')
        self.assertTrue(self.user_collection.delete_version_subtree(0, 'b'))


class _RemoteBaseTest(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        # TODO: move to `mongomock`
        super(_RemoteBaseTest, cls).setUpClass()
        conn_str = "mongodb://localhost:27017"
        connection_local = MongoClient(conn_str)
        cls.db_local = connection_local["__test__db_local"]
        connection_remote = MongoClient(conn_str)
        cls.db_remote = connection_remote["__test__db_remote"]

    def setUp(self) -> None:
        self.local = User(self.db_local)
        self.remote = User(self.db_remote)

        self.DOCUMENT = {
            'name': 'Goethe',
            'emails': ['oh_my@goethe.de']
        }

        self.DOCUMENT2 = {
            'name': 'Euler',
            'emails': ['euler@mathsclub.ch']
        }

        self.DOCUMENT3 = {
            'name': 'Gauss',
            'emails': ['gauss@mathsclub.de', 'gauss@astronomyclub.de']
        }

    def tearDown(self) -> None:
        self.local.drop()
        self.remote.drop()


class TestVersionCollectionEquality(_RemoteBaseTest):

    def test_untracked_collections_are_equal_if_they_have_the_same_name(self):
        self.assertEqual(self.local, self.remote)

    def test_tracked_and_untracked_collections_are_not_equal(self):
        self.local.init()
        self.assertNotEqual(self.local, self.remote)

    def test_collections_with_diverging_roots_are_not_equal(self):
        self.local.init()
        self.remote.init()
        self.assertNotEqual(self.local, self.remote)

    def test_empty_collections_are_equal(self):
        self.local.init()
        self.local.push(self.remote)
        self.assertEqual(self.local, self.remote)
        self.assertTrue(self.local <= self.remote)
        self.assertTrue(self.local >= self.remote)

    def test_collection_not_equal_to_none(self):
        self.assertNotEqual(self.local, None)

    def test_versioned_collections_type_check_equality(self):
        other = pymongo.collection.Collection(self.db_local, 'other')
        self.assertNotEqual(self.local, other)
        other.drop()

    def test_collection_equal_to_self(self):
        self.assertEqual(self.local, self.local)

    def test_collections_not_equal_if_names_not_equal(self):
        other = VersionedCollection(self.db_local, 'User')
        self.assertNotEqual(self.local, other)
        other.drop()

    def test_collections_not_equal_if_different_number_of_branches(self):
        self.local.init()
        self.remote.init()
        self.local.create_branch('brr')
        self.assertNotEqual(self.local, self.remote)

    def test_less_with_different_collection_types(self):
        other = pymongo.collection.Collection(self.db_local, 'User')
        with self.assertRaisesRegex(
                TypeError,
                "not supported between instances of"
        ):
            _ = self.local < other
        other.drop()

    def test_collection_less_than_self(self):
        self.assertFalse(self.local < self.local)
        self.assertTrue(self.local <= self.local)

    def test_comparing_with_less_throws_error_when_names_different(self):
        other = VersionedCollection(self.db_local, 'User')
        with self.assertRaisesRegex(ValueError, "different names"):
            _ = self.local < other
        other.drop()

    def test_untracked_collections_are_less_then_each_other(self):
        self.assertFalse(self.remote.is_tracked())
        self.assertFalse(self.local.is_tracked())
        self.assertTrue(self.remote < self.local)

    def test_less_with_one_tracked_one_untracked(self):
        self.remote.init()
        self.assertFalse(self.local < self.remote)
        self.remote.drop()

        self.local.init()
        self.assertFalse(self.remote.is_tracked())
        self.assertFalse(self.local < self.remote)

    def test_less_with_different_number_of_branches(self):
        self.local.init()
        self.remote.init()
        self.local.create_branch('brr')
        self.assertFalse(self.local < self.remote)
        self.assertTrue(self.local > self.remote)

    def test_less(self):
        self.local.init()
        self.assertTrue(self.local.push(self.remote))
        self.local.insert_one(self.DOCUMENT)
        self.assertTrue(self.local.register('v1'))

        self.assertTrue(self.local != self.remote)
        self.assertTrue(self.remote < self.local)
        self.assertTrue(self.remote <= self.local)

    def test_less_with_more_branches(self):
        self.local.init()
        self.local.push(self.remote)
        self.local.create_branch('branch')

        self.assertTrue(self.local != self.remote)
        self.assertTrue(self.remote < self.local)

    def _setup_local_remote(self):
        self.local.init('0_m')
        self.local.insert_one(self.DOCUMENT)
        self.assertTrue(self.local.register('1_m'))
        self.local.checkout(0)
        self.local.create_branch('b')
        self.local.insert_one(self.DOCUMENT2)
        self.assertTrue(self.local.register('0_b'))

        self.assertTrue(self.local.push(self.remote, 'main'))
        self.assertTrue(self.local.push(self.remote, 'b'))

        self.assertEqual(self.local, self.remote)

    def test_less_tree_subset(self):
        self._setup_local_remote()

        self.local.insert_one(self.DOCUMENT3)
        self.assertTrue(self.local.register('1_b'))
        self.local.checkout(1, branch='main')
        self.assertTrue(1, self.local.version)
        self.assertTrue('main', self.local.branch)
        self.assertFalse(self.local.is_detached())
        self.assertFalse(self.local.has_changes())

        self.local.insert_one(self.DOCUMENT3)
        self.assertTrue(self.local.register('2_m'))

        self.assertTrue(self.local != self.remote)
        self.assertTrue(self.local > self.remote)
        self.assertTrue(self.local >= self.remote)

    def test_different_branch_length(self):
        self._setup_local_remote()

        self.local.checkout(1, 'main')
        self.local.insert_one(self.DOCUMENT3)
        self.assertTrue(self.local.register('2_main'))

        self.assertTrue(self.remote < self.local)

        self.remote.checkout(branch='b')
        self.remote.insert_one(self.DOCUMENT3)
        self.assertTrue(self.remote.register('1_b'))

        self.assertNotEqual(self.local, self.remote)
        self.assertFalse(self.local < self.remote)
        self.assertFalse(self.remote < self.local)

    def test_different_set_of_empty_branches(self):
        self.local.init()
        self.local.push(self.remote)
        self.assertEqual(self.local, self.remote)

        self.local.create_branch('b_local')
        self.remote.create_branch('b_remote')

        self.assertNotEqual(self.local, self.remote)
        self.assertFalse(self.local < self.remote)
        self.assertFalse(self.local <= self.remote)


class TestVersionedCollectionPull(_RemoteBaseTest):

    def test_pulling_from_self_not_allowed(self):
        self.local.init('v0')
        with self.assertRaises(InvalidOperation):
            self.local.pull(self.local)

    def test_pulling_when_local_empty_and_not_tracked_initialises_it(self):
        self.remote.init('v0')
        self.assertTrue(self.local.pull(self.remote))
        self.assertEqual(1, len(self.local.branches()))

    def test_pulling_from_untracked_collections_returns_false(self):
        self.assertFalse(self.local.pull(self.remote))

    def test_pulling_from_collection_with_different_name_raises_error(self):
        self.remote.insert_one(self.DOCUMENT)
        self.remote = self.remote.rename('OtherName')
        self.remote.init()
        with self.assertRaises(ValueError):
            self.local.pull(self.remote)
        self.remote.drop()

    def test_pull_raises_error_when_head_detached_and_branch_not_provided(self):
        self.local.init()
        self.local.insert_one(self.DOCUMENT)
        self.local.register('v1')
        self.local.checkout(0)
        self.remote.init()  # dummy
        with self.assertRaises(InvalidOperation):
            self.local.pull(self.remote, branch=None)

    def test_pulling_non_existent_branches_raises_error(self):
        self.remote.init()
        self.assertTrue(self.local.pull(self.remote))
        with self.assertRaises(InvalidOperation):
            self.local.pull(self.remote, branch='not_found')

    def test_pull_does_nothing_if_local_up_to_date(self):
        self.local.init()
        self.assertTrue(self.local.push(self.remote))
        self.assertTrue(self.local.pull(self.remote))
        self.local.insert_one(self.DOCUMENT)
        self.local.register('v1')
        self.assertTrue(self.local.pull(self.remote))

    def test_pull_from_remote(self):
        self.remote.init()
        self.remote.insert_one(self.DOCUMENT)
        self.remote.register('v1')
        self.assertTrue(self.local.pull(self.remote))

        self.assertEqual(self.remote.version, self.local.version)
        self.assertEqual(self.remote.branch, self.local.branch)
        self.assertEqual(self.remote.count_documents({}),
                         self.local.count_documents({}))
        self.assertEqual(self.remote.find_one({}), self.local.find_one({}))
        self.assertEqual(self.remote.get_log(), self.local.get_log())
        self.assertEqual(self.remote.branches(), self.local.branches())
        self.assertEqual(1, self.local.count_documents({}))
        self.assertEqual(self.local.find_one({}), self.remote.find_one({}))

    def test_pull_not_allowed_when_there_are_changes(self):
        self.local.init()
        self.local.push(self.remote)
        self.local.insert_one(self.DOCUMENT)
        with self.assertRaises(InvalidOperation):
            self.local.pull(self.remote)

    def test_pulling_not_allowed_if_diverging_roots(self):
        self.local.init()
        self.remote.init()
        with self.assertRaises(InvalidCollectionState):
            self.local.pull(self.remote)

    def test_pulling_with_divergence_and_stashed_data_and_changes(self):
        self.local.init()
        self.local.push(self.remote)
        self.remote.insert_one(self.DOCUMENT)
        self.remote.register('v1')
        self.local.insert_one(self.DOCUMENT)
        self.local.register('v1')

        self.local.insert_one(self.DOCUMENT2)
        self.local.stash()

        self.local.create_branch('other')
        self.local.insert_one(self.DOCUMENT3)
        with self.assertRaises(InvalidOperation):
            self.local.pull(self.remote, branch='main')

    def _setup_remote(self):
        self.remote.init('v0_main')

        self.remote.insert_many([self.DOCUMENT, self.DOCUMENT2])
        self.remote.register('v1_main')

    def _update_remote_1(self):
        self.remote.update_one(
            {'_id': self.DOCUMENT['_id']}, {"$set": {'new_field': 'value1'}})
        self.remote.register('v2_main')

    def _update_remote_2(self):
        self.remote.checkout(1, 'main')
        self.remote.create_branch('b')
        self.remote.insert_one(self.DOCUMENT3)
        self.remote.register('v0_b')
        self.remote.delete_one({'_id': self.DOCUMENT2['_id']})
        self.remote.register('v1_b')
        self.remote.checkout(branch='main')

    def _update_remote_3(self):
        self.remote.checkout(branch='main')
        self.remote.insert_one(self.DOCUMENT3)
        self.remote.register('v3_main')

    def test_pull_another_branch(self):
        self._setup_remote()
        self.local.pull(self.remote)
        self._update_remote_2()

        self.assertTrue(self.local.pull(self.remote, branch='b'))

        local_log = self.local.get_log('b')
        remote_log = self.remote.get_log('b')
        self.assertEqual(len(remote_log), len(local_log))
        self.assertEqual(local_log, remote_log)

        self.remote.checkout(1, branch='b')
        self.local.checkout(1, branch='b')
        self.assertEqual(2, self.local.count_documents({}))
        self.assertEqual(
            self.local.find_one({'_id': self.DOCUMENT['_id']}),
            self.remote.find_one({'_id': self.DOCUMENT['_id']})
        )
        self.assertEqual(
            self.local.find_one({'_id': self.DOCUMENT3['_id']}),
            self.remote.find_one({'_id': self.DOCUMENT3['_id']})
        )

        self.local.checkout(0, 'b')
        self.remote.checkout(0, 'b')
        self.assertEqual(3, self.local.count_documents({}))
        self.assertEqual(
            self.local.find_one({'_id': self.DOCUMENT['_id']}),
            self.remote.find_one({'_id': self.DOCUMENT['_id']})
        )
        self.assertEqual(
            self.local.find_one({'_id': self.DOCUMENT3['_id']}),
            self.remote.find_one({'_id': self.DOCUMENT3['_id']})
        )

    def test_pull_another_branch_from_incomplete_branch(self):
        self._setup_remote()
        self.local.pull(self.remote)
        self._update_remote_1()
        self._update_remote_2()
        self.local.pull(self.remote, branch='b')

        # # we need weak equality here because some entries of the
        # # remote log have more ``next`` references, since we haven't pushed
        # # the entire main branch
        local_log = self.local.get_log('b')
        remote_log = self.remote.get_log('b')
        self.assertEqual(len(remote_log), len(local_log))
        self.assertTrue(all([
            local_log[i].weakly_equals(remote_log[i])
            for i in range(len(local_log))
        ]))

    def test_pulling_when_local_diverged(self):
        self.local.init('v0')
        self.local.insert_one(self.DOCUMENT)
        self.local.register('v1')
        self.local.push(self.remote)

        self.remote.insert_one(self.DOCUMENT2)
        self.remote.register('v2_remote')
        self.local.insert_one(self.DOCUMENT3)
        self.local.register('v2_local')

        self.assertTrue(self.local.pull(self.remote))
        local_log = self.local.get_log()

        # 0, 1, 2 (remote), 3 -> merged
        self.assertEqual(4, len(local_log))

        # Test log entries are equal
        local_log.pop(0)
        remote_log = self.remote.get_log()
        self.assertTrue(all([
            local_log[i].weakly_equals(remote_log[i])
            for i in range(len(local_log))
        ]))

        self.assertEqual(3, self.local.count_documents({}))
        self.assertEqual(
            self.DOCUMENT,
            self.local.find_one({'_id': self.DOCUMENT['_id']})
        )
        self.assertEqual(
            self.DOCUMENT2,
            self.local.find_one({'_id': self.DOCUMENT2['_id']})
        )
        self.assertEqual(
            self.DOCUMENT3,
            self.local.find_one({'_id': self.DOCUMENT3['_id']})
        )

    def test_pull_when_local_diverged_and_same_docs_were_modified(self):
        self.local.insert_one(self.DOCUMENT)
        self.local.init('v0')
        self.assertTrue(self.local.push(self.remote))

        # Local and Remote modify different fields of the same document
        self.local.update_one(
            {'_id': self.DOCUMENT['_id']},
            {"$set": {'local_field': True}}
        )
        self.local.insert_one(self.DOCUMENT2)
        self.remote.update_one(
            {'_id': self.DOCUMENT['_id']},
            {"$set": {'remote_field': True}}
        )
        self.remote.insert_one(self.DOCUMENT3)
        sleep(SLEEP_TIME)
        self.local.register('v1_local')
        self.remote.register('v1_remote')

        self.assertTrue(self.local.pull(self.remote))

        self.assertEqual(2, self.local.version)
        self.assertEqual(3, len(self.local.get_log()))
        self.assertEqual(3, self.local.count_documents({}))
        target = deepcopy(self.DOCUMENT)
        target['remote_field'] = True
        target['local_field'] = True
        self.assertEqual(target, self.local.find_one({'_id': target['_id']}))

    def _conflicts_simple_setup(self):
        self.local.insert_one(self.DOCUMENT2)
        self.local.init('v0')
        self.local.push(self.remote)

        self.local.update_one(
            {'_id': self.DOCUMENT2['_id']},
            {"$set": {'conflicting_field': 1}}
        )
        self.remote.update_one(
            {'_id': self.DOCUMENT2['_id']},
            {"$set": {'conflicting_field': -1}}
        )
        sleep(SLEEP_TIME)
        self.local.register('v1_local')
        self.remote.register('v1_remote')

    def test_pull_with_conflicts(self):
        self._conflicts_simple_setup()
        with self.assertRaises(AutoMergeFailedError):
            self.local.pull(self.remote)

        self.assertTrue(self.local.has_conflicts())

    def test_pull_resolve_conflicts_by_ignoring_local_changes(self):
        self._conflicts_simple_setup()
        with self.assertRaises(AutoMergeFailedError):
            self.local.pull(self.remote)

        self.assertTrue(
            self.local.resolve_conflicts(discard_local_changes=True))
        self.assertFalse(self.local.has_conflicts())
        self.assertEqual(self.remote.find_one({}), self.local.find_one({}))
        self.assertEqual(1, len(self.local.branches()))

    def test_pull_resolve_conflicts(
            self,
            interactively_resolve_conflicts=False
    ):
        self._conflicts_simple_setup()
        with self.assertRaises(AutoMergeFailedError):
            self.local.pull(self.remote)

        if not interactively_resolve_conflicts:
            return

        self.assertTrue(self.local.resolve_conflicts())
        self.assertFalse(self.local.has_conflicts())
        self.assertEqual(1, len(self.local.branches()))


class TestVersionedCollectionPush(_RemoteBaseTest):

    def test_pushing_does_nothing_when_collection_untracked(self):
        self.assertFalse(self.local.push(self.remote))

    def test_pushing_to_self_is_not_allowed(self):
        self.local.init('v0')
        with self.assertRaisesRegex(InvalidOperation, 'are the same'):
            self.local.push(self.local)

    def test_push_with_untracked_remote_local_stash_and_changes(self):
        self.local.init()
        self.local.insert_one(self.DOCUMENT)
        self.local.stash()
        self.local.insert_one(self.DOCUMENT2)
        with self.assertRaises(InvalidOperation):
            self.local.push(self.remote)

    def test_push_with_branch_behind(self):
        self.local.init()
        self.local.insert_one(self.DOCUMENT)
        self.assertTrue(self.local.register('v1'))
        self.local.insert_one(self.DOCUMENT2)
        self.assertTrue(self.local.register('v2'))
        self.assertTrue(self.local.push(self.remote))

        self.assertTrue(self.local.delete_version_subtree(1, 'main'))
        with self.assertRaisesRegex(
                InvalidOperation, "tip of your current branch is behind"
        ):
            self.local.push(self.remote)

    def test_push_with_diverging_changes(self):
        self.local.init()
        self.local.insert_one(self.DOCUMENT)
        self.assertTrue(self.local.register('v1'))
        self.assertTrue(self.local.push(self.remote))

        self.local.insert_one(self.DOCUMENT2)
        self.assertTrue(self.local.register('v2'))

        self.remote.insert_one(self.DOCUMENT3)
        self.assertTrue(self.remote.register('v2'))

        with self.assertRaisesRegex(InvalidOperation, "have diverged"):
            self.local.push(self.remote)

    def test_push_initialises_remote_if_not_tracked(self):
        self.local.init('v0')
        self.assertEqual({'tracked': False}, self.remote.status())
        self.local.push(self.remote)
        self.assertTrue('current_version' in self.remote.status())

    def test_push_transfers_the_original_state_to_remote_if_not_tracked(self):
        self.local.insert_one(self.DOCUMENT)
        self.local.init('v0')
        self.assertTrue(self.local.push(self.remote))
        self.assertEqual(self.local.get_log(), self.remote.get_log())
        self.assertEqual(self.DOCUMENT, self.remote.find_one({}))
        self.assertEqual(1, self.remote.count_documents({}))

    def test_push_when_remote_untracked_and_local_has_changes(self):
        self.local.insert_one(self.DOCUMENT)
        self.local.init('v0')
        self.local.insert_one(self.DOCUMENT2)
        self.local.register('v1')
        self.local.insert_one(self.DOCUMENT3)
        self.assertTrue(self.local.push(self.remote))

        self.assertTrue(self.local.has_changes())
        self.assertEqual(1, self.local.version)
        self.assertEqual(3, self.local.count_documents({}))

    def test_push_a_new_collection_to_untracked_remote(self):
        self.local.insert_many([{'id': i} for i in range(53)])
        self.local.init()
        self.assertTrue(self.local.push(self.remote))
        self.assertEqual(53, self.remote.count_documents({}))

    def test_pushing_from_detached_head_without_specifying_branch(self):
        self.local.init('v0')
        self.local.insert_one(self.DOCUMENT)
        self.local.register('v1')
        self.local.checkout(0)
        with self.assertRaises(InvalidOperation):
            self.local.push(self.remote)

    def test_pushing_to_a_non_matching_collection(self):
        self.local.init('v0')
        remote = VersionedCollection(self.db_remote, name='Users2')
        with self.assertRaises(ValueError):
            self.local.push(remote)
        remote.drop()

    def test_pushing_when_no_changes_returns_true(self):
        self.local.init('v0')
        self.local.insert_one(self.DOCUMENT)
        self.local.register('v1')
        self.assertTrue(self.local.push(self.remote))
        self.assertEqual(1, self.remote.version)
        self.assertTrue(self.local.push(self.remote))

    def test_pushing_empty_branch(self):
        self.local.init('v0')
        self.local.insert_one(self.DOCUMENT)
        self.local.register('v1')
        self.local.checkout(0)
        self.local.create_branch('b')
        self.assertTrue(self.local.push(self.remote))
        self.assertEqual(2, len(self.remote.branches()))

    def test_pushing_nonexistent_branches(self):
        self.local.init()
        self.local.push(self.remote)
        with self.assertRaises(InvalidOperation):
            self.local.push(self.remote, 'who_am_i')

    def test_push_more_versions_at_once(self):
        self.local.init('v0')
        self.local.insert_one(self.DOCUMENT)
        self.local.register('v1')
        self.local.insert_one(self.DOCUMENT2)
        self.local.register('v2')
        self.local.insert_one(self.DOCUMENT3)
        self.local.register('v3')

        self.assertTrue(self.local.push(self.remote))

        # Assert tracking info correct
        self.assertEqual(self.local.branch, self.remote.branch)
        self.assertEqual(self.local.version, self.remote.version)
        self.assertEqual(self.local.get_log(), self.remote.get_log())

        # Assert remote in correct state
        self.assertIsNotNone(
            self.remote.find_one({'_id': self.DOCUMENT['_id']}))
        self.assertIsNotNone(
            self.remote.find_one({'_id': self.DOCUMENT2['_id']}))
        self.assertIsNotNone(
            self.remote.find_one({'_id': self.DOCUMENT3['_id']}))

    def test_push_a_different_branch_than_the_current_one(self):
        self.local.init('v0')
        self.local.insert_one(self.DOCUMENT)
        self.local.register('v1')

        # push current branch
        self.assertTrue(self.local.push(self.remote))

        # create a branch at version 0 on main
        self.local.checkout(0)
        self.local.insert_one(self.DOCUMENT2)
        self.local.register('v0_b', 'branch')
        self.local.insert_one(self.DOCUMENT3)
        self.local.register('v1_b')

        self.local.checkout(branch='main')
        self.assertTrue(self.local.push(self.remote, 'branch'))
        local_branches = set(self.local.branches())
        remote_branches = set(self.remote.branches())
        self.assertEqual(local_branches, remote_branches)
        self.assertEqual(2, len(remote_branches))

        # the branch was added without checking out there
        self.assertEqual(self.local.branch, self.remote.branch)

        self.remote.checkout(0, branch='branch')
        self.assertEqual(self.DOCUMENT2, self.remote.find_one({}))
        self.assertEqual(1, self.remote.count_documents({}))
        self.remote.checkout(1, branch='branch')
        self.assertEqual(2, self.remote.count_documents({}))
        self.assertEqual(self.DOCUMENT3, self.remote.find_one(self.DOCUMENT3))

    def test_push_branch_when_parent_branch_not_push_raises_error(self):
        self.local.init('v0')
        # push the initial (empty version)
        self.assertTrue(self.local.push(self.remote))

        self.local.insert_one(self.DOCUMENT)
        self.local.register('m_v1')

        # on branch 'b'
        self.local.create_branch('b')
        self.local.insert_one(self.DOCUMENT2)
        self.local.register('b_v0')
        self.local.insert_one(self.DOCUMENT3)
        self.local.register('b_v1')

        with self.assertRaises(InvalidOperation):
            self.local.push(self.remote, branch='b')

    def test_push_when_collection_has_changes(self):
        self.local.init('v0')
        self.local.insert_one(self.DOCUMENT)
        self.local.register('v1')
        self.local.insert_one(self.DOCUMENT2)
        self.assertTrue(self.local.push(self.remote))
        self.assertEqual(self.local.get_log(), self.remote.get_log())
        self.assertEqual(1, self.remote.count_documents({}))
        self.assertEqual(self.DOCUMENT, self.remote.find_one({}))
