from tests.test_versioned_collection.common import _BaseTest


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
        self.user_collection.update_one(
            {'name': self.DOCUMENT['name']}, {"$set": {'age': 99}}
        )
        
        self.user_collection.register('v1')

    def test_multiple_updates_to_doc_before_register_with_more_versions(self):
        self.user_collection.init('v0')
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.register('v1')
        self.user_collection.update_one(
            {'name': self.DOCUMENT['name']}, {"$set": {'age': 99}}
        )
        self.user_collection.update_one(
            {'name': self.DOCUMENT['name']}, {"$set": {'is_wizard': False}}
        )
        
        self.user_collection.register('v2')

    def test_multiple_updates_to_doc_before_register_with_more_versions2(self):
        self.user_collection.init('v0')
        self.user_collection.insert_one(self.DOCUMENT2)
        self.assertTrue(self.user_collection.register('v1'))
        self.user_collection.insert_one(self.DOCUMENT)
        self.assertTrue(self.user_collection.register('v2'))
        self.user_collection.update_one(
            {'name': self.DOCUMENT['name']}, {"$set": {'age': 199}}
        )
        self.user_collection.update_one(
            {'name': self.DOCUMENT['name']}, {"$set": {'is_wizard': False}}
        )
        
        self.assertTrue(self.user_collection.register('v3'))
        self.assertEqual(
            3, self.user_collection._deltas_collection.count_documents({})
        )

    def test_register_with_multiple_updates_from_empty_branch(self):
        self.user_collection.init('v0')
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.register('v1')
        self.user_collection.create_branch('b')
        self.user_collection.update_one(
            {'_id': self.DOCUMENT['_id']}, {"$set": {'name': 'GOETHE'}}
        )
        self.user_collection.update_one(
            {'_id': self.DOCUMENT['_id']}, {"$set": {'is_wizard': False}}
        )
        
        self.user_collection.register('b_v0')
        self.assertEqual(
            2, self.user_collection._deltas_collection.count_documents({})
        )

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
