
from tests.test_versioned_collection.common import _BaseTest


class TestVersionedCollectionDeleteSubtree(_BaseTest):

    def test_deleting_does_nothing_for_untracked_collections(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.assertFalse(self.user_collection.delete_version_subtree(0))

    def test_deleting_the_root_of_the_version_tree(self):
        self.user_collection.init('v0')
        self.user_collection.insert_one(self.DOCUMENT)
        self.assertTrue(self.user_collection.register('v1'))
        self.assertTrue(self.user_collection.delete_version_subtree(0))
        self.assertEqual({'tracked': False}, self.user_collection.status())
        self.assertIsNone(self.user_collection.find_one())

    def test_deleting_the_last_version(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init('v0')
        self.user_collection.insert_one(self.DOCUMENT2)
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
        self.assertTrue(self.user_collection.register('v0_other_branch'))

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
