from tests.test_versioned_collection.common import _RemoteBaseTest
from versioned_collection import VersionedCollection
from versioned_collection.errors import (
    InvalidOperation,
)


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
            self.remote.find_one({'_id': self.DOCUMENT['_id']})
        )
        self.assertIsNotNone(
            self.remote.find_one({'_id': self.DOCUMENT2['_id']})
        )
        self.assertIsNotNone(
            self.remote.find_one({'_id': self.DOCUMENT3['_id']})
        )

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
