from copy import deepcopy

from tests.test_versioned_collection.common import _RemoteBaseTest
from versioned_collection.errors import (
    InvalidOperation,
    InvalidCollectionState,
    AutoMergeFailedError,
)


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
        self.assertEqual(
            self.remote.count_documents({}), self.local.count_documents({})
        )
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
        self.assertTrue(self.remote.register('v1'))
        self.local.insert_one(self.DOCUMENT)
        self.assertTrue(self.local.register('v1'))

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
            {'_id': self.DOCUMENT['_id']}, {"$set": {'new_field': 'value1'}}
        )
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
            self.remote.find_one({'_id': self.DOCUMENT['_id']}),
        )
        self.assertEqual(
            self.local.find_one({'_id': self.DOCUMENT3['_id']}),
            self.remote.find_one({'_id': self.DOCUMENT3['_id']}),
        )

        self.local.checkout(0, 'b')
        self.remote.checkout(0, 'b')
        self.assertEqual(3, self.local.count_documents({}))
        self.assertEqual(
            self.local.find_one({'_id': self.DOCUMENT['_id']}),
            self.remote.find_one({'_id': self.DOCUMENT['_id']}),
        )
        self.assertEqual(
            self.local.find_one({'_id': self.DOCUMENT3['_id']}),
            self.remote.find_one({'_id': self.DOCUMENT3['_id']}),
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
        self.assertTrue(
            all(
                [
                    local_log[i].weakly_equals(remote_log[i])
                    for i in range(len(local_log))
                ]
            )
        )

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
        self.assertTrue(
            all(
                [
                    local_log[i].weakly_equals(remote_log[i])
                    for i in range(len(local_log))
                ]
            )
        )

        self.assertEqual(3, self.local.count_documents({}))
        self.assertEqual(
            self.DOCUMENT, self.local.find_one({'_id': self.DOCUMENT['_id']})
        )
        self.assertEqual(
            self.DOCUMENT2, self.local.find_one({'_id': self.DOCUMENT2['_id']})
        )
        self.assertEqual(
            self.DOCUMENT3, self.local.find_one({'_id': self.DOCUMENT3['_id']})
        )

    def test_pull_when_local_diverged_and_same_docs_were_modified(self):
        self.local.insert_one(self.DOCUMENT)
        self.local.init('v0')
        self.assertTrue(self.local.push(self.remote))

        # Local and Remote modify different fields of the same document
        self.local.update_one(
            {'_id': self.DOCUMENT['_id']}, {"$set": {'local_field': True}}
        )
        self.local.insert_one(self.DOCUMENT2)
        self.remote.update_one(
            {'_id': self.DOCUMENT['_id']}, {"$set": {'remote_field': True}}
        )
        self.remote.insert_one(self.DOCUMENT3)
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
            {'_id': self.DOCUMENT2['_id']}, {"$set": {'conflicting_field': 1}}
        )
        self.remote.update_one(
            {'_id': self.DOCUMENT2['_id']}, {"$set": {'conflicting_field': -1}}
        )
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
            self.local.resolve_conflicts(discard_local_changes=True)
        )
        self.assertFalse(self.local.has_conflicts())
        self.assertEqual(self.remote.find_one({}), self.local.find_one({}))
        self.assertEqual(1, len(self.local.branches()))

    def test_pull_resolve_conflicts(
        self, interactively_resolve_conflicts=False
    ):
        self._conflicts_simple_setup()
        with self.assertRaises(AutoMergeFailedError):
            self.local.pull(self.remote)

        if not interactively_resolve_conflicts:
            return

        self.assertTrue(self.local.resolve_conflicts())
        self.assertFalse(self.local.has_conflicts())
        self.assertEqual(1, len(self.local.branches()))
