from time import sleep

from tests.test_versioned_collection.common import _BaseTest, SLEEP_TIME
from versioned_collection.errors import (
    InvalidCollectionVersion,
)
from versioned_collection.utils.serialization import stringify_object_id


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
        self.assertEqual({}, self.user_collection.diff())

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
        self.user_collection.update_one(
            {'name': 'Goethe'}, {"$set": {'name': 'GOETHE'}}
        )
        self.user_collection.delete_one({'name': 'Euler'})
        sleep(SLEEP_TIME)
        diffs = self.user_collection.diff(deep=True)
        self.assertEqual(2, len(diffs))
        diffs = self.user_collection.diff(deep=False)
        self.assertEqual(2, len(diffs))
        ids = set(diffs.keys())
        doc_ids = {
            stringify_object_id(self.DOCUMENT['_id']),
            stringify_object_id(self.DOCUMENT2['_id']),
        }
        self.assertEqual(ids, doc_ids)

    def test_deep_diff(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init()
        self.user_collection.update_one(
            {'name': 'Goethe'}, {"$set": {'name': 'GOETHE'}}
        )
        sleep(SLEEP_TIME)
        diff = self.user_collection.diff(deep=True)[self.DOCUMENT['_id']]
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
        diffs = self.user_collection.diff(0, deep=True)
        self.assertEqual(2, len(diffs))

    def test_diffs_with_unregister_changes2(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        sleep(SLEEP_TIME)

        diffs = self.user_collection.diff(branch='main')
        self.assertEqual(1, len(diffs))
        diffs = self.user_collection.diff(branch='main', deep=True)
        self.assertEqual(1, len(diffs))

    def test_diffs_with_no_changes(self):
        self.user_collection.init()
        diff = self.user_collection.diff(branch='main')
        self.assertEqual(0, len(diff))
        diff = self.user_collection.diff(branch='main', deep=True)
        self.assertEqual(0, len(diff))

    def test_diffs_between_versions_with_untracked_changes(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        self.assertTrue(self.user_collection.register('v1'))
        self.user_collection.insert_one(self.DOCUMENT2)
        sleep(SLEEP_TIME)
        diffs = self.user_collection.diff(0, 'main')
        self.assertEqual(2, len(diffs))
        diffs = self.user_collection.diff(0, 'main', deep=True)
        self.assertEqual(2, len(diffs))

    def test_diffs_with_untracked_changes(self):
        self.user_collection.insert_one(self.DOCUMENT2)
        self.user_collection.init()
        self.user_collection.update_one(
            {'_id': self.DOCUMENT2['_id']}, {"$set": {'new_field': 'new_value'}}
        )
        sleep(SLEEP_TIME)

        diffs = self.user_collection.diff(0, direction='to', deep=True)
        self.assertEqual(1, len(diffs))
        diff = diffs[self.DOCUMENT2['_id']]
        self.assertIn('dictionary_item_removed', diff)
        self.assertEqual(1, len(diff['dictionary_item_removed']))  # noqa

    def test_diff_with_both_registered_and_untracked_changes(self):
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.init()
        self.user_collection.update_one(
            {'_id': self.DOCUMENT['_id']}, {"$set": {'field_1': 'value_1'}}
        )
        self.user_collection.insert_one(self.DOCUMENT2)
        sleep(SLEEP_TIME)
        self.user_collection.register('v1')
        self.user_collection.update_one(
            {'_id': self.DOCUMENT['_id']}, {"$set": {'field_2': 'value_2'}}
        )
        self.user_collection.update_one(
            {'_id': self.DOCUMENT2['_id']}, {"$set": {'field_1': 'value_1'}}
        )
        sleep(SLEEP_TIME)

        diffs = self.user_collection.diff(0, direction='to', deep=True)
        self.assertEqual(2, len(diffs))
        diff1 = diffs[self.DOCUMENT['_id']]
        diff2 = diffs[self.DOCUMENT2['_id']]
        self.assertIn('dictionary_item_removed', diff1)
        self.assertEqual(2, len(diff1['dictionary_item_removed']))  # noqa
        self.assertIn('dictionary_item_removed', diff2)
        self.assertEqual(4, len(diff2['dictionary_item_removed']))  # noqa

    def test_diff_ignores_no_ops(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.delete_one({'_id': self.DOCUMENT['_id']})
        diff = self.user_collection.diff(0, 'main')
        self.assertEqual(0, len(diff))
        diff = self.user_collection.diff(0, 'main', deep=True)
        self.assertEqual(0, len(diff))

    def tests_diffs_forward_and_backward(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.register('v1')
        self.user_collection.insert_one(self.DOCUMENT2)
        self.user_collection.register('v2')

        diff_backward_2 = self.user_collection.diff(0, deep=True)
        self.assertEqual(2, len(diff_backward_2))

        self.user_collection.checkout(1)
        diff_backward_1 = self.user_collection.diff(0, deep=True)
        self.assertEqual(1, len(diff_backward_1))

        self.user_collection.checkout(0)
        diff_forward_1 = self.user_collection.diff(1, deep=True)
        self.assertEqual(1, len(diff_forward_1))
        diff_forward_2 = self.user_collection.diff(2, deep=True)
        self.assertEqual(2, len(diff_forward_2))

        pairs = [
            (diff_forward_1, diff_backward_1),
            (diff_forward_2, diff_backward_2),
        ]
        for diff_forward, diff_backward in pairs:
            for obj_id in diff_forward.keys():
                diff_f = diff_forward[obj_id]
                diff_b = diff_backward[obj_id]

                self.assertIn('dictionary_item_removed', diff_f)
                self.assertIn('dictionary_item_added', diff_b)
                self.assertEqual(
                    diff_f['dictionary_item_removed'],  # noqa
                    diff_b['dictionary_item_added'],  # noqa
                )

    def test_diff_direction(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.register('v1')

        diff_0_1 = self.user_collection.diff(0, deep=True)
        diff_1_0 = self.user_collection.diff(0, direction='to', deep=True)

        self.assertEqual(1, len(diff_1_0))
        self.assertEqual(1, len(diff_0_1))

        diff_0_1 = diff_0_1[self.DOCUMENT['_id']]
        diff_1_0 = diff_1_0[self.DOCUMENT['_id']]
        self.assertIn('dictionary_item_added', diff_0_1)
        self.assertIn('dictionary_item_removed', diff_1_0)

    def test_bidirectional_diff(self):
        self.user_collection.init()
        self.user_collection.insert_one(self.DOCUMENT)
        self.user_collection.register('v1')

        diff = self.user_collection.diff(
            0, direction='bidirectional', deep=True
        )

        self.assertEqual(2, len(diff))
        self.assertIn('to', diff)
        self.assertIn('from', diff)

        diff_to = self.user_collection.diff(0, direction='to', deep=True)
        diff_from = self.user_collection.diff(0, direction='from', deep=True)
        self.assertEqual(diff_to, diff['to'])
        self.assertEqual(diff_from, diff['from'])
