from .common import _BaseTest


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
