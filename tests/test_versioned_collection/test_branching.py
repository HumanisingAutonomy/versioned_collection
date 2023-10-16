from copy import deepcopy

from bson import ObjectId

from tests.test_versioned_collection.common import _BaseTest, SLEEP_TIME
from versioned_collection.errors import (
    InvalidCollectionVersion,
    BranchNotFound,
)


class TestVersionedCollectionBranching(_BaseTest):

    def test_create_branch_returns_none_if_collection_not_initialised(self):
        self.assertIsNone(self.user_collection.create_branch('b'))

    def test_illegal_branch_name(self):
        self.user_collection.init()
        with self.assertRaisesRegex(
            ValueError, "Branch names cannot start with '__'"
        ):
            self.user_collection.create_branch('__super_secret_branch')

    def test_create_branch_when_detached(self):
        self.user_collection.init()

        self.user_collection.insert_one(self.DOCUMENT)
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

        n_branches = self.user_collection._branches_collection.count_documents(
            {}
        )
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

        n_branches = self.user_collection._branches_collection.count_documents(
            {}
        )
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
        self.assertTrue(self.user_collection.register('added doc', 'other'))

        with self.assertRaises(BranchNotFound):
            self.user_collection._branches_collection.get_branch('other')

        branch = self.user_collection._branches_collection.get_branch('main')
        self.assertEqual(1, branch.points_to_collection_version)

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

        ::

                  0_m
                  /
                1_m
              /     \\
            2_m     0_b2
            / \\       \\
          3_m 0_b1     1_b2
                        \\
                        2_b2
                          \\
                          3_b2

        self.DOCUMENT is updated in all versions except in 0_m;
        self.DOCUMENT2 is updated only in 1_m, 2_m and 0_b2
        """

        def _increase_v_and_update(doc):
            doc['v'] += 1
            self.user_collection.find_one_and_replace({'_id': doc['_id']}, doc)

        def _assert_version(doc, version):
            d = self.user_collection.find_one({"_id": doc['_id']})
            self.assertEqual(d['v'], version)

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
        _assert_version(self.DOCUMENT, 2)
        _increase_v_and_update(self.DOCUMENT)
        self.user_collection.register('0_b1', 'b1')

        self.user_collection.checkout(1, 'main')
        _assert_version(self.DOCUMENT, 1)
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
            self.user_collection.find_one_and_replace({'_id': doc['_id']}, doc)

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
            self.user_collection.find_one_and_replace({'_id': doc['_id']}, doc)

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
