from tests.test_tracking_collection.in_memory_database import InMemoryDatabaseSetup
from versioned_collection.collection.tracking_collections import LockCollection


class TestDeltasCollectionUnitTests(InMemoryDatabaseSetup):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()

    def setUp(self):
        self.col = LockCollection(self.database)

    def tearDown(self):
        self.col.drop()

    def test_init(self):
        col_name = "i_need_a_beer_a_beer_is_what_i_need"
        self.col.init_lock(col_name)
        res = self.col.find_one(projection={'_id': False})
        self.assertEqual({'collection_name': col_name, 'locked': False}, res)

    def test_init_does_not_overwrite_locked_collections(self):
        col_name = "i_need_a_beer_a_beer_is_what_i_need"
        self.col.init_lock(col_name)
        self.col.lock_acquire(col_name)
        self.assertTrue(self.col.is_locked(col_name))

        self.col.init_lock(col_name)
        self.assertTrue(self.col.is_locked(col_name))

    def test_remove_collection(self):
        self.col.init_lock('name')
        self.col.init_lock('name2')

        self.col.remove_collection('name2')
        res = list(self.col.find(projection={'_id': False}))
        self.assertEqual([{'collection_name': 'name', 'locked': False}], res)

        self.col.remove_collection('name')
        self.assertIsNone(self.col.find_one({}))

    def test_lock_acquire(self):
        self.col.init_lock('name')
        self.assertFalse(self.col.lock_acquire('name'))
        # held by someone else
        self.assertFalse(self.col.try_lock_acquire('name'))

    def test_lock_release(self):
        self.col.init_lock('name')
        self.assertFalse(self.col.lock_release('name'))
        self.assertFalse(self.col.lock_acquire('name'))
        self.assertTrue(self.col.lock_release('name'))
