from unittest.mock import patch, MagicMock

import pymongo.collection
import pymongo.database

from tests.test_tracking_collection.in_memory_database import InMemoryDatabaseSetup
from versioned_collection.collection.tracking_collections import ReplicaCollection


class TestReplicaCollection(InMemoryDatabaseSetup):

    def setUp(self) -> None:
        self._target_collection_name = 'col'
        self.collection = ReplicaCollection(
            self.database, self._target_collection_name
        )

    def test_build_creates_snapshot(self):
        with patch.object(ReplicaCollection, 'build') as mock:
            self.collection.build()
            mock.assert_called_once()

    def test_create_snapshot(self):
        with patch.object(pymongo.database.Database, '__getitem__') as get_mock:
            mock = MagicMock()
            get_mock.return_value = mock
            self.collection.create_snapshot()
            get_mock.assert_called_once_with(self._target_collection_name)
            mock.aggregate.assert_called_once_with(
                [{"$match": {}}, {"$out": self.collection.name}]
            )
