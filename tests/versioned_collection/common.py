from typing import List
from unittest import TestCase

from pymongo import MongoClient

from versioned_collection import VersionedCollection


class User(VersionedCollection):
    SCHEMA = {'name': str, 'emails': List[str]}


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

        self.DOCUMENT = {'name': 'Goethe', 'emails': ['oh_my@goethe.com']}

        self.DOCUMENT2 = {'name': 'Euler', 'emails': ['euler@mathsclub.ch']}

    def tearDown(self) -> None:
        self.user_collection.drop()


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

        self.DOCUMENT = {'name': 'Goethe', 'emails': ['oh_my@goethe.de']}

        self.DOCUMENT2 = {'name': 'Euler', 'emails': ['euler@mathsclub.ch']}

        self.DOCUMENT3 = {
            'name': 'Gauss',
            'emails': ['gauss@mathsclub.de', 'gauss@astronomyclub.de'],
        }

    def tearDown(self) -> None:
        self.local.drop()
        self.remote.drop()
