import os
from time import sleep
from typing import List
from unittest import TestCase

from pymongo import MongoClient

from versioned_collection import VersionedCollection

# There's something weird going on when running the whole suite,
# causing some tests to randomly fail, while running the tests individually
# always works, so it's super hard to debug.
# This may be caused by the mongo change stream and the listener. Probably it
# takes some time for the change streams to warm up and building and
# destroying the collection after each test causes it.
SLEEP_TIME = 0.12


class User(VersionedCollection):
    SCHEMA = {'name': str, 'emails': List[str]}

    def register(self, *args, **kwargs) -> bool:
        sleep(SLEEP_TIME)
        return super().register(*args, **kwargs)

    def diff(self, *args, **kwargs):
        sleep(SLEEP_TIME)
        return super().diff(*args, **kwargs)


def _fetch_connection_string() -> str:
    username = os.getenv("VC_MONGO_USER", "")
    password = os.getenv("VC_MONGO_PASSWORD", "")
    credentials = f"{username}:{password}@" if len(username) else ""
    host = "localhost"
    port = 27017
    return f"mongodb://{credentials}{host}:{port}"


class _BaseTest(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super(_BaseTest, cls).setUpClass()
        connection = MongoClient(_fetch_connection_string())
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
        conn_str = _fetch_connection_string()
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
