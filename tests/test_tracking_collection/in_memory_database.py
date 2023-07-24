import logging
from functools import wraps
from unittest import TestCase

import pymongo.database
import pymongo_inmemory


# TODO: share this instance across all tests in a test module to speed things
#  up.
class InMemoryDatabaseSetup(TestCase):
    """An in-memory MongoDB instance."""
    # Unfortunately the full in-memory database is not free, so
    # ``pymongo-inmemory`` uses a `ephemeralForTest` instance, which does not
    # support replica sets, therefore change streams, so we cannot use it to
    # test the full functionality of VersionedCollection.
    # Another annoying thing is that it's pretty hard to mock a
    # pymongo.database since there are a lot of type checks inside pymongo,
    # so it's just easier to spawn an instance even if it's not always used.
    # The most expensive part it starting the instance, but otherwise it's fast.

    client: pymongo_inmemory.MongoClient
    database_name: str
    database: pymongo.database.Database

    @classmethod
    def setUpClass(cls) -> None:
        logging.getLogger("PYMONGOIM_DOWNLOADER").setLevel(logging.CRITICAL)
        cls.client = pymongo_inmemory.MongoClient()
        logging.getLogger("PYMONGOIM_DOWNLOADER").setLevel(logging.WARNING)
        database_name = '__test__in_memory_db__'
        cls.database = cls.client[database_name]


def build_and_destroy_collection(fn):
    @wraps(fn)
    def wrapper(self):
        self.collection.build()
        fn(self)
        self.collection.drop()

    return wrapper
