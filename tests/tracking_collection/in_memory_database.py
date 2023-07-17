import logging
from functools import wraps
from unittest import TestCase

import pymongo.database
import pymongo_inmemory


class InMemoryDatabaseSetup(TestCase):
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
