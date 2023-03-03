import dataclasses
from time import sleep
from typing import Optional

from pymongo.collection import Collection
from pymongo.database import Database


class LockCollection(Collection):
    """ Collection holding the locking information.

    This collection is necessary when multiple users are concurrently
    interacting with a `VersionedCollection`.

    The documents in this collection have the following format::

        {
            _id: ObjectId(...)
            collection_name: 'name of the tracked collection'
            locked: True/ False
        }

    This collection is shared per database, holding information about each
    tracked collection in the database. By using atomic updates,
    each `VersionedCollection` can implement a simple locking mechanism.

    """

    # The current locking mechanism is not safe against adversarial usage.
    # Since the lock is represented by a shared collection, any other user can
    # call :meth:`unlock` before calling :meth:`lock` to be able to proceed in
    # executing a function. Since this is only an internal mechanism, and it is
    # not exposed it should be fine for now.

    @dataclasses.dataclass
    class SCHEMA:
        collection_name: str
        locked: bool

    def __init__(self, database: Database, **kwargs) -> None:
        super().__init__(database, '__vc_lock', **kwargs)

    def init_lock(self, collection: str):
        """ Initialises the lock """
        doc = {'collection_name': collection, 'locked': False}
        self.find_one_and_replace(
            filter=doc, replacement=doc, upsert=True
        )

    def is_locked(self, collection: str) -> Optional[bool]:
        return self.find_one(
            {'collection_name': collection}, projection={'locked': 1}
        )

    def try_lock_acquire(self, collection: str) -> bool:
        """ Tries to acquire the lock for the given collection.

        :return: ``True`` if the lock is successfully acquired, ``False`` if
            the lock is held by other process.
        """
        ret = self.find_one_and_update(
            filter={'collection_name': collection, 'locked': False},
            update={"$set": {'locked': True}},
        )
        return ret is not None

    def lock_acquire(self, collection: str) -> bool:
        """ Acquires the lock for the given collection.

        :param collection: The name of the collection to lock.
        :return: Whether the process waited for the lock.
        """
        has_waited_for_lock = False
        while not self.try_lock_acquire(collection):
            has_waited_for_lock = True
            sleep(0.1)
        return has_waited_for_lock

    def lock_release(self, collection: str) -> bool:
        """ Releases the lock for the given collection.

        :param collection: The name of the collection to unlock.
        :return: Whether the collection was locked.
        """
        ret = self.find_one_and_update(
            filter={'collection_name': collection, 'locked': True},
            update={"$set": {'locked': False}}
        )
        return ret is not None

    def remove_collection(self, collection: str) -> None:
        """ Removes the locking information for the given collection.

        :param collection: The name of the collection for which to remove the
            lock.
        """
        self.delete_one({'collection_name': collection})
        if self.count_documents({}) == 0:
            self.drop()
