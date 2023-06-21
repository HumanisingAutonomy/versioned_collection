from __future__ import annotations

import datetime
import os
import subprocess
import warnings
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from functools import partial, wraps
from multiprocessing import cpu_count, Pool
from shutil import rmtree
from typing import (
    Optional, List, Any, Dict, Tuple, Union, Set, overload, Literal,
)

import pymongo
from bson import ObjectId
from deepdiff import DeepDiff
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from versioned_collection.collection.tracking_collections import (
    ModifiedCollection,
    MetadataCollection,
    LogsCollection,
    DeltasCollection,
    BranchesCollection,
    ReplicaCollection,
    ConflictsCollection,
    StashContainer,
    LockCollection,
)
from versioned_collection.errors import (
    CollectionAlreadyInitialised, InvalidOperation,
    InvalidCollectionVersion, InvalidCollectionState,
    BranchNotFound, AutoMergeFailedError
)
from versioned_collection.listener import CollectionListener
from versioned_collection.tree import Tree
from versioned_collection.utils.mongo_query import (
    group_documents_by_id, generate_pagination_query
)
from versioned_collection.utils.multi_processing import chunk_list
from versioned_collection.utils.serialization import (
    stringify_object_id, stringify_document, parse_json_document
)


def _collection_modified_by_pipeline(
    pipeline: List[Dict[str, Any]],
    collection_name: str,
    database_name: str
) -> bool:
    """ Checks an aggregation pipeline for collection changes.

    The only way an aggregation pipeline can modify a collection is by using
    the ``$out`` or ``$merge`` operators, which are always last in the pipeline.

    :param pipeline: the aggregation pipeline to be inspected.
    :param collection_name: the name of the collection.
    :param database_name: the name of the database.
    :return: ``True`` if the pipeline contains ``$out`` or ``$merge`` operators
        that modify the collection with `collection_name`, located on
        database with name `database_name`, ``False`` otherwise.
    """
    if (
        not len(pipeline)
        or not ("$out" in pipeline[-1] or "$merge" in pipeline[-1])
    ):
        return False

    for operator, argument in pipeline[-1].items():
        if isinstance(argument, str) and argument == collection_name:
            return True
        elif isinstance(argument, dict):
            # then the argument is a dictionary of shape
            # {"db": <database name>, "coll": <collection name>}
            if (
                argument['db'] == database_name
                and argument['coll'] == collection_name
            ):
                return True
    return False


class VersionedCollection(Collection):
    """ A tracked and versioned MongoDB collection.

    .. warning::
        All the interactions with the collection should be done through this
        class, and not by directly accessing the collection using the
        `pymongo` driver. An exception to this is when the listener is
        started using the CLI via ``vc listen``.

    .. warning::
        Note that outputting the result of an aggregation pipeline directly into
        the versioned collection using the ``$out`` or ``$merge`` stages will
        not track the changes. This is caused by how those commands are
        processed by MongoDB itself. For instance, ``$out`` creates a temporary
        collection, drops the original collection and then renames the
        temporary collection.

    To enable versioning on a collection, create a class that inherits
    from :class:`VersionedCollection`, or create a :class:`VersionedCollection`
    object and pass the name of the collection as well.

    Usage example:

    .. code-block:: python

        import pymongo
        from versioned_collection import VersionedCollection

        class Users(VersionedCollection):
            pass

        client = pymongo.MongoClient("mongodb://localhost:27017")
        db = client['database_name']

        user_collection = Users(db)
        # OR
        same_db_collection = VersionedCollection(db, name='users')

    Under the hood, the :class:`VersionedCollection` module uses pymongo to
    manage the interactions with the database, so all the available features
    and commands available in `pymongo` are available with a
    :class:`VersionedCollection` at no extra cost.

    """

    def __init__(self,
                 database: Database,
                 name: Optional[str] = None,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 **kwargs
                 ) -> None:
        """ Constructs a new :class:`VersionedCollection`.

        .. note::
            If access control is enabled then the username and the password of
            a user that has ``readWrite`` permissions for the database with name
            `database`,  should be provided. This is required because the
            collection listener starts a new database connection.

        If the collection has conflicts, a warning (reminder) message is
        prompted upon initialisation. It is the responsibility of the user
        to properly handle conflicts and make sure the collection is on the
        correct state after the conflicts have been solved,

        :param db: A :class:`pymongo.database.Database` instance.
        :param name: The name of the collection. If not given, it will
            default to the lower-cased class name.
        :param username: The name of a user that has access to `database`.
        :param password: The password of a user that has access to `database`.
        """

        if name is None:
            name = type(self).__name__.lower()
        super(VersionedCollection, self).__init__(database, name, **kwargs)
        self.__credentials = username, password
        self.__kwargs = kwargs

        self.SCHEMA = None if not hasattr(self, 'SCHEMA') else self.SCHEMA

        # A versioned collection is tracked if there exists a log book in the
        # database associated to it.
        self._tracked = LogsCollection.format_name(self.name) in \
                        self.database.list_collection_names()

        self._locked: Optional[bool] = None
        self._should_reload_tracking_cache = False

        if self._tracked:
            self._load_lock_collection()
            self._load_tracking_collections()
            self._start_listener()
            self._current_version = \
                self._meta_collection.metadata.current_version
            self._current_branch = \
                self._meta_collection.metadata.current_branch

    def __del__(self):
        if self._tracked:
            self._listener.stop()

    def __hash__(self) -> int:
        branches = sorted([
            b.name for b in self._branches_collection.get_empty_branches()
        ])
        branches_hash = hash('__'.join(branches))
        return hash((self.name, branches_hash, self.versiontree))

    def __eq__(self, other: object) -> bool:
        """
        Two :class:`VersionCollection` objects are equal if they have the
        same name and the same version trees. The log tree is almost the
        version tree, but it misses the empty branches pointers, so those
        have to be checked for equality separately.

        .. note::
            Equality assumes the equality of the versioning data, but not
            of the data itself.
        """

        if other is None:
            return False
        if not isinstance(other, VersionedCollection):
            return False
        if self is other:
            return True
        if self.name != other.name:
            return False

        if not self.is_tracked() and not other.is_tracked():
            return True

        if self.is_tracked() != other.is_tracked():
            return False

        these_empty_br = self._branches_collection.get_empty_branches()
        those_empty_br = other._branches_collection.get_empty_branches()
        if len(these_empty_br) != len(these_empty_br):
            return False
        for this_branch in these_empty_br:
            if this_branch not in those_empty_br:
                return False

        return self.version_tree == other.version_tree

    def _is_sub_collection_of(self, other: object, strict: bool) -> bool:
        """ Checks if this collection is and 'older' version of `other`.

        To compare the collections we have to compare the version trees.
        Since the version tree structure is formed by the combination of the
        structures stored in different tracking collections, we have to
        compare the following:
            * The log trees. This allows to compare the structure of the
              version tree, especially the recorded versions.
            * The set of empty branches of the two collections, since those
              are not recorded in the log tree, since they have no version
              registered on them.

        :param other: The other object to compare.
        :param strict: To check if ``self < other`` or ``self <= other``
        :return: The result of the comparison. Whether this collection is a
            sub-collection of `other`.
        """
        op_str = '<' if strict else '<='
        if not isinstance(other, VersionedCollection):
            raise TypeError(
                f"{op_str} not supported between instances of "
                f"'VersionedCollection' and '{type(other)}'"
            )
        if self is other:
            return not strict
        if self.name != other.name:
            # This can simply return ``False``, but I think it's better like
            # this to prevent nasty bugs
            raise ValueError(
                f"Cannot compare collections with different names with "
                f"{op_str}. Names: '{self.name}' and '{other.name}'"
            )

        if not self.is_tracked() and not other.is_tracked():
            # This can also be False
            # The idea here is that we test collections for versioning,
            # so untracked collections are in fact analogous to empty sets,
            # i.e., empty version trees.
            return True

        if self.is_tracked() != other.is_tracked():
            return False

        these_empty_branches = self._branches_collection.get_empty_branches()
        those_empty_branches = other._branches_collection.get_empty_branches()

        if len(these_empty_branches) > len(those_empty_branches):
            return False

        for branch in these_empty_branches:
            if branch not in those_empty_branches:
                return False

        if strict:
            ret = (
                self.version_tree < other.version_tree
                and len(these_empty_branches) <= len(those_empty_branches)
                or
                self.version_tree == other.version_tree
                and len(these_empty_branches) < len(those_empty_branches)
            )
        else:
            ret = (
                self.version_tree <= other.version_tree
                and len(these_empty_branches) <= len(those_empty_branches)
            )
        return ret

    def __lt__(self, other: object) -> bool:
        return self._is_sub_collection_of(other, strict=True)

    def __le__(self, other: object) -> bool:
        return self._is_sub_collection_of(other, strict=False)

    def __gt__(self, other: object) -> bool:
        return not (self < other or self == other)

    def __ge__(self, other: object) -> bool:
        return not self < other or self == other

    @property
    def version(self) -> Optional[int]:
        """ Returns the current version id of this collection.

        If the collection is not tracked, it returns ``None``.
        """
        return self._current_version if self._tracked else None

    @property
    def branch(self) -> Optional[str]:
        """ Returns the current branch name of this collection.

        If the collection is not tracked, it returns ``None``.
        """
        return self._current_branch if self._tracked else None

    @property
    def version_tree(self) -> Optional[Tree]:
        """ Returns the tree of register versions of this collection. """
        return self._log_collection.log_tree

    def _check_for_changes(op: str):
        """ Checks if the operation has modified the collection.

        A decorator inspecting the result returned by various potentially
        collection modifying MongoDB operations and deciding whether a
        collection was indeed modified in any way.

        This decorator should decorate any method of a :class:`Collection` (or
        even of :class:`VersionedCollection` if more MongoDB functions will be
        added) that can potentially modify the state of the underlying
        collection.
        """

        def decorator(function):
            @wraps(function)
            def wrapper(self, *args, **kwargs):
                ret = function(self, *args, **kwargs)
                if not self._tracked:
                    return ret

                if op == 'update':
                    if ret.modified_count > 0:
                        self._has_changed()
                    if hasattr(ret, 'upserted_id'):
                        if ret.upserted_id is not None:
                            self._has_changed()

                elif op == 'insert':
                    self._has_changed()

                elif op == 'find':
                    # Here we need to distinguish between vanilla finds
                    # and flavours of ``findAndModify`` that upsert the
                    # document. If the document targeted by the `filter`
                    # parameter does not exist and ``upsert=False``,
                    # then nothing happens, but if ``upsert=True`` we get
                    # an insert. If the document exists, then the command
                    # will return the document either before or after the
                    # operation has completed, so it won't be ``None`` and we
                    # can still catch it.
                    if function.__name__ in {
                        'find_one_and_replace', 'find_one_and_update'
                    }:
                        if (
                            kwargs.get('upsert', False)
                            or len(args) >= 5 and args[4]
                        ):
                            # `upsert` is the 5th argument
                            self._has_changed()
                    if ret is not None:
                        self._has_changed()

                elif op == 'delete':
                    if ret.deleted_count > 0:
                        self._has_changed()

                elif op == 'aggregate':
                    if _collection_modified_by_pipeline(
                        pipeline=args[0],
                        collection_name=self.name,
                        database_name=self.database.name
                    ):
                        self._has_changed()

                elif op == 'bulk':
                    any_count = (
                        ret.modified_count + ret.deleted_count
                        + ret.inserted_count + ret.upserted_count
                    )
                    if any_count > 0:
                        self._has_changed()
                return ret

            return wrapper

        return decorator

    def _load_lock_collection(self) -> None:
        """ Loads the collection holding the locking information. """
        self._lock_collection = LockCollection(self.database)
        self._lock_collection.init_lock(self.name)

    def _load_tracking_collections(self) -> None:
        """ Loads the associated tracking collection.

        The collection are just loaded, but not actually built on the
        database, in the case this collection is not initialised for versioning
        yet.
        """
        args = self.database, self.name
        self._modified_collection = ModifiedCollection(*args)
        self._meta_collection = MetadataCollection(*args)
        self._log_collection = LogsCollection(*args)
        self._deltas_collection = DeltasCollection(*args)
        self._replica_collection = ReplicaCollection(*args)
        self._branches_collection = BranchesCollection(*args)
        self._conflicts_collection = ConflictsCollection(*args)
        self._stash_container = StashContainer(*args)

        if self._conflicts_collection.exists():
            warnings.warn(
                f"Collection '{self.name}' has conflicts. Resolve the "
                f"conflicts before proceeding!"
            )

        self._tracking_collections: List[Any] = [
            self._modified_collection,
            self._meta_collection,
            self._log_collection,
            self._deltas_collection,
            self._branches_collection,
            self._replica_collection,
        ]
        self._temporary_tracking_collections = [
            self._conflicts_collection,
            self._stash_container
        ]

        self._should_reload_tracking_cache = False

    def _synchronize(func):
        """ Locks this collection for versioning operations.

        .. warning::
            This does not implement a collection lock at the database level.
        """

        @wraps(func)
        def wrapper(self, *args, **kwargs):
            self._lock()
            if self._should_reload_tracking_cache:
                self._load_tracking_collections()
            try:
                ret = func(self, *args, **kwargs)
            except Exception as e:
                raise e
            finally:
                self._unlock()
            return ret

        return wrapper

    def _lock(self):
        if self._locked is not None and not self._locked:
            waited_for_lock = self._lock_collection.lock_acquire(self.name)
            # If we waited for the lock it means that other process held it,
            # so the caches stored by the tracking collections may be
            # invalid, therefore reload them
            self._should_reload_tracking_cache = waited_for_lock
            self._locked = True

    def _unlock(self):
        if self._locked is not None and self._locked:
            self._lock_collection.lock_release(self.name)
            self._locked = False

    def init(self, message: Optional[str] = None) -> None:
        """ Initialises this collection for tracking.

         Creates a snapshot of the current state of the collection and
         initialises the collection used for tracking this collection.
         The current version of this collection is recorded as version ``0``.

         A versioned collection can be initialised only once. For registering
         another version of the collection, call :meth:`register()`.

        Usage example:

        .. code-block:: python

            collection = VersionedCollection(db, 'my_collection')
            collection.init('Initial version.')

        :raises `versioned_collection.errors.CollectionAlreadyInitialised`:
            If this collection has already been initialised.
        :param message: A short description of the initial state of the
            collection.
        """
        if self._tracked:
            raise CollectionAlreadyInitialised()

        self._tracked = True
        self._current_version = 0
        self._current_branch = 'main'

        self._load_tracking_collections()
        self._load_lock_collection()

        # Create the tracking collection.
        self._log_collection.build(message=message)
        for coll in self._tracking_collections:
            if coll != self._log_collection:
                coll.build()

        # Start listening to this collection
        self._start_listener()

    def _start_listener(self) -> None:
        """ Starts the listener for this collection.

        The listener will monitor the  changes to this collection and record
        them in the attached :class:`ModifiedCollection` collection.
        """
        host, port = self.database.client.address
        self._listener = CollectionListener(
            database_name=self.database.name,
            collection_name=self.name,
            host=host,
            port=port,
            credentials=self.__credentials
        )

    def drop(self, *args, **kwargs) -> None:
        """ Drops this versioned collection.

        In case this collection is being tracked, it also removes all the
        tracking information.

        .. warning::
            Calling this method is the only valid way of properly dropping
            a tracked collection. Calling ``db.drop_collection(name)`` will
            result in the removal of this collection only.

        """
        if self._tracked:
            self._listener.stop()
            for col in (
                self._tracking_collections
                + self._temporary_tracking_collections
            ):
                col.drop()
            self._tracked = False
            self._lock_collection.remove_collection(self.name)
        super().drop(*args, **kwargs)

    def rename(self, new_name: str, *args, **kwargs) -> VersionedCollection:
        """ Renames this collection and the tracking collections.

        The rename operation returns a new collection.

        Usage example:

        .. code-block:: python

            collection = VersionedCollection(db, 'usrs')
            collection = collection.rename(new_name='users')


        See the :meth:`rename()` method of the superclass for more information.

        :param new_name: The new name of the collection.
        :param args: The rest of the args.
        :param kwargs: The rest of the kwargs.

        :return: a new instance of :class:`VersionedCollection`.
        """
        super().rename(new_name, *args, **kwargs)
        if self._tracked:
            for coll in (
                self._tracking_collections
                + self._temporary_tracking_collections
            ):
                coll.rename(new_name, *args, **kwargs)
        return VersionedCollection(
            self.database, new_name, *self.__credentials, **self.__kwargs
        )

    @_synchronize
    def create_branch(self, branch_name: str) -> Tuple[int, str]:
        """ Creates a branch with the given name and checks out to it.

        When creating a new branch changes are allowed to exist since the
        last registered version. This allows checking out a previous version
        of the collection on any branch (other version than the version the
        branch's head points to), modifying the collection and then registering
        the new changes as a new version on a new branch.

        After creating a new branch, the version of the collection is set to
        ``-1``, indicating that there are no versions registered on the newly
        created branch.

        .. code-block:: python

            >>> collection: VersionedCollection # assume it exists in scope
            >>> collection.version, collection.branch
            (10, 'main')
            >>> collection.checkout(5)
            True
            >>> collection.create_branch('branch')
            (5, 'main')
            >>> collection.version, collection.branch
            (-1, 'branch')
            >>> collection.is_detached()
            False

        :raises `ValueError`: If `branch_name` starts with ``__``.
        :raises `ValueError`: If a branch with name `branch_name` already
            exists.
        :param branch_name: The name of the new branch. Can be any string,
            but it cannot start with double underscore (``__``).
        :return: The version id and branch name of the version the new branch
            points to, i.e., the previous position of the head.
        """
        if branch_name.startswith('__'):
            raise ValueError("Branch names cannot start with '__'")

        curr_branch = self._current_branch
        curr_version = self._current_version
        if self._current_version == -1:
            # Currently, we are on a new branch, without any versions on it.
            # We need to find the 'base' branch (which has versions
            # registered on it) the current branch is pointing to.
            curr = self._branches_collection.get_branch(curr_branch)
            while curr.points_to_branch != curr_branch:
                curr = self._branches_collection.get_branch(curr_branch)
                curr_branch = curr.points_to_branch
            curr_version = curr.points_to_collection_version

        # Create the new branch
        self._branches_collection.create_branch(
            branch=branch_name,
            pointing_to_collection_version=curr_version,
            pointing_to_branch=curr_branch
        )
        prev_version = curr_version
        prev_branch = curr_branch

        # Update the metadata
        self._current_version = -1
        self._current_branch = branch_name
        self._meta_collection.set_metadata(
            current_version=self._current_version,
            current_branch=self._current_branch,
            detached=False,
            changed=self.has_changes()
        )

        return prev_version, prev_branch

    @_synchronize
    def register(self, message: str, branch_name: Optional[str] = None) -> bool:
        """ Registers a new version of this collection.

        When the head is detached, a new branch with name `branch_name` is
        created pointing to the currently checked out version. This is
        equivalent to calling :meth:`create_branch` with `branch_name` as
        paramenter and then registering the new version of the collection. If
        the head is attached, i.e., it points to the latest version on the
        current branch, then the `branch_name` parameter is ignored and this
        will **not** register the version on a new branch.

        .. code-block:: python

            >>> collection: VersionedCollection # assume it exists in scope
            >>> collection.branch, collection.has_changes()
            ('main', True)
            >>> collection.register('New version')
            True
            >>> collection.version
            2
            >>> collection.checkout(1)
            True
            >>> collection.insert_one({'example': 'doc'})
            >>> collection.register('Another version', branch_name='new')
            >>> collection.version, collection.branch
            (0, 'new')


        :raises `ValueError`: If no branch name parameter is provided
            when the head is detached or if a branch with name `branch_name`
            already exists.
        :param message: The message associated with the new version of the
            collection.
        :param branch_name: The name of the branch on which to register the
            new version. This is ignored if the head is not detached.
        :return: Whether the collection was successfully registered.
        """

        # Cannot register a new version if no changes were made.
        if not self.has_changes():
            return False

        previous_version, previous_branch = None, None
        # If the head is detached, create a new branch and check out to it.
        if self.is_detached():
            if branch_name is None:
                raise ValueError(
                    "The branch name cannot be `None` when registering a new "
                    "version in detached mode!"
                )

            previous_version, previous_branch = self.create_branch(branch_name)

        elif self._current_version == -1:
            # Register a version on a freshly created branch
            branch = self._branches_collection.get_branch(self._current_branch)
            previous_version = branch.points_to_collection_version
            previous_branch = branch.points_to_branch

        # The head is not detached then the new version is about to be
        # registered on an existing branch.
        if previous_version is None or previous_branch is None:
            previous_version = self._current_version
            previous_branch = self._current_branch

        logs = self._log_collection.get_log(
            version=previous_version, branch=previous_branch
        )
        logs = [(log.version, log.branch) for log in logs]

        modified_tracker_docs = \
            self._modified_collection.find_modified_documents_ids()

        if len(modified_tracker_docs) == 0:
            self._clear_changes()
            return False

        now = datetime.datetime.utcnow()

        register_fn = partial(
            self._register_chunk,
            logs=logs,
            coll_name=self.name,
            branch=self._current_branch,
            version=self._current_version + 1,
            timestamp=now,
            database_name=self.database.name,
            address=self.database.client.address,
            credentials=self.__credentials
        )

        has_registered_deltas = False
        # An optimistic way of making sure that all changes are picked up.
        # Ideally the collection should be locked for updates before register
        # is called.
        while len(modified_tracker_docs) > 0:
            # Split the list into chunks
            modified_tracker_docs = chunk_list(modified_tracker_docs)

            with Pool(cpu_count()) as pool:
                statuses = pool.map(register_fn, modified_tracker_docs)

            has_registered_deltas = has_registered_deltas or any(statuses)

            # Make sure that all the changes are grabbed
            modified_tracker_docs = \
                self._modified_collection.find_modified_documents_ids()

        if not has_registered_deltas:
            self._clear_changes()
            return False

        # Update the tracking information for the newly registered version
        self._current_version += 1

        # Update the metadata
        self._meta_collection.set_metadata(
            current_version=self._current_version,
            current_branch=self._current_branch,
            detached=False,
            changed=False
        )

        self._clear_changes()

        # Update the branch pointer
        self._branches_collection.update_branch(
            branch=self._current_branch,
            pointing_to_collection_version=self._current_version,
            pointing_to_branch=self._current_branch
        )

        # Create a log entry
        self._log_collection.add_log_entry(
            previous_version=previous_version,
            previous_branch=previous_branch,
            current_branch=self._current_branch,
            message=message,
            timestamp=now
        )

        # Create the snapshot
        self._replica_collection.create_snapshot()

        assert self._modified_collection.count_documents({}) == 0
        return True

    @staticmethod
    def _register_chunk(
        modified_tracker_docs: List[Dict[str, ObjectId | List[ObjectId]]],
        logs: List[Tuple[int, str]],
        coll_name: str,
        branch: str,
        version: int,
        timestamp: datetime.datetime,
        database_name: str,
        credentials: Tuple[Optional[str], Optional[str]],
        address: Tuple[str, str]
    ) -> bool:
        """ Helper used to register new changes.

        This method is called by a worker process that registers the changes
        made to this collection.

        :param modified_tracker_docs: A list of documents containing
            the ids of the modified documents and the ids of the trackers.
        :param logs:  A list containing (version, branch) tuples from the
            current version (the version that is about to be registered)
            to the root of the version tree.
        :param coll_name: The name of this collection.
        :param branch: The name of the current branch.
        :param version: The version number used to register the new version.
        :param timestamp: The time when the new version is registered.
        :param database_name: The name of the database where the tracked
            collection is located on.
        :param credentials: Username and password.
        :param address: Host and port.
        :return: ``True`` if at least one delta has been registered,
            ``False`` otherwise.
        """

        client = MongoClient(
            host=address[0],
            port=int(address[1]),
            username=credentials[0],
            password=credentials[1]
        )
        database = client[database_name]

        replica_collection = ReplicaCollection(database, coll_name)
        this_collection = Collection(database, coll_name)
        deltas_collection = DeltasCollection(database, coll_name)
        modified_collection = ModifiedCollection(database, coll_name)

        tracker_ids = list()
        has_registered_deltas = False
        for tracker_doc in modified_tracker_docs:
            # Retrieve the documents
            replica_doc = replica_collection.find_one(
                {'_id': tracker_doc['_id']}
            )
            # If not found it was freshly added
            replica_doc = {} if replica_doc is None else replica_doc
            this_doc = this_collection.find_one({'_id': tracker_doc['_id']})
            # If not found it was deleted since last version
            this_doc = {} if this_doc is None else this_doc

            # Generate and store the deltas
            res = deltas_collection.add_delta(
                document_old=replica_doc,
                document_new=this_doc,
                document_id=tracker_doc['_id'],
                collection_version=version,
                branch=branch,
                timestamp=timestamp,
                branch_history=logs
            )
            tracker_ids.extend(tracker_doc['tracker_ids'])
            has_registered_deltas = has_registered_deltas or res is not None

        # Remove the processed documents
        modified_collection.delete_modified(tracker_ids)
        return has_registered_deltas

    @_synchronize
    def checkout(self,
                 version: Optional[int] = None,
                 branch: Optional[str] = None
                 ) -> bool:
        """ Checks out the given version of this collection.

        Collection versions have to exist (be registered) before checking
        them out.

        If changes were made since the latest registered version, they have to
        be discarded, registered or stashed, before checking out to another
        version.

        For checking out versions of the collection on the same branch as the
        working branch, the `branch` parameter can be skipped. For checking
        out the version the head of the branch is pointing to, the `version`
        parameter can be omitted.

        .. code-block:: python

            col = VersionedCollection(db, name='col')
            col.init('v0')  # created version 0 on 'main'
            col.insert_one({'doc': 'example'})
            col.register('v1')  # created version 1 on 'main'

            # checkout to v0
            col.checkout(0)  # now at version 0 on 'main'

            col.create_branch('branch1')  # on 'branch1', no versions registered
            col.create_branch('branch2')  # on 'branch2'
            col.checkout(0, 'main')  # on branch 'main' at version 0
            col.checkout(branch='branch1')  # on branch 'branch1'
            col.checkout(branch='branch2')  # on branch 'branch2'

            col.checkout(branch='main')  # now at version 1 on 'main'

        :raises `~versioned_collection.errors.InvalidCollectionVersion`:
            If given version does not match any recorded versions.

        :raises `~versioned_collection.errors.InvalidOperation`:
            If called is called when the collection has unregistered changes.

        :raises `ValueError`: If called without providing at least one argument.

        :param version: The version of the collection to be checked out.
        :param branch: The branch of the collection to check out to.
        :return: ``True`` if the operation succeeds, ``False`` if the checkout
            is not performed, but no errors were raised.
        """

        if version is None and branch is None:
            raise ValueError(
                "Invalid arguments to checkout!"
                "Provide at least on of 'version' or 'branch'"
            )
        if not self._tracked:
            return False
        if self.has_changes():
            raise InvalidOperation(
                "You tried to check out to another version of the "
                "collection, but unregistered changes were detected. "
                "Consider registering, discarding or stashing the changes"
                "before."
            )

        # Set the destination version and branch
        branch = self._current_branch if branch is None else branch
        curr_branch_data = self._branches_collection.get_branch(
            self._current_branch
        )
        dest_branch_data = curr_branch_data
        if branch != self._current_branch:
            dest_branch_data = self._branches_collection.get_branch(branch)
        version = dest_branch_data.points_to_collection_version \
            if version is None else version

        if self._current_version == -1:
            if version == 0 and branch == self._current_branch:
                raise InvalidCollectionVersion(version, branch)

            # The current branch is empty, i.e., there are no versions
            # registered on it. Move the HEAD to the base branch and continue
            # the checkout from there.
            self._current_branch = curr_branch_data.points_to_branch
            self._current_version = \
                curr_branch_data.points_to_collection_version

            self._meta_collection.set_metadata(
                current_version=self._current_version,
                current_branch=self._current_branch,
                detached=False,
                changed=False
            )

        # Save the proper target branch and version number. If the
        # destination branch is empty we need to check out to its 'base' to
        # be able to retrieve the correct documents, but then we need to
        # switch between the base branch and the target branch.
        destination_branch = branch
        branch = self._branches_collection.get_branch(branch).points_to_branch
        destination_version = version if destination_branch == branch else -1

        if version == self._current_version and branch == self._current_branch:
            if destination_version == -1:
                # If the destination branch is empty, the target
                # collection is already in a valid state, so just update the
                # head pointer.
                self._meta_collection.set_metadata(
                    current_version=destination_version,
                    current_branch=destination_branch,
                    detached=False,
                    changed=False
                )
                self._current_version = destination_version
                self._current_branch = destination_branch

            # Already checked out here
            return True

        # Get the target documents
        documents = self._get_documents_modified_between_versions(
            current_version=(self._current_version, self._current_branch),
            target_version=(version, branch),
        )[0]

        # Stop the listener, so the rollbacks are not recorded as changes.
        self._listener.stop()

        def process_doc(item: Tuple[ObjectId, Dict[str, Any]]) -> None:
            doc_id, doc = item
            if len(doc) == 0:
                # The updated document is empty, so remove it
                self.delete_one(filter={'_id': doc_id})
            else:
                self.replace_one(
                    filter={'_id': doc_id},
                    replacement=doc,
                    upsert=True
                )

        documents = list(documents.items())
        with ThreadPoolExecutor(max_workers=cpu_count()) as executor:
            executor.map(process_doc, documents)

        # Restart listening to this collection
        self._listener.start()

        # Set the current version to be the checked out version.
        self._current_version = destination_version
        self._current_branch = destination_branch

        # If the current version is the latest version on this branch,
        # then the head is attached, otherwise it is detached
        branch_data = dest_branch_data
        attached = \
            branch_data.points_to_branch == destination_branch and \
            branch_data.points_to_collection_version == destination_version

        # Update the metadata
        self._meta_collection.set_metadata(
            current_version=self._current_version,
            current_branch=self._current_branch,
            detached=not attached,
            changed=False
        )

        # This could be optimised, but I think it is better to do it like this
        # because it separates the versioning features, which take longer to
        # execute (register and checkout) from the normal collection commands.
        # This way, there is no need to wait for the first insert if the
        # replica is not synchronised, you'll wait just when registering and
        # checking out
        self._replica_collection.create_snapshot()

        return True

    def _get_documents_modified_between_versions(
        self,
        current_version: Tuple[int, str],
        target_version: Tuple[int, str],
        current_source: Optional[Collection] = None,
    ) -> Tuple[Dict[Any, Dict[str, Any]], Dict[Any, Dict[str, Any]]]:
        """ Gets the modified documents since the target version was registered.

        :param current_version: The current version.
        :param target_version: The target version.
        :param current_source: The collection from which to pull the current
            documents. The sensible options are ``self`` or `replica`.
        :return: The documents of the target version and the documents of the
            current version that were modified between the two versions.
        """
        if self._log_collection.get_log_entry(*target_version) is None:
            raise InvalidCollectionVersion(*target_version)

        if current_version == target_version:
            return dict(), dict()

        path = self._log_collection.get_path_between_versions(
            current=current_version,
            target=target_version
        )

        # Compute the deltas
        per_document_deltas = self._deltas_collection.get_deltas(path=path)

        if len(per_document_deltas) == 0:
            raise InvalidCollectionState(
                "No deltas found between the currently checked out version "
                "and the target version, even though both versions are "
                "registered. You'd better have a backup, amigo!"
            )

        # Get the documents that have to be updated
        if current_source is None:
            current_source = self

        doc_ids = list(per_document_deltas.keys())
        current_documents = list(current_source.find({'_id': {'$in': doc_ids}}))

        # Get the updated documents
        documents, current_documents = self._deltas_collection.apply_deltas(
            per_document_deltas=per_document_deltas,
            documents=current_documents,
            return_current_documents=True
        )
        return documents, current_documents

    @_synchronize
    def stash(self, overwrite: bool = False) -> bool:
        """ Stashes the changes made to this collection.

        Only a single set of changes can be stashed at any time. Calling this
        method multiple times without restoring the stashed data first will
        either raise an exception or will overwrite the previously stored
        stash data if ``overwrite=True``.

        .. code-block:: python

            >>> collection: VersionedCollection # assume it exists in scope
            >>> collection.status()
            {'tracked': False}
            >>> collection.init()
            True
            >>> collection.stash(), collection.has_changes()
            (False, False)
            >>> collection.insert_one({'field': 'value'})
            ObjectId('54f112defba522406c9cc207')
            >>> collection.has_changes()
            True
            >>> collection.stash()
            True
            >>> collection.has_changes()
            False
            >>> collection.count_documents({})
            0
            >>> collection.insert_one({'field': 'new value'})
            ObjectId('54f112defba522406c9cc208')
            >>> try:
            ...     collection.stash()
            ... except InvalidOperation:
            ...     print("Stash blocked")
            ...
            Stash blocked
            >>> collection.stash(overwrite=True)
            True
            >>> collection.count_documents({})
            0
            >>> collection.stash_apply()
            True
            >>> collection.find_one({})
            {'_id': ObjectId('54f112defba522406c9cc208'), 'field': 'new value'}


        :raises `InvalidOperation`: When ``overwrite=False`` and there exists
            another stash.
        :param overwrite: Whether to overwrite the existing stash space.
        :return: ``True`` if the changes were stashed, ``False`` if the
            collection is not tracked or there is nothing to stash.
        """
        if not self._tracked or not self.has_changes():
            return False

        if self._stash_container.exists() and not overwrite:
            raise InvalidOperation(
                "Changes already stashed. Set `overwrite=True` if the "
                "previously stashed data should be ignored. "
            )

        self._stash_container.stash(self, self._modified_collection)
        self._meta_collection.set_metadata(has_stash=True)
        self.discard_changes()
        return True

    @_synchronize
    def stash_apply(self) -> bool:
        """ Applies the stashed changes over the currently checked out version.

        .. warning::
            Applying the stashed changes will overwrite the existing
            dococuments.

        .. code-block:: python

            >>> collection: VersionedCollection # assume it exists in scope
            >>> collection.has_changes()
            True
            >>> collection.stash()
            True
            >>> collection.checkout(0, 'main')
            True
            >>> collection.has_changes()
            False
            >>> collection.stash_apply()
            True
            >>> collection.has_changes()
            True

        :raises `InvalidOperation`: If the collection has changes.
        :return: ``True`` if the stash is successfully applied, ``False`` if
            the collection is not tracked or there is no stash to apply.
        """
        if not self._tracked or not self._stash_container.exists():
            return False

        if self.has_changes():
            raise InvalidOperation(
                "Cannot apply stashed data because the collection has changes."
                "Either register a new version or discard the current changes "
                "before applying the stash."
            )
        self._listener.stop()
        self._stash_container.stash_apply(self, self._modified_collection)
        self._listener.start()
        self._meta_collection.set_metadata(has_stash=False)
        return True

    @_synchronize
    def stash_discard(self) -> bool:
        """ Removes the stashed data from the stash area.

        After this method is called the stash will be empty and cannot be
        recovered.

        .. note::
            Stashing and discarding the stashed data is equivalent to calling
            :meth:`discard_changes()`.

        .. code-block:: python

            >>> collection: VersionedCollection # assume it exists in scope
            >>> collection.has_changes()
            True
            >>> collection.stash()
            True
            >>> collection.stash_discard()
            True
            >>> collection.has_changes()
            True

        :return: ``True`` if the stash is successfully discarded, ``False``
            if it does not exist or the collection is not tracked.
        """
        if not self._tracked or not self._stash_container.exists():
            return False
        self._stash_container.drop()
        self._meta_collection.set_metadata(has_stash=False)
        return True

    @_synchronize
    def discard_changes(self) -> bool:
        """ Discards the changes made to the collection.

        After discarding the changes, the collection will return to the state
        of the previous registered version.

        If the changes made to the collection should be temporarily and safely
        stored, consider calling :meth:`stash()`.

        :return: Whether the operation was successfully executed or not.
        """
        if not self._tracked:
            return False

        self._listener.stop()
        docs = self._modified_collection \
            .get_modified_document_ids_by_operation()

        restored = set()

        # Inserts
        if 'i' in docs:
            self.delete_many(filter={'_id': {"$in": docs['i']}})
            restored.update(set(docs['i']))

        # Deletes
        if 'd' in docs:
            _docs = list(
                self._replica_collection.find({'_id': {"$in": docs['d']}})
            )
            if len(_docs) > 0:
                self.insert_many(documents=_docs)
                restored.update(set(docs['d']))

        # Updates
        if 'u' in docs:
            docs['u'] = list(set(docs['u']).difference(restored))
            if len(docs['u']):
                # Upserts are also classified as update operations, so we need
                # to flag them and handle as inserts
                #  Does this actually happen? I think upserts are just
                #  inserts. We still need to do most of the work of finding
                #  the old and new documents, so there won't be too much to
                #  optimise. PRIORITY: low
                old = group_documents_by_id(
                    self._replica_collection.find({'_id': {"$in": docs['u']}})
                )
                new = group_documents_by_id(
                    self.find({'_id': {"$in": docs['u']}})
                )

                ids = set(old.keys()).union(set(new.keys()))
                upserts = []
                updates = []
                for _id in ids:
                    if _id in old and _id in new:
                        updates.append(_id)
                    elif _id in new and _id not in old:
                        upserts.append(_id)
                    else:
                        raise InvalidCollectionState(
                            f"Invalid collection state! Found document that is "
                            f"{'not' if _id not in old else ''} in replica and "
                            f"is {'not' if _id not in new else ''} in the "
                            f"untracked working collection"
                        )
                if len(upserts):
                    self.delete_many(filter={'_id': {"$in": upserts}})

                if len(updates):
                    self.delete_many(filter={'_id': {"$in": updates}})
                    self.insert_many(documents=[old[k] for k in updates])

        self._clear_changes()
        self._modified_collection.drop()
        self._listener.start()
        return True

    @overload
    def diff(
        self,
        version: Optional[int] = None,
        branch: Optional[str] = None,
        deep: Literal[False] = False,
        direction: Literal['to', 'from', 'bidirectional'] = 'from',
    ) -> Optional[Dict[Any, str]]:
        ...

    @overload
    def diff(
        self,
        version: Optional[int] = None,
        branch: Optional[str] = None,
        deep: Literal[True] = True,
        direction: Literal['to', 'from', 'bidirectional'] = 'from',
    ) -> Optional[Dict[Any, DeepDiff]]:
        ...

    @overload
    def diff(
        self,
        version: Optional[int] = None,
        branch: Optional[str] = None,
        deep: Literal[False] = False,
        direction: Literal['to', 'from', 'bidirectional'] = 'bidirectional',
    ) -> Optional[Dict[Literal['to', 'from'], Dict[Any, str]]]:
        ...

    @overload
    def diff(
        self,
        version: Optional[int] = None,
        branch: Optional[str] = None,
        deep: Literal[True] = True,
        direction: Literal['to', 'from', 'bidirectional'] = 'bidirectional',
    ) -> Optional[Dict[Literal['to', 'from'], Dict[Any, DeepDiff]]]:
        ...

    def diff(
        self,
        version: Optional[int] = None,
        branch: Optional[str] = None,
        deep: Literal[True, False] = False,
        direction: Literal['to', 'from', 'bidirectional'] = 'from',
    ) -> Union[
            Optional[Dict[Any, str]],
            Optional[Dict[Any, DeepDiff]],
            Optional[Dict[Literal['to', 'from'], Dict[Any, str]]],
            Optional[Dict[Literal['to', 'from'], Dict[Any, DeepDiff]]],
    ]:
        """ Returns the diffs between the current and the given version.

        If no version id or branch are given, this method computes the diffs
        between the current working version and the last version registered.

        If the `version` parameter is omitted and the `branch` parameter is
        given, then the target version is considered to be the version the
        `branch`'s branch pointer is pointing to.

        If the `branch` parameter is omitted and the `version` parameter is
        given, then the target version is considered to be version with id
        `version` from the current branch.

        .. note::
            Passing ``deep=True`` can consume a large volume of memory for
            large collection diffs since each diff stores both versions of
            a document.

        Examples:

        .. code-block:: python

            >>> collection: VersionedCollection  # assume this exists in scope
            >>> collection.diff()
            <diffs between the current state and latest version registered>
            >>> collection.diff(0, 'main')
            <diffs between current state and version 0 on branch 'main'>
            >>> collection.diff(2)
            <diffs between current state and version 0 on the current branch>
            >>> collection.diff(branch='branch')
            <diffs between current state and the latest version from 'branch'>
            >>> print(collection.diff(structural=True))
            <pretty structural diff>
            >>> collection.diff(0, 'main', direction='to')
            <diff from the current version to version 0 on branch main>


        :raises `~versioned_collection.errors.InvalidCollectionVersion`: If
            the given version does not exist.

        :param version: The version to compare the current version with.
        :param branch: The branch on which the version to compare the current
            version with is registered on.
        :param deep: Whether to compute the class:`DeepDiff` object containing
            the deep differences between the objects or a structural diff (
            printable, similar to git diffs). Defaults to ``False``.
            the deep differences between the objects.
        :param direction: The direction in which to compute the diff. When
            equal to ``'to'``, the current version is considered the reference
            version and the diffs represent the changes made to current
            collection state to reach the target collection state. When equal to
            ``'from'``, the given version is considered the reference version.
            When equal to ``'bidirectional'``, both forward and backward
            diffs are computed and returned. Defaults to ``'from'``.
        :return: The structural or deep diffs of the modified documents,
            grouped by their ids, in case of unidirectional diffs. In the
            case of bidirectional diffs, it returns the diffs grouped by the
            modified document id and grouped by the direction. If the
            collection is not tracked, returns
            ``None``.
        """
        if not self._tracked:
            return None

        if version is None and branch is None:
            if not self.has_changes():
                return dict()

        mod_ids = self._modified_collection.find_modified_documents_ids()
        mod_ids = [doc['_id'] for doc in mod_ids]
        if version is None and branch is None:
            # The other documents are in the replica collection
            current = group_documents_by_id(
                self.find({'_id': {"$in": mod_ids}})
            )
            other = group_documents_by_id(
                self._replica_collection.find({'_id': {"$in": mod_ids}})
            )
        else:
            # Get the other documents from the target version
            if version is None:
                br_data = self._branches_collection.get_branch(branch)
                version = br_data.points_to_collection_version
                branch = br_data.points_to_branch
            branch = self._current_branch if branch is None else branch

            if (
                version == self.version
                and branch == self.branch
                and not self.has_changes()
            ):
                return dict()

            other, current = self._get_documents_modified_between_versions(
                current_version=(self._current_version, self._current_branch),
                target_version=(version, branch),
                current_source=self._replica_collection,
            )

            if self.has_changes():
                # If there are changes and the target version is not necessarily
                # the latest version registered, then also grab the unregistered
                # changes and update the current documents.
                current_modified = group_documents_by_id(
                    self.find({'_id': {"$in": mod_ids}})
                )
                current.update(current_modified)
                if version == self.version and branch == self.branch:
                    other_modified = group_documents_by_id(
                        self._replica_collection.find({'_id': {"$in": mod_ids}})
                    )
                    other.update(other_modified)

        doc_ids = set(other.keys()).union(set(current.keys()))

        def compute_deep_diff(doc1, doc2, doc_id):
            return doc_id, DeepDiff(doc1, doc2)

        def compute_structural_diff(doc1, doc2, doc_id):
            doc1 = stringify_document(doc1)
            doc2 = stringify_document(doc2)
            doc_id = stringify_object_id(doc_id)

            _diff = DeepDiff(doc1, doc2)['values_changed']['root']['diff']
            return doc_id, _diff

        diff_fn = compute_deep_diff if deep else compute_structural_diff

        diffs_to = dict()
        diffs_from = dict()
        for _id in doc_ids:
            if _id not in other and _id not in current:
                # These are documents inserted and deleted between the two
                # versions, so we don't care about them
                continue
            other_doc = other.get(_id, {})
            current_doc = current.get(_id, {})

            if direction == 'from' or direction == 'bidirectional':
                __id, diff = diff_fn(other_doc, current_doc, _id)
                diffs_from[__id] = diff

            if direction == 'to' or direction == 'bidirectional':
                __id, diff = diff_fn(current_doc, other_doc, _id)
                diffs_to[__id] = diff

        if direction == 'bidirectional':
            diffs = {'from': diffs_from, 'to': diffs_to}
        elif direction == 'to':
            diffs = diffs_to
        else:
            diffs = diffs_from

        return diffs

    def get_log(
        self, branch: Optional[str] = None
    ) -> List[LogsCollection.SCHEMA]:
        """ Returns the log of this collection for the given branch.

        The returned history is in descending order (the latest entry first).
        The first entry will correspond to the previous registered version on
        the given branch, with respect to the current version.

        :raises `~versioned_collection.error.BranchNotFound`: If no branch
            with the given name exists.

        :param branch: The name of the branch for which to get the history.
            If it is not provided, this defaults to the current branch
        :return: The history for the specified branch.
        """
        branch = self._current_branch if branch is None else branch
        branch_data = self._branches_collection.get_branch(branch)
        branch = branch_data.points_to_branch
        version = branch_data.points_to_collection_version
        return self._log_collection.get_log(
            version=version, branch=branch, return_ids=True
        )

    def _set_changed(self, changed: bool = True) -> None:
        """ Reflects information about the collection's status to metadata."""
        m = self._meta_collection.metadata
        if m.changed == changed:
            return
        self._meta_collection.set_metadata(changed=changed)

    def _has_changed(self) -> None:
        """ Sets this collection's status as changed. """
        self._set_changed(True)

    def _clear_changes(self) -> None:
        """ Sets this collection's status as unchanged.

        Use this when a new version of the collection is registered, so there
        are no non-tracked changes, or when changes get discarded.
        """
        self._set_changed(False)

    def has_changes(self) -> bool:
        """ Returns whether this collection has unregistered changes. """
        return self._meta_collection.metadata.changed \
            if self._tracked else False

    def is_tracked(self) -> bool:
        """ Returns whether this collection is initialised for versioning. """
        return self._tracked

    def has_conflicts(self) -> bool:
        """ Returns whether this collection has unresolved conflicts. """
        return self._meta_collection.metadata.has_conflicts \
            if self._tracked else False

    def has_stash(self) -> bool:
        """ Returns whether this collection has stashed changes. """
        return self._meta_collection.metadata.has_stash \
            if self._tracked else False

    def is_detached(self) -> bool:
        """ Returns whether this collection is in the detached head mode. """
        return self._meta_collection.metadata.detached \
            if self._tracked else False

    def status(self) -> Dict[str, Union[str, bool, int]]:
        """ Returns the status of this collection. """
        if self._tracked:
            return self._meta_collection.metadata.__dict__
        else:
            return {'tracked': False}

    def branches(self) -> Set[str]:
        """ Returns the names of the existing branches.

        :return: A set containing the name of all branches registered on
            the collection. If the collection is not initialised for
            tracking, an empty set is returned.
        """
        return self._branches_collection.get_branch_names() if self._tracked \
            else []

    @_synchronize
    def push(self,
             remote_collection: VersionedCollection,
             branch: Optional[str] = None,
             do_checkout: bool = True
             ) -> bool:
        """ Pushes a branch of this collection to a remote collection.

        If the remote collection is checked out on branch `branch`,
        by default, upon pushing, the remote's collection state is updated,
        and it is checked out to the latest version pushed. To change this
        behaviour set ``do_checkout=False``.

        .. warning::
            This does not perform a remote collection validation to check if
            the local and the remote collections are of the same 'type'. If
            the local and remote collections have the same name, a branch
            that is not present in the remote collection can be pushed.

        This method initialises the remote collection in case it is not, e.g.,
        the collection is pushed for the first time. This is the preferred
        method of doing it, since manually initialising the remote calling
        :meth:`init()` will cause in discrepancies between the two
        collections and pushes will be denied.

        .. note::
            This method locks both the remote and the local collections,
            so none of the collections can perform other versioning operations
            until the synchronisation is finished.

        :raises `~versioned_collection.errors.InvalidOperation`: If trying to
            push from a collection to itself, if trying to push when the
            collection's head is detached and no `branch` parameter is
            provided, or when the remote branch has changes that are not
            present on the local branch.

        :raises `~versioned_collection.errors.InvalidOperation`: If the remote
            collection is not initialised, the local collection has more
            than one version registered, the local collection has data into
            the stash area and the local collection has unregistered changes.

        :raises `ValueError`: When the remote collection has a  different name
            and type compared to this collection.

        :param remote_collection: The versioned collection on which the branch
            will be pushed. This can be a collection from the same database
            as the current collection, on other database, or even on a
            different host server.
        :param branch: The branch to push to the remote collection. If it is
            omitted, the currently checked out branch is considered for being
            pushed. If the collection's head is detached this must be provided.
        :param do_checkout: Whether to update the remote collection to the
            latest pushed version if `remote_collection` is checked out at
            the tip of branch `branch`.
        :return: ``False`` if this collection is not tracked, otherwise ``True``
            if the operation completed successfully, or the remote branch is
            up-to-date.
        """
        remote_collection._lock()
        try:
            ret = self._push(self, remote_collection, branch, do_checkout)
        except Exception as e:
            raise e
        finally:
            remote_collection._unlock()
        return ret

    @staticmethod
    def _push(src: VersionedCollection,
              remote_collection: VersionedCollection,
              branch: Optional[str],
              do_checkout: bool = True
              ) -> bool:
        """ Pushes a branch from `src` to `remote_collection`

        This is a helper method that performs the ``push`` operation between
        two versions collections. See :meth:`push()` for a broader description.

        :param src: The source versioned collection.
        :param remote_collection: The destination versioned collection.
        :param branch: The branch to push
        :param do_checkout: Whether to check out the head of the branch
            when `branch` is the current branch.
        :return: ``False`` if this collection is not tracked, otherwise ``True``
            if the operation completed successfully, or the remote branch is
            up-to-date.
        """
        if not src.is_tracked():
            return False
        if remote_collection is src:
            raise InvalidOperation(
                "The source and destination collections are the same."
            )

        if remote_collection.name != src.name:
            # Just a quick check to prevent obvious errors.
            # This could be made more robust by comparing the roots of the
            # log trees (or split check and move below)
            raise ValueError(
                f"Cannot transfer data between collections with different "
                f"names: source ('{src.name}'), destination "
                f"('{remote_collection.name}')."
            )

        if not remote_collection.is_tracked():
            # The state of the collection before initialisation is not stored
            # using any tracking collections, so to transfer it to the remote
            # collection we need to check out at (0, 'main') on `src` and
            # manually transfer it.
            try:
                src.stash()
            except InvalidOperation:
                raise InvalidOperation(
                    "Cannot transfer data when the destination collection is "
                    "untracked, the source's collection stash area is "
                    "occupied and the source collection has changes. Either "
                    "clear the stash or discard the changes before."
                )
            src_version = src.version, src.branch

            src.checkout(0, 'main')
            remote_collection.drop()

            limit = 10000  # maybe more, maybe less
            query, last_key_fn = generate_pagination_query({})
            data = list(src.find(query).limit(limit))
            last_key = last_key_fn(data)

            while len(data) > 0:
                remote_collection.insert_many(documents=data)
                query, last_key_fn = generate_pagination_query(
                    query={}, last_key=last_key
                )
                data = list(src.find(query).limit(limit))
                last_key = last_key_fn(data)

            remote_collection.init()
            # Properly synchronise the logs
            log = src.get_log(branch)[-1]
            remote_collection._log_collection.reset()
            remote_collection._log_collection.build(
                message=log.message, timestamp=log.timestamp, with_id=log.id
            )

            if src_version[0] == -1:
                src.checkout(branch=src_version[1])
            else:
                src.checkout(*src_version)
            src.stash_apply()

        if branch is None and src.is_detached():
            raise InvalidOperation(
                "Operation not allowed in detached mode. Either provide a "
                "valid branch name or checkout to a branch first."
            )

        branch = src.branch if branch is None else branch

        if branch not in src.branches():
            raise InvalidOperation(
                f"Branch {branch} does not exist in the source collection"
            )

        local_log = None

        # Lazily initialise it since this involves querying the database
        local_branch_data = None

        if branch in remote_collection.branches():
            # Check if the current branch of this collection is up-to-date
            # with the remote branch
            remote_log = remote_collection.get_log(branch)
            local_log = src.get_log(branch)

            if (
                len(remote_log) == len(local_log)
                and remote_log[0] == local_log[0]
            ):
                # Everything is up-to-date
                return True

            if len(remote_log) > len(local_log):
                raise InvalidOperation(
                    "Push rejected! The tip of your current branch is behind "
                    "the remote. \n"
                    "You can either move the last registered version(s) to "
                    "another branch and push it, or pull the latest changes "
                    "from the remote and integrate yours."
                )
        else:
            # Create the branch on the remote
            try:
                # The local branch is not empty; get the information from the
                # log tree.
                parent_version, parent_branch = \
                    src._log_collection.get_parent_version(version=(0, branch))
            except (BranchNotFound, InvalidCollectionVersion):
                # The local branch is empty, i.e., no versions are registered
                # on this branch, so it does not appear in the log tree. Get
                # the branch info from the branches collection
                local_branch_data = src._branches_collection.get_branch(branch)
                parent_version = local_branch_data.points_to_collection_version
                parent_branch = local_branch_data.points_to_branch

            # Check if the branching point exists on the remote collection
            if (
                not remote_collection._log_collection.contains_version(
                    parent_version, parent_branch
                )
            ):
                raise InvalidOperation(
                    f"Cannot push branch {branch} while its parent branch "
                    f"{parent_branch} is not pushed up to the branching "
                    f"point, at version ({parent_version}, {parent_branch})"
                )

            remote_collection._branches_collection.create_branch(
                branch=branch,
                pointing_to_collection_version=parent_version,
                pointing_to_branch=parent_branch
            )

        if local_branch_data is None:
            local_branch_data = src._branches_collection.get_branch(branch)

        local_version = local_branch_data.points_to_collection_version
        local_branch = local_branch_data.points_to_branch
        remote_branch_data = remote_collection._branches_collection.get_branch(
            branch
        )
        remote_version = remote_branch_data.points_to_collection_version
        remote_branch = remote_branch_data.points_to_branch

        # Check for divergence
        remote_branch_tip_log = remote_collection._log_collection.get_log_entry(
            remote_version, remote_branch
        )
        remote_version_on_local = src._log_collection.get_log_entry(
            remote_version, remote_branch
        )

        if not remote_branch_tip_log.weakly_equals(remote_version_on_local):
            raise InvalidOperation(
                "Operation rejected! The remote and local branches have "
                f"diverged at (or before) version ({remote_version}, "
                f"{remote_branch})"
            )

        if local_version == remote_version and local_branch == remote_branch:
            # An empty branch was pushed
            return True

        if branch != remote_branch_data.points_to_branch:
            start = remote_version, remote_branch_data.points_to_branch
        else:
            start = remote_version, branch

        path = src._log_collection.get_path_between_versions(
            current=start, target=(local_version, branch)
        )
        if local_log is None:
            local_log = src.get_log(branch)

        # Add the log entries to the remote collection
        logs = local_log[:len(path) - 1]
        prev_version = list(path.keys()).pop(0)
        for log in reversed(logs):
            prev_version = remote_collection._log_collection.add_log_entry(
                previous_version=prev_version[0],
                previous_branch=prev_version[1],
                current_branch=log.branch,
                message=log.message,
                timestamp=log.timestamp,
                with_id=log.id
            )

        # Get deltas between the latest version on remote and latest version
        # on local.
        deltas_per_doc = src._deltas_collection.get_delta_documents_in_path(
            path=path, sorting_order=pymongo.ASCENDING
        )
        for doc in deltas_per_doc:
            remote_collection._deltas_collection.insert_delta_docs(
                doc['deltas']
            )

        # Update the remote branch pointer
        remote_collection._branches_collection.update_branch(
            branch=branch,
            pointing_to_collection_version=local_version,
            pointing_to_branch=branch
        )

        if remote_collection.branch == branch and do_checkout:
            remote_collection.checkout(branch=branch)

        return True

    @_synchronize
    def pull(self,
             remote_collection: VersionedCollection,
             branch: Optional[str] = None
             ) -> bool:
        """ Pulls a branch from a remote collection to this collection.

        Pulling allows downloading a single branch at a time and does not
        pull the entire version tree of the remote collection. If this is
        desired, then it can be achieved by iteratively pulling the all
        branches of the remote collection.

        .. warning::
            If the local and remote versions of `branch` have diverged and
            more branches were created locally on branch `branch` after the
            divergence point, all that data will be lost after `branch` is
            pulled.

        .. note::
            If the local and remote collection's versions of `branch` have
            diverged and in both versions a document containing the same data
            has been added to the local and remote collections, resulting in
            two documents with different ids, but the same data, then pulling
            the remote branch will result in having duplicated documents,
            since the auto-merge will be successful since the two documents
            have different ids.

        If `branch` is the current branch and the head is attached, i.e.,
        the collection is checked out at the last version on `branch`,
        then after the branch is pulled the collection will be checked out at
        the last version of the newly pulled branch. If other branch,
        different from the current branch, is pulled or the collection is in
        detached mode, then the after the branch is pulled, the collection
        will be at the same version as it was before calling this method.

        .. note::
            If the auto-merging of the local and remote versions of `branch`
            has failed due to merge conflicts and the local collection had
            unregistered changes, a warning will be displayed, notifying
            the version at which the collection was checked out when
            :meth:`pull` was called and that the modified data from that
            version is saved in the stash area. After solving the conflicts,
            the user should manually check out the that version and apply the
            stash, or discard it.

        .. note::
            This method locks both the remote and the local collections,
            so none of the collections can perform other versioning operations
            until the synchronisation is finished.

        :raises `~versioned_collection.errors.InvalidOperation`: If trying to
            pull from the same collection into itself.
        :raises `ValueError`: If the name of the `remote_collection` is
            different from the name of this collection.
        :raises `~versioned_collection.errors.InvalidOperation`: If the
            collection is in detached mode and `branch` is not given.
        :raises `~versioned_collection.errors.InvalidOperation`: If the
            collection is checked out to the head of local `branch`, but the
            collection has changes.
        :raises `~versioned_collection.errors.InvalidOperation`: If `branch`
            is not a branch of the `remote_collection`.
        :raises `~versioned_collection.errors.InvalidCollectionState`: If this
            collection and the `remote_collection` have diverging initial
            versions, i.e., they were initialised independently and not
            properly synchronised using :meth:`pull` or :meth:`push`.
        :raises `~versioned_collection.errors.InvalidOperation`: If the local
            and remote versions of `branch` have diverged, the local
            collection has data in the stash area and the local collection
            has unregistered changes. Automatic stashing is possible,
            but since there is already data in the stashing area, that data
            will be lost, so an error is raised to manually correct it.
        :raises `~versioned_collection.errors.AutoMergeFailedError`: If the
            auto-merging the local and remote versions of `branch` resulted
            in merge conflicts.

        :param remote_collection: The remote :class:`VersionedCollection`
            from which to download a branch.
        :param branch: The name of the branch of the remote collection to pull.
            If omitted, it defaults to the current branch of this collection.
        :return: ``False`` if the remote collection is not initialised,
            ``True`` if everything is up-to-date or the ``pull`` operation
            has finished successfully.
        """
        remote_collection._lock()
        try:
            ret = self._pull(remote_collection, branch)
        except Exception as e:
            raise e
        finally:
            remote_collection._unlock()
        return ret

    def _pull(self,
              remote_collection: VersionedCollection,
              branch: Optional[str] = None
              ) -> bool:
        if not remote_collection.is_tracked():
            return False

        if remote_collection is self:
            raise InvalidOperation(
                "The source and destination collections are the same."
            )

        if remote_collection.name != self.name:
            # Just a quick check to prevent obvious errors.
            # This could be made more robust by comparing the roots of the
            # log trees (or split check and move below)
            raise ValueError(
                f"Cannot transfer data between collections with different "
                f"names: source ('{remote_collection.name}'), destination "
                f"('{self.name}')."
            )

        if branch is None and self.is_detached() and self._tracked:
            raise InvalidOperation(
                "Operation not allowed in detached mode. Either provide a "
                "valid branch name or checkout to a branch first."
            )

        branch = branch if branch is not None else self.branch

        if (
            self.has_changes()
            and branch == self.branch
            and not self.is_detached()
        ):
            # Raise error here since automatic stashing could result in data
            # loss since stashing overwrites the documents and does not merge
            # them.
            # Note: This could be implemented in future if needed.
            raise InvalidOperation(
                "Collection has changes. Either stash the changes or register "
                "a new version.\nNOTE:: Stashing and applying the stash will "
                "overwrite the documents in the collection."
            )

        if not self._tracked:
            branch = 'main'

        if branch not in remote_collection.branches():
            raise InvalidOperation(
                f"Branch {branch} does not exist in the remote collection."
            )

        diverging_version, separation_point = None, None
        if branch in self.branches():
            # Compare the logs and decide the nature of the differences.
            # Let the `separation point` be the last versions at which the
            # local and remote logs agree. There can be 3 types of separation
            # points:
            #   I.   Divergence point: both local and remote have changes
            #   II.  Local is behind:  we can pull from remote
            #   III. Remote is behind: nothing to pull
            local_log = self.get_log(branch)
            remote_log = remote_collection.get_log(branch)

            for local, remote in zip(reversed(local_log), reversed(remote_log)):
                if not local.weakly_equals(remote):
                    diverging_version = remote.version, remote.branch
                    break
                separation_point = remote.version, remote.branch
            else:
                if len(local_log) >= len(remote_log):
                    # Nothing to pull
                    return True

            if separation_point is None:
                raise InvalidCollectionState(
                    "The local and remote log trees have diverging roots."
                )

        new_branch_name = None
        stashed_changes = False
        if diverging_version is not None:
            # If the local and remote branches have diverged
            if self.has_changes():
                # Catch this before actually modifying the collection.
                # Otherwise, an `InvalidOperation` exception will be raised in
                # `checkout`.
                try:
                    stashed_changes = self.stash()
                except InvalidOperation:
                    raise InvalidOperation(
                        "Proceeding in pulling the data will result in the "
                        "lost of the current unregistered changes. The local "
                        "and remote branches have diverged and auto-merging "
                        "requires checking out the head of the newly pulled "
                        "branch, but changing versions is not possible with "
                        "unregistered changes on the HEAD.\n"
                        "If the stashed data is no longer relevant, consider "
                        "calling `stash_discard()` to clear it."
                    )

            new_branch_name = self._rebranch(*diverging_version)

        # Pull from remote
        do_checkout = False
        if (
            diverging_version is None
            and
            (
                branch == self.branch and not self.is_detached()
                or not self._tracked
            )
        ):
            do_checkout = True

        self._push(remote_collection, self, branch, do_checkout=do_checkout)

        if diverging_version is not None:
            # Rebase and try to automatically solve conflicts
            destination_version = self.get_log(branch)[0].version
            source_version = self.get_log(new_branch_name)[0].version
            current_version = self.version, self.branch
            try:
                self._merge(
                    destination=(destination_version, branch),
                    source=(source_version, new_branch_name),
                    separation_point=separation_point,
                )
            except AutoMergeFailedError as e:
                if stashed_changes:
                    warnings.warn(
                        f"\n\nThe unregistered changes from version "
                        f"{current_version} have been automatically stashed."
                        f"Remember to apply the stash back after resolving "
                        f"the conflicts.\n\n"
                    )
                raise e

            if stashed_changes:
                if current_version[0] == -1:
                    self.checkout(branch=current_version[1])
                else:
                    self.checkout(*current_version)
                self.stash_apply()

        return True

    def _merge(self,
               destination: Tuple[int, str],
               source: Tuple[int, str],
               separation_point: Tuple[int, str]
               ) -> None:
        """ Merges the `source` branch into the `destination` branch.

        .. warning::
            Merging a branch that has child branches will result in the lost
            of the data associated to the child branches.

        Merging will 'compact' all versions registered on the `source` branch,
        into a single version on the `destination` branch and will remove the
        `source` branch including all its sub-branches.

        .. note::
            The merge operation preserves the tree structure of the version
            tree, and does not create versions that have multiple ancestors.

        :param destination: The last version on the branch into which the merge
            happens.
        :param source: The last version of the branch to be merged.
        :param separation_point: The version of the common ancestor of the two
            branches.
        """

        self.checkout(*separation_point)
        dest_docs, original = self._get_documents_modified_between_versions(
            current_version=separation_point, target_version=destination
        )
        source_docs = self._get_documents_modified_between_versions(
            current_version=separation_point, target_version=source
        )[0]
        self.checkout(*destination)

        all_ids = set(original.keys()) | set(dest_docs.keys()) | \
                  set(source_docs.keys())

        merged, updated, conflicting = [], [], []

        for _id in all_ids:
            if (
                _id not in original
                and _id not in dest_docs
                and _id not in source_docs
            ):
                # Added and deleted in both
                continue
            if _id not in original and _id not in source_docs:
                # Added only on the destination branch
                continue
            o = original.get(_id, {})
            s = source_docs.get(_id, {})

            diff_o_s = DeepDiff(o, s, view='tree')
            if len(diff_o_s) == 0:
                # Not modified on the source branch
                continue

            d = dest_docs.get(_id, {})
            diff_o_d = DeepDiff(o, d, view='tree')
            if len(diff_o_d) == 0:
                # The document was modified only on the source branch
                if len(s) == 0:
                    self.delete_one(filter={'_id': _id})
                else:
                    self.replace_one(
                        filter={'_id': _id}, replacement=s, upsert=True
                    )
            else:
                doc, conflict = self._auto_merge(d, s, diff_o_d, diff_o_s)
                merged.append(doc)
                updated.append(_id)
                if len(conflict):
                    # TODO: batch-update the db instead of keeping
                    #  everything in memory till the end
                    conflicting.append({
                        'destination': d,
                        'merged': doc,
                        'source': s,
                        'destination_branch': destination[1],
                        'source_branch': source[1]
                    })

        if len(merged):
            self.delete_many(filter={'_id': {"$in": updated}})
            self.insert_many(documents=merged)

        if len(conflicting):
            self._conflicts_collection.insert_many(conflicting)
            self._meta_collection.set_metadata(has_conflicts=True)
            raise AutoMergeFailedError(destination[1])

        # Get the log messages of the merged versions
        logs = self._log_collection.get_log(*source[::-1])
        messages = []
        for entry in logs:
            if entry.branch != source[1]:
                break
            messages.append(entry.message)

        # Clear the re-branched branch
        self.delete_version_subtree(0, source[1])

        # Register the new merged version
        self.register(message='[Auto-Merged] \n ' + '\n'.join(messages))

    @staticmethod
    def _auto_merge(
        destination: Dict[str, Any],
        source: Dict[str, Any],
        diff_destination: DeepDiff,
        diff_source: DeepDiff
    ) -> Tuple[Dict[str, Any], List[str]]:
        """ Merges the `source` and `destination` dictionaries.

        :param destination: The dictionary to update.
        :param source: The dictionary that contains the updates.
        :param diff_destination: The diff between the ancestor and the
            `destination` dictionaries.
        :param diff_source: The diff between the ancestor and the `source`
            dictionaries.
        :return: A dictionary containing the auto-merged fields and a list of
            conflicting paths.
        """
        d_paths = {diff.path(output_format='list')[0]
                   for v in diff_destination.values() for diff in v}
        s_paths = {diff.path(output_format='list')[0]
                   for v in diff_source.values() for diff in v}

        auto_merged = deepcopy(destination)
        conflict_paths = []
        both_modified = s_paths & d_paths
        for k in both_modified:
            if DeepDiff(destination[k], source[k]) == {}:
                continue
            conflict_paths.append(k)

        for k in s_paths - both_modified:
            if k in source:
                auto_merged[k] = source[k]
            else:
                auto_merged.pop(k)

        return auto_merged, conflict_paths

    def _rebranch(self, version: int, branch: str) -> str:
        """ Moves the subtree of the version tree rooted at the given version
        to another branch.

        Re-branching involves updating the log tree references, the branch
        references inside the deltas contained in the subtree that needs to
        be re-branched, updating the branch pointer and updating any empty
        branches that point at `branch`.

        :param version: The version from which to re-branch.
        :param branch: The branch name of the branch to modify.
        :return: The name of the branch on which the subtree of the version
            tree was moved to.
        """
        if version == 0 and branch == 'main':
            raise InvalidOperation(
                "Cannot rebranch the root of the version tree!"
            )

        last_version_on_branch = self.get_log(branch)[0].version
        num_versions_to_rebranch = last_version_on_branch - version + 1

        new_name = f"__rebranched_{branch}"
        next_branch_id = len(list(filter(
            lambda b: b.startswith(new_name), self.branches()
        )))
        new_name = f"{new_name}_{next_branch_id}"

        # Update the log
        self._log_collection.rebranch(
            version=(version, branch), new_branch=new_name
        )
        # Update the deltas
        self._deltas_collection.rebranch(
            start_version=(version, branch),
            new_branch=new_name,
            num_versions=num_versions_to_rebranch
        )
        # Change the branch pointer
        self._branches_collection.update_branch(
            branch=branch,
            pointing_to_collection_version=version - 1,
            pointing_to_branch=branch,
        )
        # Add the new branch
        self._branches_collection.create_branch(
            branch=new_name,
            pointing_to_collection_version=num_versions_to_rebranch - 1,
            pointing_to_branch=new_name
        )
        # Update any other branches originally pointing to `branch`
        for br in self._branches_collection.get_empty_child_branches(branch):
            self._branches_collection.update_branch(
                branch=br.name,
                pointing_to_collection_version=(
                    br.points_to_collection_version - version
                ),
                pointing_to_branch=new_name
            )
        # Update the HEAD pointer
        if self.branch == branch:
            self._current_branch = new_name
            self._current_version -= version
            self._meta_collection.set_metadata(
                current_version=self._current_version,
                current_branch=self._current_branch,
            )

        return new_name

    @_synchronize
    def resolve_conflicts(self, discard_local_changes: bool = False) -> bool:
        """ Call this method to interactively resolve the merge conflicts.

        A GUI conflict resolver will pop up for each conflicting document. You
        will view three columns: the one in the left of the screen represents
        the `destination` or the `remote` version of the document, the one in
        the middle represents the `auto-merged` document with conflicts,
        and reflects the current state of the document in this collection,
        and finally, the rightmost column shows the `source` or `local` version
        of the document.

        The GUI of the merge tool can be used to automatically edit and
        integrate the changes, but it also serves as a full text editor,
        so in that the suggested conflict resolution does not satisfy the
        requirements, the document can be manually edited. Note that the
        `remote` and `local` files cannot and should not be edited, because
        the changes are ignored

        To move to the next conflict in another document make sure you save the
        document (by pressing on one of the save icons or pressing ``Ctrl+s``)
        and then close the merge tool.

        .. seealso:: `Meld merge tool <https://meldmerge.org/>`_.

        :param discard_local_changes: Whether to ignore the local changes of
            the conflicting documents.
        :return: ``True`` if the operation ended successfully, ``False``
            otherwise, or if there were no conflicts to resolve.
        """

        if not self.has_conflicts():
            return False

        if discard_local_changes:
            conflict_doc = self._conflicts_collection.find_one()
            source_branch_name = conflict_doc['source_branch']
            self.delete_version_subtree(version=0, branch=source_branch_name)
            self._conflicts_collection.drop()
            self._meta_collection.set_metadata(has_conflicts=False)

            # Ignore the partially auto-merged documents and the documents
            # only from the source branch
            self.discard_changes()
            return True

        _dir = '/tmp/vc/conflicts_resolution/'
        if os.path.exists(_dir):
            rmtree(_dir)
        os.makedirs(_dir)
        files = ['DESTINATION', 'MERGED', 'SOURCE']
        files = {n.lower(): os.path.join(_dir, n) for n in files}

        source_branch = None

        for conflict in self._conflicts_collection.find({}):
            if source_branch is None:
                source_branch = conflict['source_branch']

            for doc_type, file_name in files.items():
                with open(file_name, 'w+') as f:
                    f.write(stringify_document(conflict[doc_type]))

            subprocess.run([
                'meld', files['destination'], files['merged'], files['source'],
                '--auto-merge', '-L REMOTE', '-L MERGED', '-L LOCAL'
            ], check=True)

            with open(files['merged'], 'r') as f:
                merged_doc = parse_json_document(f.read())

            self.find_one_and_replace(
                filter={'_id': merged_doc['_id']},
                replacement=merged_doc
            )
            print(f"[vc] Resolved conflict for document {merged_doc['_id']}")
            self._conflicts_collection.delete_one({'_id': conflict['_id']})

        self.delete_version_subtree(version=0, branch=source_branch)
        self._conflicts_collection.drop()
        self._meta_collection.set_metadata(has_conflicts=False)
        return True

    @_synchronize
    def delete_version_subtree(
        self,
        version: int,
        branch: Optional[str] = None
    ) -> bool:
        """ Deletes a version and all versions registered after it.

        .. warning::
            This deletes the subtree of the version tree rooted in version
            ``(version, branch)``, and does not just remove a version in the
            middle of a branch.

        .. warning::
            Deleting the root of the version tree is equivalent to dropping the
            collection. After this step, the collection is uninitialised for
            tracking, so :meth:`init()` has to be called again on it.

        If the collection is checked out on the branch and a version that needs
        to be deleted, all the changes made to the collection are discarded,
        as well. Also, in the same case, the state of the collection will be
        rolled back to the parent's version state of the given version.

        :raises `~versioned_collection.errors.InvalidCollectionVersion`:
            If the given version does not exist.

        :param version: The version id of the version that will be removed.
        :param branch: The branch on which the versions to be deleted are
            located. If no branch name is given, `branch` is assumed to be
            the current branch.
        :return: ``True`` if the versions were successfully removed,
            ``False`` otherwise.
        """
        if not self._tracked:
            return False

        branch = self._current_branch if branch is None else branch

        version_id = (version, branch)
        previous = self._log_collection.get_parent_version(version_id)
        if previous is None:
            # The given version is the root of the version tree.
            self.drop()
            return True

        _parent_br = self._current_branch
        ancestor_and_curr_br = []
        process_empty_branch = self._current_version == -1
        while _parent_br is not None:
            ancestor_and_curr_br.append(_parent_br)
            if process_empty_branch:
                _br = self._branches_collection.get_branch(self._current_branch)
                _parent_br = _br.points_to_branch
                process_empty_branch = False
            else:
                _parent_br = self._log_collection.get_parent_branch(_parent_br)

        # If the current version is part of the subtree that is about to be
        # deleted, check out to the version before that subtree.
        if branch in ancestor_and_curr_br:
            self.discard_changes()
            self.checkout(*previous)

            self._meta_collection.set_metadata(detached=True)

        branches_to_delete = [
            b.name for b in self._branches_collection.get_empty_child_branches(
                branch, version
            )]

        leaves = self._log_collection.get_branch_tips_versions(version_id)
        for _, leaf_branch in leaves:
            if leaf_branch != branch:
                branches_to_delete.append(leaf_branch)

        if previous[1] != branch:
            # Delete the whole branch
            branches_to_delete.append(branch)
        else:
            # Update the branch pointer
            self._branches_collection.update_branch(
                branch=branch,
                pointing_to_collection_version=version - 1,
                pointing_to_branch=branch
            )

        # Delete the branch pointers
        self._branches_collection.delete_branches(branches_to_delete)

        self._log_collection.delete_subtree(version_id)

        self._deltas_collection.delete_subtrees(
            root=version_id,
            leaves=leaves
        )

        if self.is_detached():
            branch_data = self._branches_collection.get_branch(
                self._current_branch
            )
            if (
                self.version == branch_data.points_to_collection_version
                and self.branch == branch_data.points_to_branch
            ):
                # The subtree bellow the current version has been deletes so
                # the head is now attached to the new tip of the branch
                self._meta_collection.set_metadata(detached=False)

        return True

    """
    Methods that modify the state of this collection.
    """

    @_check_for_changes('find')
    def find_one_and_update(self, *args, **kwargs):
        return super().find_one_and_update(*args, **kwargs)

    @_check_for_changes('find')
    def find_one_and_replace(self, *args, **kwargs):
        return super().find_one_and_replace(*args, **kwargs)

    @_check_for_changes('find')
    def find_one_and_delete(self, *args, **kwargs):
        return super().find_one_and_delete(*args, **kwargs)

    @_check_for_changes('delete')
    def delete_many(self, *args, **kwargs):
        return super().delete_many(*args, **kwargs)

    @_check_for_changes('delete')
    def delete_one(self, *args, **kwargs):
        return super().delete_one(*args, **kwargs)

    @_check_for_changes('update')
    def update_many(self, *args, **kwargs):
        return super().update_many(*args, **kwargs)

    @_check_for_changes('update')
    def update_one(self, *args, **kwargs):
        return super().update_one(*args, **kwargs)

    @_check_for_changes('update')
    def replace_one(self, *args, **kwargs):
        return super().replace_one(*args, **kwargs)

    @_check_for_changes('insert')
    def insert_many(self, *args, **kwargs):
        return super().insert_many(*args, **kwargs)

    @_check_for_changes('insert')
    def insert_one(self, *args, **kwargs):
        return super().insert_one(*args, **kwargs)

    @_check_for_changes('bulk')
    def bulk_write(self, *args, **kwargs):
        return super().bulk_write(*args, **kwargs)

    @_check_for_changes('aggregate')
    def aggregate_raw_batches(self, pipeline, *args, **kwargs):
        return super().aggregate_raw_batches(pipeline, *args, **kwargs)

    @_check_for_changes('aggregate')
    def aggregate(self, pipeline, *args, **kwargs):
        return super().aggregate(pipeline, *args, **kwargs)

    # hack
    _check_for_changes = staticmethod(_check_for_changes)
    _synchronize = staticmethod(_synchronize)
