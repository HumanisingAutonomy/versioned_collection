import queue
from datetime import datetime
from multiprocessing import Process, Value, Queue, Lock
from time import sleep
from typing import Optional, Tuple

from bson import ObjectId
from pymongo import MongoClient

from versioned_collection.collection.tracking_collections import (
    ModifiedCollection,
)


class CollectionListener:
    """Listens to changes to a specific collection.

    Starts a background process that monitors the target collection for any
    changes and stores the ids of the updated documents in the
    `ModifiedCollection` linked to the target collection.
    """

    def __init__(
        self,
        database_name: str,
        collection_name: str,
        host: str = 'localhost',
        port: int = 27017,
        credentials: Tuple[Optional[str], Optional[str]] = None,
    ) -> None:
        """Initialise a  :class:`CollectionListener`.

        :param database_name: The name of the database on which the target
            collection is located.
        :param collection_name: The name of the collection to listen to.
        :param host: The host of the MongoDB server.
        :param port: The port on which to connect to the MongoDB server.
        :param credentials: The username and password of a valid user with
            access to the database.
        """
        self._database_name = database_name
        self._collection_name = collection_name
        self.__credentials = (
            (None, None) if credentials is None else credentials
        )
        self._address = host, port
        self._p: Optional[Process] = None
        self._HEARTBEAT_TIMEOUT = 0.05

        # Listener synchronisation helpers
        self._listening: Value = None
        self._lock: Lock = None
        self._timestamp_q: Optional[Queue] = None
        self._heartbeat_q: Optional[Queue] = None

        # Start the listener
        self.start()

    def __del__(self):
        self.stop()

    def is_listening(self) -> bool:
        """Check if this listener has started listening to changes.

        :return: Whether the listener is listening or not.
        """
        if self._listening is None:
            return False
        return bool(self._listening.value)

    def stop(self) -> None:
        """Stop this listener from monitoring the target collection.

        The listener is safely stopped to allow the changes (that were produced
        before signaling the listener to stop) to be processed. This is
        relevant when a large number of entries are modified in any way from
        the tracked collection in a transaction, or when unacknowledged
        operations are performed and the database queues the operations to be
        performed.
        """
        if not self.is_listening():
            return

        with self._lock:
            timestamp = datetime.utcnow()
            self._listening.value = False
            self._timestamp_q.put(timestamp)

        # Wait for the listener to finish consuming the valid changes from
        # the change stream.
        while True:
            try:
                self._heartbeat_q.get(
                    block=True,
                    timeout=self._HEARTBEAT_TIMEOUT,
                )
            except queue.Empty:
                # The heartbeat timeout expired, so the listener is
                # probably idle, therefore kill it.
                break

        self._p.terminate()
        self._p.join()

    def start(self) -> None:
        """Start the listener to monitor the target collection.

        Lunches a monitoring daemon that uses `changeStreams` to watch the
        target collection for all types of updates.

        Blocks until the listener daemon has successfully started to prevent
        the client from modifying the target collection.
        """
        if self.is_listening():
            return

        self._listening = Value('b', False)
        self._timestamp_q = Queue()
        self._lock = Lock()
        self._heartbeat_q = Queue()
        self._p = Process(
            target=self._listen,
            args=(
                self._database_name,
                self._collection_name,
                *self._address,
                *self.__credentials,
                self._listening,
                self._timestamp_q,
                self._heartbeat_q,
                self._lock,
            ),
        )
        self._p.daemon = True
        self._p.start()

        # Block until the listener started.
        while True:
            sleep(0.001)
            if self.is_listening():
                break

    @staticmethod
    def _listen(
        database_name: str,
        collection_name: str,
        host: str,
        port: int,
        username: Optional[str],
        password: Optional[str],
        listening: Value,
        last_timestamp: Queue,
        heartbeat_q: Queue,
        lock: Lock,
    ) -> None:
        """Listen in a background task.

        Opens a client connection to the given database and starts watching
        the collection identified by `collection_name`. The ids of the
        modified documents are inserted as standalone documents into the
        `__modified_<collection_name>` collection.

        For a description of the synchronisation mechanisms see the
        documentation of :meth:`stop` method.

        :param database_name: The name of the database to connect to.
        :param collection_name: The name of the collection to watch for changes.
        :param host: The host where the database is located.
        :param port: The port at which the database can be accessed.
        :param listening: Whether the listener listens (or should listen) to
            the collection or not.
        :param last_timestamp: The time after which changes should be ignored
        :param heartbeat_q: A channel for sending heartbeats to the parent
            process.
        :param lock: A lock used to synchronise the shared variables.
        """
        client = MongoClient(
            host=host, port=port, username=username, password=password
        )
        target_collection = client[database_name][collection_name]

        _output_collection = ModifiedCollection(
            database=client[database_name],
            parent_collection_name=collection_name,
        )

        timestamp = None
        docs = []
        batch_size = 100
        with target_collection.watch() as change_stream:
            listening.value = True
            for change in change_stream:
                with lock:
                    # The timestamp is sent only once by the parent process
                    # before terminating the process
                    try:
                        timestamp = last_timestamp.get(block=False)
                    except queue.Empty:
                        pass

                    # Process all changes that happened before the time the
                    # stop listening 'signal' was sent. This allows properly
                    # processing the pending changes that queued before being
                    # streamed through the change stream by mongo.
                    if timestamp is not None:
                        change_time = change['clusterTime'].as_datetime()
                        change_time = change_time.replace(tzinfo=None)
                        if change_time > timestamp:
                            # stop listening
                            if len(docs):
                                _output_collection.insert_many(docs)
                                docs = []
                            break

                    # Send heartbeats to the parent process to signal that
                    # this process is still processing the pending changes.
                    if not listening.value:
                        heartbeat_q.put(0)

                    try:
                        document_id = change["documentKey"]['_id']
                        op_type = change["operationType"][0]
                        if op_type == 'r':
                            op_type = 'u'
                        # Manually generate ids to keep the order of the events
                        # and allow parallel insertion in database
                        docs.append({
                            '_id': ObjectId(),
                            'id': document_id,
                            'op': op_type,
                        })
                        if (
                            not change_stream._cursor._has_next()  # noqa
                            or len(docs) > batch_size  # noqa
                        ):
                            _output_collection.insert_many(docs)
                            docs = []
                    except KeyError:
                        # not really needed, but just in case the change stream
                        # hangs
                        break
        if len(docs) > 0:
            _output_collection.insert_many(docs)
