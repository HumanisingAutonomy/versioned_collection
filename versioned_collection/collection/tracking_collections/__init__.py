""" Helper tracking collection used in versioning.

This module defines various tracking collection used to version the target
:class:`VersionedCollection` collection.

This module should NOT be directly used by the user. Any interaction with a
tracked collection should be done through the :class:`VersionedCollection`
object.

.. note::
    Across this module there is mentioned the Version Tree. This is not an
    actual data structure, and it is not physically implemented by any class or
    collection. The version tree is the combination between the deltas and
    the log tree, so the Version Tree is the tree that has the same sets of
    nodes as the Log Tree, while its edges are represented by the set of
    per-document deltas that have to be applied to move between two
    consecutive versions. Another view of this abstract versioning
    structure is represented as a directed acyclic graph, that has the same
    nodes as the Log Tree, and each edge in the graph that links
    two versions represents a delta document that updates one document in the
    source version of the target collection to its version in the destination
    version node.
"""

from .base import _BaseTrackerCollection
from .branches import BranchesCollection
from .conflicts import ConflictsCollection
from .deltas import DeltasCollection
from .logs import LogsCollection
from .metadata import MetadataCollection
from .modified import ModifiedCollection
from .replica import ReplicaCollection
from .stash import StashCollection, StashContainer
from .lock import LockCollection

__all__ = [
    '_BaseTrackerCollection',
    'BranchesCollection',
    'ConflictsCollection',
    'DeltasCollection',
    'LogsCollection',
    'MetadataCollection',
    'ModifiedCollection',
    'ReplicaCollection',
    'StashCollection',
    'StashContainer',
    'LockCollection'
]
