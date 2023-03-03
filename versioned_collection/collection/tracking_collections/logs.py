import dataclasses
import datetime
from typing import Dict, Optional, List, Tuple, Union

import pymongo
from bson import ObjectId
from pymongo.database import Database
from treelib import Node
from treelib.exceptions import NodeIDAbsentError

from versioned_collection.collection.tracking_collections import \
    _BaseTrackerCollection
from versioned_collection.errors import (
    BranchNotFound, InvalidCollectionState, InvalidCollectionVersion
)
from versioned_collection.tree import Tree
from versioned_collection.utils.data_structures import hashabledict


class LogsCollection(_BaseTrackerCollection):
    """ Stores the logs for the tracked versions of the target collection.

    This collection represents a log book for storing information about the
    registered versions of the target collection. A version has a number,
    a branch, a short description message and the timestamp at which it was
    registered.

    The log is structured as a tree, each branch of the tree representing a
    versioning branch, therefore the log tree can be seen as a tree
    describing how the versions of the target collection have evolved in
    time, i.e., a tree that has as nodes the nodes of the version tree.
    Since this tree is a small data structure, it is cached in memory and
    automatically updated when new versions are registered, together with its
    persistent in-database representation.

    """
    _NAME_TEMPLATE = '__log_{}'

    @dataclasses.dataclass
    class SCHEMA:
        version: int
        branch: str
        timestamp: datetime.datetime
        message: str
        prev: Optional[ObjectId]
        next: List[ObjectId]

        @property
        def id(self) -> Optional[ObjectId]:
            _id = None
            if hasattr(self, '_id'):
                _id = self._id
            return _id

        def __str__(self) -> str:
            return f"""\
            *   version:   {self.version}
                branch:    {self.branch}
                message:   {self.message}
                timestamp: {self.timestamp}
            """

        def __repr__(self) -> str:
            return f"<version: {self.version}, branch: {self.branch}>"

        def __hash__(self) -> int:
            return 0 if self.id is None else 389 * hash(self.id) \
                + 523 * self.version + 37 * hash(self.branch) \
                + 307 * hash(self.timestamp) + 31 * hash(self.message)

        def __eq__(self, other) -> bool:
            if other is None:
                return False
            if other is self:
                return True
            if not isinstance(other, LogsCollection.SCHEMA):
                return False

            return self.weakly_equals(other) and \
                self.message == other.message and \
                self.prev == other.prev and \
                set(self.next) == set(other.next)

        def weakly_equals(self, other) -> bool:
            """ Checks if two log objects are weakly equal.

            The weak equality does not take into account the linkage relation
            between log entries, i.e., it ignores the ``next`` and ``prev``
            fields.

            :param other: The object to check for weak equality.
            :return: Whether the log entries are weakly equal.
            """
            if other is None:
                return False
            if other is self:
                return True
            if not isinstance(other, LogsCollection.SCHEMA):
                return False
            return self.version == other.version and \
                self.branch == other.branch and \
                self.timestamp == self.timestamp and \
                self.id == other.id

    def __init__(self,
                 database: Database,
                 parent_collection_name: str,
                 **kwargs
                 ) -> None:
        super().__init__(database, parent_collection_name, **kwargs)

        self._log_tree: Optional[Tree] = None
        self._levels: Optional[Dict[Dict[str, Union[int, str]], int]] = None
        # Load the log tree
        if self.exists():
            self._load_log_tree()

    @property
    def log_tree(self) -> Optional[Tree]:
        """ Returns the log tree. """
        return self._log_tree

    def _load_log_tree(self) -> None:
        """ Builds and loads the log tree into memory. """
        log_entries = {e['_id']: e for e in self.find({})}
        # find the root
        root = None
        for entry_id, entry in log_entries.items():
            if entry['prev'] is None:
                root = entry
                break

        assert root is not None, "No root entry in the log tree!"

        # Build the tree and cache the level of each node.
        self._log_tree = Tree()
        self._levels = dict()
        to_visit = [root]
        root_identifier = self._get_log_tree_identifier(
            version=root['version'], branch=root['branch'])
        self._levels[root_identifier] = 0

        while len(to_visit) > 0:
            node = to_visit.pop(-1)

            _id = node.pop('_id')
            node_identifier = self._get_log_tree_identifier(
                version=node['version'], branch=node['branch'])

            if node['prev'] is None:
                parent = None
            else:
                parent = log_entries[node['prev']]
                parent = self._get_log_tree_identifier(
                    version=parent['version'], branch=parent['branch'])

            self._log_tree.create_node(
                identifier=node_identifier,
                tag=_id,
                data=self.SCHEMA(**node),
                parent=parent
            )

            for child_id in node['next']:
                child_doc = log_entries[child_id]
                to_visit.append(child_doc)

                child_node_id = self._get_log_tree_identifier(
                    child_doc['version'], child_doc['branch']
                )
                self._levels[child_node_id] = self._levels[node_identifier] + 1

    def build(self,
              message: Optional[str] = None,
              timestamp: Optional[datetime.datetime] = None,
              with_id: Optional[ObjectId] = None
              ) -> bool:
        """ Builds this collection on the database.

        :param message: The message associated with the initial version of
            this collection.
        :param timestamp: The timestamp when this collection was created.
        :param with_id: The id of the root of the log tree.
        :return: ``True`` if the collection was successfully built,
            ``False`` otherwise.
        """
        if self.exists():
            return False

        self._log_tree = Tree()
        self._levels = dict()
        message = "Initial collection." if message is None else message
        timestamp = datetime.datetime.utcnow() \
            if timestamp is None else timestamp

        self.add_log_entry(
            previous_version=-1,
            previous_branch=None,
            current_branch='main',
            message=message,
            timestamp=timestamp,
            with_id=with_id
        )

        self.create_index([
            ('version', pymongo.DESCENDING),
            ('branch', pymongo.ASCENDING)
        ])
        return True

    def reset(self) -> bool:
        """ Resets this collection and the in-memory cache.

        :return: ``True`` if the operation is successful, ``False`` otherwise.
        """
        if not self.exists():
            return False
        self._log_tree = Tree()
        self._levels = None
        self.drop()
        return True

    @staticmethod
    def _get_log_tree_identifier(version: int, branch: str) \
            -> Dict[str, Union[int, str]]:
        """ Creates a hashable dictionary with the given fields. """
        return hashabledict({'version': version, 'branch': branch})

    def contains_version(self, version: int, branch: str) -> bool:
        """ Returns whether the given version is present in the log tree.

        :param version: The version id of the version.
        :param branch: The branch on which the version is registered.
        :return: Whether the version exists in the log tree, i.e., if it is
            registered.
        """
        if self._log_tree is None:
            return False
        v_id = self._get_log_tree_identifier(version, branch)
        return self._log_tree.get_node(v_id) is not None

    def get_previous_version_and_branch(
            self,
            current_version: int,
            current_branch: str
    ) -> Optional[Tuple[int, str]]:
        """ Gets the version and branch name of the previous version.

        Looks up the current version in the log tree and returns the
        parent's node version number and the branch name.

        :return: ``None`` if the collection is not tracked, or if the current
            version is invalid or not registered, the version and branch name
            of the previous node otherwise. If the current version is the root
            version, ``-1`` will be returned as the previous version number.
        """
        if self._log_tree is None:
            return None

        curr_node = self._log_tree.get_node(self._get_log_tree_identifier(
            current_version, current_branch))
        if curr_node is None:
            return None

        node = self._log_tree.parent(curr_node.identifier)
        if node is None:
            # The current version is the root of the tree
            return -1, current_branch
        return node.data.version, node.data.branch

    def get_path_between_versions(
            self,
            current: Tuple[int, str],
            target: Tuple[int, str]
    ) -> Dict[Tuple[int, str], int]:
        """ Finds the path between the given points in the log tree.

        The current and the target versions should exist in the log tree,
        otherwise a :class:`ValueError` error is raised.

        .. note::
            The returned path includes the both ends.

        :param current: The start point version.
        :param target: The end point version.
        :return: An ordered dictionary representing the path that has to be
            followed in the log tree to get from the current version to the
            target version. The keys of the returned dictionary are tuples
            containing the version represented as ``(version, branch)`` and the
            values are the directions in time to be taken to get to the next
            version. The forward direction is represented as ``1``, and
            the backward direction as ``-1``. The last entry, representing the
            target version, has the direction of the previous step as direction.
        """

        _ERROR_MSG = "Version {} does not exist in the log tree!"

        if current == target:
            # The versions are the same, so there is no path.
            return dict()

        current_node: Node = self._log_tree.get_node(
            self._get_log_tree_identifier(*current)
        )
        if current_node is None:
            raise ValueError(_ERROR_MSG.format(current))

        target_node: Node = self._log_tree.get_node(
            self._get_log_tree_identifier(*target)
        )
        if target_node is None:
            raise ValueError(_ERROR_MSG.format(target))

        # Find the path between the nodes by computing their lowest common
        # ancestor.
        path: List[Tuple[Tuple[int, str], int]] = []
        src = current_node
        dst = target_node
        _swapped = False
        if self._levels[src.identifier] < self._levels[dst.identifier]:
            _swapped = True
            src, dst = dst, src

        while self._levels[src.identifier] != self._levels[dst.identifier]:
            path.append((src.identifier, -1))
            src = self._log_tree.parent(src.identifier)

        _path_dst = [(dst.identifier, 0)]
        while src != dst:
            path.append((src.identifier, -1))
            src = self._log_tree.parent(src.identifier)
            # The order here also takes case to add the root of the subtree
            # rooted in the LCA.
            dst = self._log_tree.parent(dst.identifier)
            _path_dst.insert(0, (dst.identifier, 1))

        path = path + _path_dst

        # Process the path
        # A bit ugly because tuples are immutable
        if _swapped:
            # Reverse the path and the direction in time.
            path = path[::-1]
            path = [((i['version'], i['branch']), -d) for (i, d) in path]

            sgn = 1
            if len(_path_dst) > 1:
                # In this case `src` and `dst` are on different branches,
                # joined by a branching node. For any possible path between
                # `src` and `dst` in this case, the direction at the
                # branching node should be 1, so fix it
                node_id, _ = path[len(_path_dst) - 1]
                path[len(_path_dst) - 1] = node_id, 1

                # After reversing the path in this case the direction of the
                # first step should be the opposite of the last step
                sgn = -1

            # Fix the direction at the beginning of the path after reversing
            path[0] = ((path[0][0][0], path[0][0][1]), sgn * path[-1][1])

        else:
            path = [((i['version'], i['branch']), d) for (i, d) in path]
            # This is not really needed for this direction, but change it for
            # consistency
            path[-1] = ((path[-1][0][0], path[-1][0][1]), path[-2][1])

        return dict(path)

    def add_log_entry(
            self,
            previous_version: int,
            previous_branch: Optional[str],
            current_branch: str,
            message: str,
            timestamp: datetime.datetime,
            with_id: Optional[ObjectId] = None
    ) -> Tuple[int, str]:
        """ Adds a new entry to the log tree.

        The log entries are created and added to the log tree when a new
        version of the target collection is registered. This method updates
        both the memory cached tree and the persistent database tree.

        :param previous_version: The version id of the previous version, i.e.,
            the version which was modified to generate the version to be
            registered. This should be `None` only for the first version.
        :param previous_branch: The branch name of the previous version, i.e.,
            the branch on which the previous version was registered.
        :param current_branch: The branch on which the new version should be
            registered.
        :param message: The message describing changes made to this version.
        :param timestamp: The time the version was registered.
        :param with_id: An optional :class:`ObjectId` used for inserting the
            new entry into this collection. This is used when adding entries
            to a remote collection.
        :return: The version id and the branch name of the new entry.
        """

        if previous_version == -1 and previous_branch is None:
            # This adds the root of the log tree
            version = 0
            previous_id = None
            prev_tree_node = None
        else:
            # A previous logical version exists
            identifier = self._get_log_tree_identifier(
                previous_version, previous_branch
            )
            prev_tree_node = self._log_tree.get_node(identifier)
            previous_id = prev_tree_node.tag

            # If the previous version branch is different, then this is the
            # first node on a new branch, so set its version to 0, otherwise
            # it is a node on an existing branch, so just increment the version
            version = 0 if previous_branch != current_branch else \
                prev_tree_node.data.version + 1

        # Create the new entry
        log_data = self.SCHEMA(
            version=version,
            branch=current_branch,
            message=message,
            timestamp=timestamp,
            prev=previous_id,
            next=[]
        )

        # Persist the change
        _log_data_dict = log_data.__dict__
        if with_id is not None:
            _log_data_dict['_id'] = with_id
        log_entry_id = self.insert_one(_log_data_dict).inserted_id

        # Update the `next` list of the parent on the database
        if prev_tree_node is not None:
            next_nodes = prev_tree_node.data.next
            next_nodes.append(log_entry_id)
            self.find_one_and_update(
                filter={'_id': prev_tree_node.tag},
                update={"$set": {"next": next_nodes}})

        # Update the in-memory cache
        identifier = self._get_log_tree_identifier(
            log_data.version, log_data.branch
        )
        self._log_tree.create_node(
            identifier=identifier,
            tag=log_entry_id,
            data=log_data,
            parent=prev_tree_node
        )

        # Update the node's cached level in the log tree.
        if prev_tree_node is not None:
            self._levels[identifier] = \
                self._levels[prev_tree_node.identifier] + 1
        else:
            # root node
            self._levels[identifier] = 0

        return version, current_branch

    def get_log(
            self,
            branch: str,
            version: Optional[int] = None,
            return_ids: bool = False
    ) -> List[SCHEMA]:
        """ Returns the log sorted in descending order for the given branch.

        If a version number is provided, the log will start at the collection
        version identified by `version` and `branch`, otherwise, the
        returned log will contain the entire history from the top of the
        branch to the root of the log tree.

        Raises :class:`ValueError` if there is no node on the given branch,
        or if there is no node identified by `version` and `branch`.

        :param branch: The branch on which for which to retrieve the log.
        :param version: The version where to start the log from.
        :param return_ids: Whether to include the ids of the log documents as
            well.
        :return: A list of log entries in descending order, i.e., the latest
            version at top.
        """
        if self._log_tree is None:
            return []

        if version is None:
            nodes = self._log_tree.leaves()
            leaf = None
            for node in nodes:
                if node.data.branch == branch:
                    leaf = node
            if leaf is None:
                raise ValueError(
                    f"Invalid branch name {branch}! No branches named {branch} "
                    f"were found in the log tree."
                )
        else:
            n_id = self._get_log_tree_identifier(
                version=version, branch=branch
            )
            leaf = self._log_tree.get_node(n_id)
            if leaf is None:
                raise ValueError(
                    f"Invalid version (version: {version}, branch: {branch})!"
                    f"No such version exists in the log tree."
                )

        if return_ids:
            leaf.data._id = leaf.tag
        entries: List[LogsCollection.SCHEMA] = [leaf.data]
        parent: Node = self._log_tree.parent(leaf.identifier)
        while parent is not None:
            data = parent.data
            if return_ids:
                data._id = parent.tag
            entries.append(data)
            parent = self._log_tree.parent(parent.identifier)
        return entries

    def get_log_entry(self, version: int, branch: str) -> Optional[SCHEMA]:
        """ Returns the entry for the given version in the log tree.

        :param version: The version for which the entry will be retrieved.
        :param branch: The branch for which the entry will be retrieved.
        :return: The entry in the log tree if it exists, ``None`` if the
            version or the log tree does not exist.
        """
        if self._log_tree is None:
            return None

        nid = self._get_log_tree_identifier(version, branch)
        node = self._log_tree.get_node(nid)
        return None if node is None else node.data

    def get_log_doc_id(self, version: int, branch: str) -> Optional[ObjectId]:
        """ Returns the id of the document with the given version.

        :param version: The version of the log entry.
        :param branch: The branch of the log entry.
        :return: The id of the log document in this collection, ``None`` if the
            version or the log tree does not exist.
        """
        if self._log_tree is None:
            return None

        nid = self._get_log_tree_identifier(version, branch)
        node = self._log_tree.get_node(nid)
        return None if node is None else node.tag

    def get_parent_branch(self, branch: str) -> Optional[str]:
        """ Retrieves the parent branch of the branch identified by `branch`.

        .. note::
            Branches are just unliked pointer documents. The branching structure
            of the collection is reflected in the log tree.

        :raises `~versioned_collection.error.BranchNotFound`: If no branch
            with the given name exists.
        :raises `~versioned_collection.error.InvalidCollectionState`: If the
            given branch does not have a parent.
        :param branch: The name of the branch whose parent should be returned.
        :return: The parent branch, or ``None`` is ``branch='main'``.
        """
        if branch == 'main':
            return None
        try:
            node = self._log_tree.parent(
                self._get_log_tree_identifier(0, branch)
            )
        except NodeIDAbsentError:
            raise BranchNotFound(branch)
        if node is None:
            raise InvalidCollectionState(
                f"Branch {branch} does not have a parent"
            )
        return node.data.branch

    def get_parent_version(self, version: Tuple[int, str]) \
            -> Optional[Tuple[int, str]]:
        """ Returns the version and branch of the previous version.

        :raises `treelib.exceptions.NodeIDAbsentError`: If the version
            identified by `version` does not exist.
        :param version: The version whose parent should be retrieved.
        :return: The parent of the given version.
        """
        if version == (0, 'main'):
            return None
        version = self._get_log_tree_identifier(*version)
        try:
            node = self._log_tree.parent(version)
        except NodeIDAbsentError:
            raise InvalidCollectionVersion(
                *version, message='Invalid collection version ({}, {})'
            )
        return node.data.version, node.data.branch

    def rebranch(self, version: Tuple[int, str], new_branch: str) -> None:
        """ Moves the versions starting at `version` to a new branch.

        This only updates the `branch` field of the log entries to be
        `new_branch` and resets the `version` counter field for `version`.

        :param version: The versions at which the enw branch should start.
        :param new_branch: The name of the new branch.
        """

        node: Node = self._log_tree.parent(
            self._get_log_tree_identifier(*version)
        )

        # Update the cached log tree
        doc_ids = []
        versions = 0
        children = self._log_tree.children(node.identifier)
        while len(children) > 0:
            for child in children:
                # Follow the branch
                if child.data.branch != version[1]:
                    continue
                child.data.branch = new_branch
                child.data.version = versions

                new_id = self._get_log_tree_identifier(versions, new_branch)
                self._levels[new_id] = self._levels.pop(child.identifier)

                # Update the node identifier in cache
                self._log_tree.update_node(
                    child.identifier,
                    identifier=new_id
                )
                versions += 1
                doc_ids.append(child.tag)
                node = child

            children = self._log_tree.children(node.identifier)

        # Update the database
        self.update_many(
            {'_id': {"$in": doc_ids}},
            {"$set": {'branch': new_branch}, "$inc": {'version': -version[0]}}
        )

    def delete_subtree(self, version: Tuple[int, str]) -> None:
        """ Deletes the subtree of the version tree rooted in `version`. """
        version_identifier = self._get_log_tree_identifier(*version)
        parent_node: Node = self._log_tree.parent(version_identifier)

        tree = self._log_tree.subtree(self._get_log_tree_identifier(*version))
        paths = {(p['version'], p['branch'])
                 for paths in tree.paths_to_leaves() for p in paths}
        cond = {"$or": [{'version': v, 'branch': b} for (v, b) in paths]}

        version_db_id = tree.get_node(tree.root).tag

        # Delete the entries from the database and cache
        self._log_tree.remove_subtree(version_identifier)
        self.delete_many(cond)

        # Update the parent
        parent_node.data.next.remove(version_db_id)
        self.find_one_and_update(
            filter={'_id': parent_node.tag},
            update={"$set": {"next": parent_node.data.next}}
        )

    def get_branch_tips_versions(self, version: Tuple[int, str]) \
            -> List[Tuple[int, str]]:
        """ Gets the versions of the leafs of the subtree rooted in `version`.

        :param version: A version in the log tree.
        :return: The versions of the tip of the branches of the log subtree
            rooted in `version`.
        """
        tree = self._log_tree.subtree(self._get_log_tree_identifier(*version))
        leaves = [
            (p[-1]['version'], p[-1]['branch'])
            for p in tree.paths_to_leaves()
        ]
        return leaves
