import dataclasses
import datetime
from copy import deepcopy
from functools import partial
from multiprocessing import cpu_count, Pool
from typing import Any, Dict, Optional, List, Tuple, Union

import pymongo
from bson import ObjectId
from deepdiff import DeepDiff, Delta
from pymongo.command_cursor import CommandCursor
from pymongo.database import Database
from treelib import Node

from versioned_collection.collection.tracking_collections import (
    _BaseTrackerCollection,
)
from versioned_collection.errors import InvalidCollectionState
from versioned_collection.tree import Tree
from versioned_collection.utils.data_structures import hashabledict
from versioned_collection.utils.mongo_query import group_documents_by_id


class DeltasCollection(_BaseTrackerCollection):
    """Stores the deltas between different versions of the target collection.

    Each `delta` document in this collection reflects the changes and
    therefore, the actions needed to be performed to move between different
    versions of the documents in the target collection. If no deltas for a
    particular document are present, then that document has not been modified
    since the target collection was initialised for versioning.

    The `deltas` documents store information about the changes of particular
    documents in the target collection for a specific version and a specific
    branch of the versioning system. Even though the deltas for all documents
    are stored together, they can be grouped by the document in the target
    collection that they modify. The per-document deltas are tree
    structures, where the branches of the tree are a subset of the branches of
    the version tree (because some documents may not be modified on some
    branches). The per-document delta tree allows applying changes in both
    directions of time for a specific document. Since the target collection
    may contain multiple documents, this collection represents a set of
    per-document delta trees.

    To get to a specific version of the target collection we need the path in
    version tree or log tree between the two versions. All the deltas between
    the start and the end points of the path will be retrieved according to
    the direction in which we should navigate the delta trees. The delta
    trees for each modified document between the two versions are built and
    intersected with the given path, to get the sequence of deltas that have
    to be applied to a document to move across versions.

    """

    _NAME_TEMPLATE = '__deltas_{}'
    _DOCUMENT_TYPE = Dict[str, Any]

    # Used by deepdiff.Delta
    _SAFE_TO_IMPORT = {'bson.objectid.ObjectId'}

    @dataclasses.dataclass
    class SCHEMA:
        document_id: Any  # ObjectId | Dict[str, Any] | float | str | int
        collection_version_id: int
        branch: str
        timestamp: datetime.datetime
        forward: bytes
        backward: bytes
        prev: Optional[ObjectId]
        next: List[ObjectId]

        def __getitem__(self, attr: str):
            if not hasattr(self, attr):
                return None
            return self.__dict__[attr]

    def __init__(
        self, database: Database, parent_collection_name: str, **kwargs
    ) -> None:
        super().__init__(database, parent_collection_name, **kwargs)

    def build(self) -> bool:
        """Create the collection on the database.

        :return: ``False`` if the collection already exists, ``True`` otherwise.
        """
        if self.exists():
            return False

        self.create_index('document_id')
        self.create_index([
            ('collection_version_id', pymongo.DESCENDING),
            ('branch', pymongo.ASCENDING),
        ])
        return True

    def _build_delta_tree_set(
        self, deltas: List[_DOCUMENT_TYPE]
    ) -> Optional[List[Tree]]:
        """Build the per-document delta tree set.

        .. note::
            The given list of documents should be deltas related to a single
            document.

        The delta tree is just an in-memory representation of the delta tree
        stored in this collection for each document. Since deltas do not
        necessarily need to be all connected, the delta tree may be split
        into unconnected components, which for the delta tree set.

        :param deltas: A list of document deltas related to a document.
        :return: The delta tree set.
        """
        if len(deltas) == 0:
            return None

        # Find the roots of each unconnected delta tree or the root of the
        # document delta tree. Note that it is impossible to have both a
        # global root and unconnected trees, because if there is a global
        # root, then all subtrees will be connected.
        deltas = {d['_id']: self._delta_doc_to_schema(d) for d in deltas}
        deltas: Dict[ObjectId, DeltasCollection.SCHEMA]
        roots: List[ObjectId] = []
        root = None
        for d_id, delta in deltas.items():
            if root is None:
                root = d_id, delta
            if delta.timestamp < root[1].timestamp:
                root = d_id, delta
            if delta.prev is None:
                roots.append(d_id)

        if len(roots) == 0:
            roots.append(root[0])

        return [self._build_delta_tree(root, deltas) for root in roots]

    @staticmethod
    def _build_delta_tree(
        root: ObjectId, deltas: Dict[ObjectId, SCHEMA]
    ) -> Tree:
        # Build the per-document partial delta tree.
        tree = Tree()
        to_visit = [root]  # the id of the root delta
        while len(to_visit) > 0:
            _id = to_visit.pop(-1)
            if _id not in deltas:
                continue
            _delta = deltas[_id]
            # This allows building partial trees
            parent = _delta.prev if _delta.prev in deltas else None
            tree.create_node(identifier=_id, parent=parent, data=_delta)
            to_visit.extend(_delta.next)

        return tree

    def add_delta(
        self,
        document_old: _DOCUMENT_TYPE,
        document_new: _DOCUMENT_TYPE,
        document_id: ObjectId,
        collection_version: int,
        branch: str,
        timestamp: datetime.datetime,
        branch_history: List[Tuple[int, str]],
        with_id: Optional[ObjectId] = None,
    ) -> Optional[ObjectId]:
        """Compute and records the deltas between the given document versions.

        :raises InvalidCollectionState: If some deltas for the current
            collection version cannot be identified.

        :param document_old: The old version of the document.
        :param document_new: The new version of the document, that contains
            changes.
        :param document_id: The id of the modified document.
        :param collection_version: The version of the tracked collection to
            which the changes to the given document should be registered.
        :param branch: The branch that the modified target document belongs to.
        :param timestamp: The date and time when the delta was registered.
        :param branch_history: A set containing (version, branch) tuples
            from the previous version to the root of the version tree.
        :param with_id: An optional :class:`ObjectId` used for inserting the
            new entry into this collection. This is used when adding deltas
            from a local to a remote collection.
        :return: The id of the delta document, or ``None`` if the two versions
            of the document are unchanged.
        """
        forward = DeepDiff(
            document_old,
            document_new,
            ignore_order=False,
            report_repetition=False,
        )

        # No changes actually made since the previous registered version.
        # This can be caused by updating the document once, and then updating
        # it again to its previous version.
        if forward == {}:
            return None

        forward = Delta(forward)

        # Search the per-document delta tree for a previous delta

        # Get the set of deltas
        deltas = self.find({'document_id': document_id})

        # Keep only the deltas that are part of the branch history
        _hist_set = set(branch_history)
        # We have to add this for deltas that are already registered in case
        # of multiple document updates
        _hist_set.add((collection_version, branch))
        deltas = [
            d
            for d in deltas
            if (d['collection_version_id'], d['branch']) in _hist_set
        ]

        prev_delta_doc = None
        prev_delta_node = None

        # Force a delta recompute in the case a delta has already been
        # registered during this transaction
        # This should not happen because the tracking documents are grouped
        # by the document's ID. However, it is possible that the listener
        # hasn't finished marking all documents as modified by the time they are
        # extracted from 'modified documents' collection.
        update_forward_and_backward_deltas = False
        delta_doc_id = None

        # If the document has been modified before, find the previous delta
        # that modified the document on the current path from root to
        # the current node in the version tree.
        if len(deltas) > 0:
            # Build the delta tree for this document
            deltas = list(deltas)
            trees = self._build_delta_tree_set(deltas)
            tree = None
            for _tree in trees:
                root = _tree.get_node(_tree.root)
                root_version = root.data.collection_version_id, root.data.branch
                if root_version in _hist_set:
                    tree = _tree
                    break
            assert tree is not None

            branch_history = deepcopy(branch_history)

            # Trim the path to match the root of the delta tree
            next_node: Node = tree.get_node(tree.root)
            while (
                len(branch_history) > 0
                and self._version_of(next_node) != branch_history[-1]
            ):
                branch_history.pop(-1)

            # Use the reversed history to descend from root to the latest
            # delta that modified the document
            branch_history.insert(0, (collection_version, branch))
            while (
                len(branch_history) > 1
                and self._version_of(next_node) == branch_history[-1]
            ):
                branch_history.pop(-1)
                for child in tree.children(next_node.identifier):
                    if self._version_of(child) == branch_history[-1]:
                        next_node = child

            if self._version_of(next_node) == (collection_version, branch):
                old_forward_diff = Delta(
                    next_node.data['forward'],
                    safe_to_import=self._SAFE_TO_IMPORT,
                ).diff
                if forward.diff == old_forward_diff:
                    # TODO: log here to make sure this doesn't happen
                    #  under stress tests, then remove
                    # The document has not changed, but it was simply
                    # modified multiple times before registering the new
                    # version.
                    return next_node.identifier
                else:
                    update_forward_and_backward_deltas = True
                    delta_doc_id = next_node.identifier
            else:
                prev_delta_node = next_node
                prev_delta_doc = next_node.data.__dict__
                prev_delta_doc['_id'] = next_node.identifier

        backward = Delta(
            DeepDiff(
                document_new,
                document_old,
                ignore_order=False,
                report_repetition=False,
            )
        )

        delta_doc = self.SCHEMA(
            document_id=document_id,
            collection_version_id=collection_version,
            branch=branch,
            timestamp=timestamp,
            forward=forward.dumps(),
            backward=backward.dumps(),
            prev=None if prev_delta_doc is None else prev_delta_doc['_id'],
            next=[],
        ).__dict__

        if update_forward_and_backward_deltas:
            self.update_one(
                {'_id': delta_doc_id},
                update={
                    "$set": {
                        'forward': delta_doc['forward'],
                        'backward': delta_doc['backward'],
                    }
                },
            )
            delta_doc['_id'] = delta_doc_id
        else:
            if with_id:
                delta_doc['_id'] = with_id
            self.insert_one(delta_doc)

        # Link the delta with its parent
        if prev_delta_doc is not None:
            next_list = prev_delta_node.data.next
            next_list.append(delta_doc['_id'])
            self.find_one_and_update(
                filter={'_id': delta_doc['prev']},
                update={"$set": {"next": next_list}},
            )

        return delta_doc['_id']

    def insert_delta_docs(self, delta_docs: List[_DOCUMENT_TYPE]) -> None:
        """Insert a list of delta documents into this collection.

        .. warning::
            This method modifies the delta documents and removes the ids of
            the documents from the ``next`` field that are not part of the
            given `delta_docs` list.

        This is used during remote-local synchronisation of a branch,
        therefore the inserted deltas are slightly modified versions of the
        local deltas. Since a single branch can be pushed or pulled at a
        time, the forward references to other delta documents that are not in
        the given list are removed. Also, the parent deltas are updated to
        include the first delta document in `delta_docs` in its forward
        references field, i.e., ``next``.

        :param delta_docs: The delta documents to be inserted.
        """
        deltas_ids = {d['_id'] for d in delta_docs}
        for i, delta_doc in enumerate(delta_docs):
            # Make sure we clean up the delta documents to include
            # information only about the desired branch.
            delta_doc['next'] = [
                d for d in delta_doc['next'] if d not in deltas_ids
            ]

            if i == 0 and delta_doc['prev'] is not None:
                # Update the parent of the first delta doc.
                # The following delta docs are inserted directly
                self.find_one_and_update(
                    filter={'_id': delta_doc['prev']},
                    update={"$push": {"next": delta_doc['_id']}},
                )
        # Add the delta documents
        self.insert_many(delta_docs)

    def _delta_doc_to_schema(self, delta: Dict[str, Any]) -> SCHEMA:
        """Convert a delta document to a schema object."""
        delta.pop('_id')
        return self.SCHEMA(**delta)

    def get_deltas(
        self, path: Dict[Tuple[int, str], int]
    ) -> Dict[Any, List[Delta]]:
        """Retrieve the deltas across the given path of versions.

        :param path: The path between two versions. The keys identify the
            version (i.e., (version, branch) tuples) and the values the
            direction in time to take to move between versions.
        :return: A list of tuples containing the requested deltas, grouped by
            the documents' id.
        """
        # Get the deltas grouped by the document's id for the versions in
        # `path`.
        documents = self.get_delta_documents_in_path(path)

        deltas = dict()
        for doc in documents:
            _deltas = doc['deltas']
            # Build the partial delta tree
            trees = self._build_delta_tree_set(_deltas)
            # Since _build_partial_delta_tree is only used in this method,
            # we could've merged the functions, instead of composing them,
            # since _get_deltas overlaps a partial with the path anyway. We
            # still do the same amount of work, so it's fine for now.
            _deltas = self._get_deltas(
                self._build_partial_delta_tree(trees, path), path
            )
            doc_id = doc['_id']
            if isinstance(doc_id, dict):
                doc_id = hashabledict(doc_id)
            deltas[doc_id] = _deltas

        return deltas

    def _build_partial_delta_tree(
        self,
        trees: List[Tree],
        path: Dict[Tuple[int, str], int],
    ) -> Tree:
        """Build a partial per-document delta tree out of unconnected trees.

        :param trees: The trees to merge.
        :param path: The path to follow across the merged tree.
        :return: A partial document delta tree.
        """
        # The delta tree is complete, nothing to do.
        if len(trees) == 1:
            return trees[0]

        # Filter the trees whose roots are not in path
        trees = [
            t for t in trees if self._version_of(t.get_node(t.root)) in path
        ]

        # If after filtering the out-of-path trees, we are left with a single
        # tree, then this tree is sufficient to recover the correct deltas.
        if len(trees) == 1:
            return trees[0]

        # At this point, we can only have 2 unconnected trees that are part of
        # different branches in the version tree.
        # Proof:
        #   Suppose the 2 trees are part of the same branch. Since none of
        #   the trees were previously filtered, the root of one tree has to
        #   be one end of the path, and the leaf of the other has to be the
        #   other end of the path. But since they are part of the same
        #   branch, there exists a set of deltas for that branch that
        #   transform the document between the end points of the path,
        #   therefore there exists a unique delta tree -> contradiction.
        #   If the path is a linear chain (has no change in
        #   direction), then there exists a unique tree since the path is
        #   contained within a single branch. Otherwise, the path is split
        #   between two branches and there exists a delta tree for each
        #   branch, so 2 trees.
        assert len(trees) == 2

        direction = None
        version: Optional[int, str] = None
        for i, (_version, _direction) in enumerate(path.items()):
            _version: Tuple[int, str]
            if i == 0:
                direction = _direction
            if direction != _direction:
                version = _version
                break

        empty_delta_binary = Delta(
            DeepDiff(
                dict(),
                dict(),
                ignore_order=False,
                report_repetition=False,
            )
        ).dumps()

        tree = Tree()
        tree.create_node(
            identifier=-1,
            parent=None,
            data=DeltasCollection.SCHEMA(
                document_id=None,
                collection_version_id=version[0],
                branch=version[1],
                timestamp=None,  # noqa
                forward=empty_delta_binary,
                backward=empty_delta_binary,
                prev=None,  # noqa
                next=[t.get_node(t.root).identifier for t in trees],
            ),
        )
        tree.paste(tree.root, trees[0])
        tree.paste(tree.root, trees[1])
        return tree

    def get_delta_documents_in_path(
        self,
        path: Dict[Tuple[int, str], int],
        sorting_order: Optional[int] = None,
    ) -> CommandCursor:
        """Get the delta documents grouped by tracked document's id in `path`.

        :param path: The path in the version tree from which to pull the
            delta documents.
        :param sorting_order: The order in which to sort the delta documents by
            timestamp. If omitted, the sorting is skipped..
        :return: The delta documents.
        """
        versions = list(path.keys())
        directions = set(path.values())
        if not (1 in directions and -1 in directions):
            if len(versions) > 1:
                if path[versions[0]] == -1:
                    versions.pop(-1)
                elif path[versions[0]] in [0, 1]:
                    versions.pop(0)

        # fmt: off
        cond = {"$or": [
            {'collection_version_id': v, 'branch': b}
            for (v, b) in versions
        ]}
        # fmt: on

        sort_stage = (
            [{"$sort": {'timestamp': sorting_order}}]
            if sorting_order is not None
            else []
        )

        # fmt: off
        documents = self.aggregate([
            {"$match": cond},
            *sort_stage,
            {"$group": {'_id': "$document_id", 'deltas': {"$push": "$$ROOT"}}}
        ],
            allowDiskUse=True
        )
        # fmt: on
        return documents

    def rebranch(
        self,
        start_version: Tuple[int, str],
        new_branch: str,
        num_versions: int,
    ) -> None:
        """Move the deltas after `start_version` to another branch.

        :param start_version: The version which should be moved to a new branch.
        :param new_branch: The name of the new branch.
        :param num_versions: The number of versions to move, i.e., the length
            of the branch starting at `start_version`.
        """
        cond = {
            "$or": [
                {'collection_version_id': v, 'branch': start_version[1]}
                for v in range(
                    start_version[0], start_version[0] + num_versions
                )
            ]
        }

        self.update_many(
            cond,
            {
                "$set": {'branch': new_branch},
                "$inc": {'collection_version_id': -start_version[0]},
            },
        )

    @staticmethod
    def _version_of(n: Node) -> Tuple[int, str]:
        """Get the version identifier of a node in the delta tree."""
        return n.data.collection_version_id, n.data.branch

    def _get_deltas(
        self, tree: Tree, path: Dict[Tuple[int, str], int]
    ) -> List[Delta]:
        """Get the list of deltas for the given path.

        Given a delta tree and a sequence of versions and the direction to be
        taken into the version tree to navigate between them, it computes the
        actual deltas that needs to be applied to a document to get from the
        first version in `path` to the last one.

        Visually, it computes the overlap, or the intersection between the
        delta tree and the path. The path could either contain a subtree of
        the delta tree, or be contained in the delta tree.

        :param tree: A per-document delta tree.
        :param path: A path in the version tree between the start and end
            versions.
        :return: A list of deltas that have to be applied to the document
            linked to the given `tree` that modify the document.
        """

        def _extract_and_decode_deltas(
            nodes: List[Node], _direction: str
        ) -> List[Delta]:
            assert _direction in [
                'forward',
                'backward',
            ], "Invalid delta direction!"

            return [
                Delta(
                    n.data[_direction],
                    safe_to_import=DeltasCollection._SAFE_TO_IMPORT,
                )
                for n in nodes
            ]

        # Find the first node in the delta tree that is in :param:`path`. The
        # subtree rooted in that node contains the required deltas.
        to_visit = [tree.root]
        node = None
        while len(to_visit) > 0:
            node = tree.get_node(to_visit.pop(0))

            if self._version_of(node) in path:
                break

            for child in tree.children(node.identifier):
                to_visit.append(child.identifier)

        # Reconstruct the paths. There are two possible cases:
        #   1. `node` has only one child in `path`, therefore `node` is
        #   either the start or the end of the path. This can be decided by
        #   inspecting the direction for `node`, i.e., it is the start if the
        #   direction is `1`, and it is the end if the direction is `-1`.
        #
        #   2. `node` has two children in path, so the path starts lower in
        #   the tree, goes up to `node` and then descends to another child node,
        #   so the direction in time should change.

        # Find the paths in the subtree that intersect the `path`.
        paths = [], []
        i = 0
        for child in tree.children(node.identifier):
            _is_in_path = False
            to_visit = [child]
            # Go forward only if this child in path
            while len(to_visit) > 0 and self._version_of(child) in path:
                _is_in_path = True
                _path = paths[i]
                _node = to_visit.pop(-1)

                # Order the inserted nodes by their traversal order
                if path[self._version_of(_node)] == -1:
                    _path.insert(0, _node)
                else:
                    _path.append(_node)

                for next_child in tree.children(_node.identifier):
                    # At this point only one child can be in the path
                    if self._version_of(next_child) in path:
                        to_visit.append(next_child)

            if _is_in_path:
                i += 1

        if len(paths[1]) == 0:
            # Case 1
            _deltas = paths[0]
            if path[self._version_of(node)] == 1:
                direction = 'forward'
                _deltas.insert(0, node)
            else:
                direction = 'backward'
                _deltas.append(node)
            _deltas = _extract_and_decode_deltas(_deltas, direction)
        else:
            # Case 2
            # Fix the order
            if path[self._version_of(paths[0][0])] in [0, 1]:
                paths = paths[1], paths[0]

            # Build the path
            left = _extract_and_decode_deltas(paths[0], 'backward')
            right = _extract_and_decode_deltas(paths[1], 'forward')

            # We don't need the root here.
            _deltas = left + right

        return _deltas

    def apply_deltas(
        self,
        per_document_deltas: Dict[Any, List[Delta]],
        documents: List[_DOCUMENT_TYPE],
        return_current_documents: bool = False,
    ) -> Union[
        Dict[Any, _DOCUMENT_TYPE],
        Tuple[Dict[Any, _DOCUMENT_TYPE], Dict[Any, _DOCUMENT_TYPE]],
    ]:
        """Update the given documents and returns them.

        Applies the deltas between two versions of the target collection. It
        uses the given deltas grouped by document and sorted by the direction
        on which they have to be applied and sequentially updates each
        document to get to the version of the document for the target version
        of the tracked collection.

        :param per_document_deltas: The prefetched and sorted list of deltas
            that have to be applied, grouped by document id.
        :param documents: The list of documents that will be updated.
        :param return_current_documents: Whether to return the current
            documents grouped by id.
        :param: If `return_current_documents` is set, the current documents are
            returned as well.
        :return: The updated documents grouped by their ``'_id'`` field. If
            The documents that are empty should be removed from the target
            collection.
        """
        __PROCESSING_LIMIT = 1000
        # Allow complex ids
        documents = group_documents_by_id(documents)

        # If we have to update a lot of documents, do it in parallel.
        if len(per_document_deltas) > __PROCESSING_LIMIT:
            per_document_deltas = list(per_document_deltas.items())
            _f = partial(self._process_deltas, documents=documents)
            chunk_size = max(1, len(per_document_deltas) // cpu_count() * 3)
            with Pool(cpu_count()) as p:
                updated_docs = p.map(
                    _f, per_document_deltas, chunksize=chunk_size
                )

            updated_docs = dict(updated_docs)
        else:
            updated_docs = dict()
            for item in per_document_deltas.items():
                doc_id, doc = self._process_deltas(item, documents)
                updated_docs[doc_id] = doc

        if return_current_documents:
            return updated_docs, documents
        else:
            return updated_docs

    @staticmethod
    def _process_deltas(
        item: Tuple[Any, List[Delta]],
        documents: Dict[Any, _DOCUMENT_TYPE] = None,
    ) -> Tuple[Any, _DOCUMENT_TYPE]:
        doc_id, deltas = item
        # Documents that have to be created do not exist at all in the
        # target collection, so use an empty document and update it.
        document = documents.get(doc_id, {})

        # Sequentially apply the deltas
        for delta in deltas:
            document = document + delta
        if isinstance(doc_id, dict):
            doc_id = hashabledict(doc_id)

        return doc_id, document

    def delete_subtrees(
        self,
        root: Tuple[int, str],
        leaves: List[Tuple[int, str]],
    ) -> None:
        """Delete the deltas registered after a specific version.

        :param root: The root of the subtree of the version tree from which
            deltas will be removed.
        :param leaves: The versions of the leaves of the version subtree.
        """
        # Construct the paths on which to remove the deltas
        paths: Dict[Tuple[int, str], Optional[int]] = {root: None}
        for leaf_v, leaf_b in leaves:
            stop_v = 0 if leaf_b != root[1] else root[0]
            while leaf_v >= stop_v:
                paths[(leaf_v, leaf_b)] = None
                leaf_v -= 1

        deltas_per_doc = self.get_delta_documents_in_path(
            path=paths, sorting_order=pymongo.ASCENDING
        )
        delta_ids = []
        for doc in deltas_per_doc:
            for i, delta_doc in enumerate(doc['deltas']):
                delta_ids.append(delta_doc['_id'])

                if i == 0 and delta_doc['prev'] is not None:
                    # Remove the delta reference from the parent
                    self.find_one_and_update(
                        filter={'_id': delta_doc['prev']},
                        update={"$pull": {"next": delta_doc['_id']}},
                    )

        # Delete the deltas
        self.delete_many({'_id': {"$in": delta_ids}})
