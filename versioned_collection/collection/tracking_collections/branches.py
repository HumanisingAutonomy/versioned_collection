from __future__ import annotations

import dataclasses
from typing import Optional, List, Set, Dict, Any

from pymongo.database import Database

from versioned_collection.collection.tracking_collections import _BaseTrackerCollection
from versioned_collection.errors import BranchNotFound


class BranchesCollection(_BaseTrackerCollection):
    """Stores information about the branch pointers.

    Branches are pointers to specific version numbers and branch names on the
    version tree. The version tree is a tree that has as nodes the version
    identifiers, i.e., version id and branch name, of a version of the target
    collection, and as edges the set of per-document deltas that have to be
    applied to move between versions. Technically, the branches point to the
    latest set of per-document deltas that has to be applied to the previous
    version of the collection to get to the latest version, but that
    specific set of deltas is identified by the same tuple of version
    identifiers as the version node itself.

    """

    _NAME_TEMPLATE = '__branches_{}'

    @dataclasses.dataclass
    class SCHEMA:
        name: str
        points_to_collection_version: int
        points_to_branch: str

        def __hash__(self) -> int:
            return hash((
                self.name,
                self.points_to_branch,
                self.points_to_collection_version
            ))

        def __eq__(self, other: BranchesCollection.SCHEMA) -> bool:
            if not isinstance(other, BranchesCollection.SCHEMA):
                return False
            if self is other:
                return True
            return (
                self.name == other.name
                and self.points_to_collection_version
                == other.points_to_collection_version
                and self.points_to_branch == other.points_to_branch
            )

    def __init__(
        self,
        database: Database,
        parent_collection_name: str,
        **kwargs,
    ) -> None:
        super().__init__(database, parent_collection_name, **kwargs)

    def build(self) -> bool:
        """Create the collection on the database.

        :return: ``False`` if the collection already exists, ``True`` otherwise.
        """
        if self.exists():
            return False
        self.create_branch(
            branch='main',
            pointing_to_collection_version=0,
            pointing_to_branch='main',
        )
        return True

    def has_branch(self, branch_name: str) -> bool:
        """Check whether a branch name with the provided name exists."""
        return self.find_one({'name': branch_name}) is not None

    def get_branch_names(self) -> Set[str]:
        """Return the names of the existing branches."""
        return set(self.distinct('name'))

    def create_branch(
        self,
        branch: str,
        pointing_to_collection_version: int,
        pointing_to_branch: str,
    ) -> None:
        """Create a new branch pointing to the specified location.

        :raises ValueError: If a branch with name ``branch`` already exists.

        :param branch: The name of the new branch.
        :param pointing_to_collection_version: The collection version to
            which this branch should point to.
        :param pointing_to_branch: The branch on which the collection version
            that the new branch should point to is located.
        """
        if self.has_branch(branch):
            raise ValueError(f"Branch {branch} already exists.")

        branch = self.SCHEMA(
            name=branch,
            points_to_collection_version=pointing_to_collection_version,
            points_to_branch=pointing_to_branch,
        )
        self.insert_one(branch.__dict__)

    def update_branch(
        self,
        branch: str,
        pointing_to_collection_version: int,
        pointing_to_branch: str,
        new_name: Optional[str] = None,
    ) -> None:
        """Update the information about a branch pointer.

        :raises ValueError: If no branch with name ``branch`` exists.

        :param branch: The name of the branch to be updated.
        :param pointing_to_collection_version:  The new collection version to
            which the new branch points to.
        :param pointing_to_branch: The branch on which the new version of the
            collection was registered.
        :param new_name: The new name of the branch.
        """
        if not self.has_branch(branch):
            raise ValueError(
                f"Branch {branch} does not exist, so it cannot be updated."
            )
        new_data = self.SCHEMA(
            name=branch if new_name is None else new_name,
            points_to_collection_version=pointing_to_collection_version,
            points_to_branch=pointing_to_branch,
        ).__dict__

        self.find_one_and_replace(filter={'name': branch}, replacement=new_data)

    def get_branch(self, branch: str) -> SCHEMA:
        """Retrieve the branch information.

        :raises BranchNotFound: If no branch with the given name exists.
        :param branch: The branch for which the information should be retrieved.
        :return: The branch document.
        """
        branch_doc: Dict[str, Any] = self.find_one({'name': branch})
        if branch_doc is None:
            raise BranchNotFound(branch)
        branch_doc.pop('_id')
        return self.SCHEMA(**branch_doc)

    def get_empty_child_branches(
        self,
        branch: str,
        after_version: Optional[int] = None,
    ) -> List[SCHEMA]:
        """Return the empty branches pointing at `branch`.

        :param branch: The name of the parent branch.
        :param after_version: The version after which to retrieve empty
            branches, including the version itself. If ``None``, all empty
            branches for the given `branch` will be returned.
        :return: A list of branch data.
        """
        branches = list(self.find({'points_to_branch': branch}))
        if len(branches) == 1:
            # Branch `branch` has no children
            return []
        ret = []
        for b in branches:
            if b['name'] == branch:
                continue
            if (
                after_version is not None
                and b['pointing_to_collection_version'] < after_version
            ):
                continue

            b.pop('_id')
            ret.append(self.SCHEMA(**b))
        return ret

    def get_empty_branches(self) -> Set[BranchesCollection.SCHEMA]:
        """Return a set of empty branches data."""
        branches = list(
            self.find({"$expr": {"$ne": ['$name', '$points_to_branch']}})
        )
        branches_data = set()
        for b in branches:
            b.pop('_id')
            branches_data.add(self.SCHEMA(**b))
        return branches_data

    def delete_branches(self, branches: List[str]) -> None:
        """Delete the branches with names in the given list."""
        self.delete_many({'name': {"$in": branches}})

    def delete_branch(self, branch: str) -> None:
        """Delete the branch with the given name."""
        self.delete_one({'name': branch})
