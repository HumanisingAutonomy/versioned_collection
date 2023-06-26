"""Helper datastructures.

These extend the ``treelib`` library ``Node`` and ``Tree`` classes by adding
comparisons and equality functionalities used to compare log trees.
"""

from treelib import Node as _Node
from treelib import Tree as _Tree

from versioned_collection.utils.data_structures import hashabledict


class Node(_Node):

    def __hash__(self) -> int:
        to_hash = [self.identifier, self.tag]
        if self.data is not None:
            data = self.data
            if isinstance(self.data, dict):
                data = hashabledict(data)
            to_hash.append(data)
        return hash(tuple(to_hash))

    def __eq__(self, other: object) -> bool:
        if other is None:
            return False
        if not (isinstance(other, Node) or isinstance(other, _Node)):
            return False
        if other is self:
            return True
        return (
            self.tag == other.tag
            and self.identifier == other.identifier
            and self.data == other.data
            and self.predecessor(self.identifier)
            == other.predecessor(self.identifier)
            and set(self.successors(self.identifier))
            == set(other.successors(self.identifier))
        )


class Tree(_Tree):

    def __init__(self, tree=None, deep=False, node_class=None, identifier=None):
        if node_class is None:
            node_class = Node
        super().__init__(tree, deep, node_class, identifier)

    def __hash__(self) -> int:
        root: Node = self.get_node(self.root)
        _hash = ""

        to_visit = [root]
        while len(to_visit):
            node = to_visit.pop(0)

            leading = "0" if node.is_leaf(self._identifier) else "1"
            _hash += leading + str(hash(node))

            to_visit.extend(sorted(self.children(node.identifier)))

        return hash(_hash)

    def __eq__(self, other: object) -> bool:
        if not (isinstance(other, Tree) or isinstance(other, _Tree)):
            return False
        if other is self:
            return True
        # Different number of nodes
        if len(self) != len(other):
            return False

        to_visit_here = [self.get_node(self.root)]
        to_visit_there = [other.get_node(other.root)]
        while len(to_visit_here) > 0:
            if len(to_visit_here) != len(to_visit_there):
                return False
            this_node: Node = to_visit_here.pop(0)
            that_node: Node = to_visit_there.pop(0)

            # The nodes should be the same
            if (
                this_node.identifier != that_node.identifier
                or this_node.tag != that_node.tag
                or this_node.data != that_node.data
            ):
                return False

            # The nodes should have the same number of children
            this_node_children = self.children(this_node.identifier)
            that_node_children = other.children(that_node.identifier)
            if len(this_node_children) != len(that_node_children):
                return False

            # The children nodes should be the same
            to_visit_here.extend(sorted(this_node_children))
            to_visit_there.extend(sorted(that_node_children))

        return True

    def _is_subtree_of(self, other: object, strict: bool = True) -> bool:
        if not (isinstance(other, Tree) or isinstance(other, _Tree)):
            op_str = '<' if strict else '<='
            raise TypeError(
                f"{op_str} not supported between instances of "
                f"'Tree' and '{type(other)}'"
            )

        if other is self:
            return not strict

        # Different number of nodes
        if len(self) > len(other):
            return False

        these_paths_to_leaves = self.paths_to_leaves()
        if len(these_paths_to_leaves) > len(other.paths_to_leaves()):
            return False

        # `self` has either the same or a lower number of branches as `other`
        these_leaves = {p[-1] for p in these_paths_to_leaves}

        # All leaves of `self` are nodes in `other`
        for this_leaf in these_leaves:
            if not other.contains(this_leaf):
                return False

        trimmed = False
        other_copy = Tree(tree=other, deep=True)
        for leaf in these_leaves:
            other_node: Node = other_copy.get_node(leaf)
            # Other node should exist because otherwise we should have
            # returned ``False`` above
            assert other_node is not None

            if other_node.is_leaf(other_copy.identifier):
                # Nothing to trim
                continue

            for other_child in other_copy.children(other_node.identifier):
                other_copy.remove_subtree(other_child.identifier)
                if other_node.data is not None:
                    # This is ugly because it makes assumption about the data.
                    # Only valid for log trees, but it's the only use case
                    # anyway
                    other_node.data.next.remove(other_child.tag)
                trimmed = True

        # `other_copy` is trimmed to the length of `self` for all branches of
        # `self` that are also branches of `other`. We still have to remove
        # those branches that are only in `other`.
        removed_branches = False
        for path in other_copy.paths_to_leaves():
            if path[-1] in these_leaves:
                # this has already been trimmed
                continue
            for other_node_id in path:
                if not self.contains(other_node_id):
                    other_copy.remove_subtree(other_node_id)

                    removed_branches = True
                    break

        # `other_copy` is trimmed to the length of `self`. At this point,
        # `self` is a subtree of `other` if `self` equals `other_copy`
        if self != other_copy:
            return False

        if strict:
            return trimmed or removed_branches
        return True

    def __lt__(self, other: object) -> bool:
        return self._is_subtree_of(other, strict=True)

    def __le__(self, other: object) -> bool:
        return self._is_subtree_of(other, strict=False)
