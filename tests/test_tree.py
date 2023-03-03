import random
from typing import Dict
from unittest.mock import Mock

from versioned_collection.tree import Tree

from unittest import TestCase


class _Data(dict):
    def __init__(self, seq=None, **kwargs):
        if seq is not None:
            super().__init__(seq, **kwargs)
        else:
            super().__init__(**kwargs)
        self.next = Mock()


class TestTreeComparison(TestCase):

    @staticmethod
    def _generate_data() -> Dict[int, int]:
        d = _Data()
        _max = 9999
        for i in range(random.randint(0, 5)):
            d[random.randint(-_max, _max)] = random.randint(-_max, _max)
        return d

    def test_tree_equals_self(self):
        tree = Tree()
        tree.create_node(1, 1, data=self._generate_data())
        self.assertEqual(tree, tree)
        self.assertTrue(tree <= tree)
        self.assertFalse(tree < tree)

    def test_tree_not_equals_none(self):
        self.assertNotEqual(Tree(), None)

    def test_different_length_trees(self):
        t1 = Tree()
        t1.create_node(1, 1, data=self._generate_data())
        t2 = Tree(tree=t1, deep=True)
        t2.create_node(2, 2, parent=t2.root, data=self._generate_data())
        t2.create_node(3, 3, parent=t2.root, data=self._generate_data())
        self.assertNotEqual(t1, t2)

        self.assertTrue(t1 < t2)
        self.assertTrue(t1 <= t2)

        self.assertFalse(t2 < t1)
        self.assertFalse(t2 <= t1)

    def test_tree_equality(self):
        data = self._generate_data()
        t1 = Tree()
        t1.create_node(1, 1, data=data)
        t2 = Tree()
        t2.create_node(1, 1, data=data)

        self.assertEqual(t1, t2)

        self.assertFalse(t1 < t2)
        self.assertFalse(t2 < t1)

        self.assertFalse(t1 > t2)
        self.assertFalse(t2 > t1)

        self.assertTrue(t1 <= t2)
        self.assertTrue(t2 <= t1)

        self.assertTrue(t1 >= t2)
        self.assertTrue(t2 >= t1)

    def test_trees_not_equal(self):
        t1 = Tree()
        t1.create_node(1, 1, data=self._generate_data())
        b1 = t1.create_node(2, 2, data=self._generate_data(), parent=t1.root)
        b2 = t1.create_node(3, 3, data=self._generate_data(), parent=t1.root)

        t2 = Tree(tree=t1, deep=True)

        t2.create_node(4, 4, data=self._generate_data(), parent=b1.identifier)
        t1.create_node(5, 5, data=self._generate_data(), parent=b2.identifier)

        self.assertFalse(t1 < t2)
        self.assertFalse(t1 <= t2)

        self.assertTrue(t1 != t2)
