# AVL Tree with random access.
#

class avltree(object):

        def __init__(self, lst=None):
                self.clear()
                if lst is not None:
                        self.extend(lst)


        def __len__(self):
                return self.root.size


        def __getitem__(self, index):
                if index < 0 or index >= len(self):
                        raise IndexError()
                return self.root.get_node_at(index).value

        def append(self, val):
                self.root = self.root.insert(val)


        def extend(self, lst):
                for val in lst:
                        self.append(val)

        def clear(self):
                self.root = avltree.Node.EMPTY_LEAF_NODE


        # Note: Not fail-fast on concurrent modification.
        def __iter__(self):
                stack = []
                node = self.root
                while node is not avltree.Node.EMPTY_LEAF_NODE:
                        stack.append(node)
                        node = node.left
                while len(stack) > 0:
                        node = stack.pop()
                        yield node.value
                        node = node.right
                        while node is not avltree.Node.EMPTY_LEAF_NODE:
                                stack.append(node)
                                node = node.left


        def __str__(self):
                return "[" + ", ".join(str(x) for x in self) + "]"


        class Node(object):

                def __init__(self, val, isleaf=False):
                        # The object stored at this node. Can be None.
                        self.value = val

                        if isleaf:  # For the singleton empty leaf node
                                self.height = 0
                                self.size   = 0
                                self.left  = None
                                self.right = None

                        else:  # Normal non-leaf nodes
                                # The height of the tree rooted at this node. Empty nodes have height 0.
                                # This node has height equal to max(left.height, right.height) + 1.
                                self.height = 1

                                # The number of nodes in the tree rooted at this node, including this node.
                                # Empty nodes have size 0. This node has size equal to left.size + right.size + 1.
                                self.size = 1

                                # The root node of the left subtree.
                                self.left  = avltree.Node.EMPTY_LEAF_NODE

                                # The root node of the right subtree.
                                self.right = avltree.Node.EMPTY_LEAF_NODE


                def get_node_at(self, index):
                        assert 0 <= index < self.size
                        if self is avltree.Node.EMPTY_LEAF_NODE:
                                raise ValueError()

                        leftsize = self.left.size
                        if index < leftsize:
                                return self.left.get_node_at(index)
                        elif index > leftsize:
                                return self.right.get_node_at(index - leftsize - 1)
                        else:
                                return self


                def insert(self, obj):
                        if self is avltree.Node.EMPTY_LEAF_NODE:
                            return avltree.Node(obj)

                        if obj <= self.value:
                                self.left = self.left.insert(obj)
                        else:
                                self.right = self.right.insert(obj)
                        self._recalculate()
                        return self._balance()

                def __str__(self):
                        return "avltreeNode(size={}, height={}, val={})".format(self.size, self.height, self.value)


                def _get_successor(self):
                        if self is avltree.Node.EMPTY_LEAF_NODE or self.right is avltree.Node.EMPTY_LEAF_NODE:
                                raise ValueError()
                        node = self.right
                        while node.left is not avltree.Node.EMPTY_LEAF_NODE:
                                node = node.left
                        return node.value


                # Balances the subtree rooted at this node and returns the new root.
                def _balance(self):
                        bal = self._get_balance()
                        assert abs(bal) <= 2
                        result = self
                        if bal == -2:
                                assert abs(self.left._get_balance()) <= 1
                                if self.left._get_balance() == +1:
                                        self.left = self.left._rotate_left()
                                result = self._rotate_right()
                        elif bal == +2:
                                assert abs(self.right._get_balance()) <= 1
                                if self.right._get_balance() == -1:
                                        self.right = self.right._rotate_right()
                                result = self._rotate_left()
                        assert abs(result._get_balance()) <= 1
                        return result

                def _rotate_left(self):
                        if self.right is avltree.Node.EMPTY_LEAF_NODE:
                                raise ValueError()
                        root = self.right
                        self.right = root.left
                        root.left = self
                        self._recalculate()
                        root._recalculate()
                        return root

                def _rotate_right(self):
                        if self.left is avltree.Node.EMPTY_LEAF_NODE:
                                raise ValueError()
                        root = self.left
                        self.left = root.right
                        root.right = self
                        self._recalculate()
                        root._recalculate()
                        return root


                # Needs to be called every time the left or right subtree is changed.
                # Assumes the left and right subtrees have the correct values computed already.
                def _recalculate(self):
                        assert self is not avltree.Node.EMPTY_LEAF_NODE
                        assert self.left.height >= 0 and self.right.height >= 0
                        assert self.left.size >= 0 and self.right.size >= 0
                        self.height = max(self.left.height, self.right.height) + 1
                        self.size = self.left.size + self.right.size + 1
                        assert self.height >= 0 and self.size >= 0


                def _get_balance(self):
                        return self.right.height - self.left.height


# Static initializer. A bit of a hack, but more elegant than using None values as leaf nodes.
avltree.Node.EMPTY_LEAF_NODE = avltree.Node(None, True)
