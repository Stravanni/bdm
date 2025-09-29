import random
from typing import List, Optional

from utils import display_skip_list

# for replicability
random.seed(130399)


class SkipListNode:
    def __init__(self, item: int, level: int = 0) -> None:
        self.item = item
        self.level = level
        self.forward: List[Optional["SkipListNode"]] = [None] * (level + 1)

    def __str__(self) -> str:
        return "[" + f"{self.item}".rjust(2) + "]"


class SkipList:
    def __init__(self, p: float = 0.25, max_level: int = 4) -> None:
        self.p = p
        self.max_level = max_level
        self.level = 0
        self._header = SkipListNode(None, max_level)

    def _random_level(self) -> int:
        level = 0
        while random.random() < self.p and level < self.max_level:
            level += 1

        return level

    def _compare_keys(self, k1: int, k2: int) -> int:
        if k1 is None or k2 is None:
            return -1 if k1 is None else 1
        return (k1 > k2) - (k1 < k2)

    def search(self, q) -> Optional[int]:
        current = self._header

        # Starting from the highest level, we go down
        # to the bottom on the same node (vertical move)
        for level in range(self.max_level, -1, -1):
            # current.forward[level] gives us the access to the
            # next node at the same current level (horizontal move)
            while (
                current.forward[level] is not None
                and self._compare_keys(current.forward[level].item, q) < 0
            ):
                current = current.forward[level]

        current = current.forward[0]

        if current is not None and self._compare_keys(current.item, q) == 0:
            return current.item
        return None

    def insert(self, q: int) -> None:
        current = self._header

        # at each level we will need to update
        # a node, and here we keep track of which
        # nodes must be updated in the end
        update = [None] * (self.max_level + 1)

        # the current level is not set to self.max_level,
        # since a less number of layers might be used at this stage
        for level in range(self.level, -1, -1):
            while (
                current.forward[level] is not None
                and self._compare_keys(current.forward[level].item, q) < 0
            ):
                current = current.forward[level]
            update[level] = current

        if current is not None and self._compare_keys(current.item, q) == 0:
            return

        new_level = self._random_level()
        new_node = SkipListNode(q, new_level)

        # if we added a new layer, we have to update the
        # list level (e.g. if any node before was at layer 3,
        # and this new one reaches 4, we have to update other
        # nodes properly)
        if new_level > self.level:
            for level in range(self.level + 1, new_level + 1):
                update[level] = self._header
            self.level = new_level

        # update the previous node:
        # from the level 0 (lowest) to the new level, its
        # successor is the new node now
        for level in range(new_level + 1):
            new_node.forward[level] = update[level].forward[level]
            update[level].forward[level] = new_node


if __name__ == "__main__":
    s = SkipList(p=0.75, max_level=5)

    for _ in range(10):
        s.insert(random.randrange(0, 100))

    display_skip_list(s)
