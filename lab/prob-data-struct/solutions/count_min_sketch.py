# You can use the Python Murmurhash implementation
# the function mmh3.hash() accept a string/bytes object
# and return the relative digest
#
# With the parameter `seed` you can define a different
# consistent output from the default one: mmh3.hash(x, seed=seed)
#
# https://pypi.org/project/mmh3/
import mmh3


class CountMinSketch:
    def __init__(self, width: int, depth: int):
        self.width = width
        self.depth = depth

        # the hash functions used for BF operations
        #
        # You can use lambda-functions to define a list of
        # hash functions, in combination with mmh3.hash
        self.hash_functions = [
            lambda x, seed=i: mmh3.hash(x, seed=seed) for i in range(depth)
        ]

        self.table = [[0 for _ in range(self.width)] for _ in range(self.depth)]

    def add(self, item: str):
        """
        Add an item to the sketch, updating
        the table with the hash function values.
        """
        for i in range(self.depth):
            # we access the i-th row of the table and compute with
            # the i-th hash function, modulo the table width, the
            # position we need to update
            index = self.hash_functions[i](item) % self.width
            self.table[i][index] += 1

    def check(self, item: str) -> int:
        """
        Return the estimated count for the given item
        """
        values = []

        for i in range(self.depth):
            index = self.hash_functions[i](item) % self.width
            values.append(self.table[i][index])

        return min(values)

    def delete(self, item: str):
        """
        Delete the item from the sketch
        """
        raise NotImplementedError("Delete is not possible in CMS!")
