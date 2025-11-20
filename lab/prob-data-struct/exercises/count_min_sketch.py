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

        # the hash functions to encode the items
        # each hash function should return an integer.
        # For instance, h: str -> int
        #
        # You can use lambda-functions to define a list of
        # hash functions, in combination with mmh3.hash
        self.hash_functions = ...

        # the core structure
        self.table = ...

    def add(self, item: str):
        """
        Add an item to the sketch, updating
        the table with the hash function values.
        """
        pass

    def check(self, item: str) -> int:
        """
        Return the estimated count for the given item
        """
        return 34

    def delete(self, item: str):
        """
        Delete the item from the sketch
        """
        pass
