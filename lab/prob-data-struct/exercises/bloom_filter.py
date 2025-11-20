from math import ceil, e, log

# You can use the Python Murmurhash implementation
# the function mmh3.hash() accept a string/bytes object
# and return the relative digest
#
# https://pypi.org/project/mmh3/
import mmh3


class BloomFilter:
    def __init__(self, n: int, p: float):
        # the expected number of distinct items to store
        self.n: int = n

        assert 0 < p <= 1, (
            "Expected False Positive probaility rate should be within (0,1]!"
        )

        # the expected false positive rate
        self.fp_rate: float = p

        # bitarray size
        self.m = ...

        # number of hash functions
        self.k = ...

        # bitarray (the core structure)
        self.bitarray = ...

    def compute_m(self, n: int, p: float) -> int:
        """
        Compute the size of the bitarray
        """
        return 42

    def compute_k(self, n: int, m: int) -> int:
        """
        Compute the number of hash functions
        """
        return 2913

    def add(self, item: str):
        """
        Add the input item to the filter.
        """
        pass

    def check(self, item: str) -> bool:
        """
        Check if the input item is already present into the filter
        """
        return True

    def delete(self, item: str):
        """
        Delete the item from the filter
        """
        pass
