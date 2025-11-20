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
        self.m = self.compute_m(n, p)

        # number of hash functions
        self.k = self.compute_k(n, self.m)

        # the hash functions used for BF operations
        #
        # You can use lambda-functions to define a list of
        # hash functions, in combination with mmh3.hash
        self.hash_functions = [
            lambda x, seed=i: mmh3.hash(x, seed=seed) for i in range(self.k)
        ]

        # bitarray (the core structure)
        self.bitarray = [0 for _ in range(self.m)]

    def compute_m(self, n: int, p: float) -> int:
        """
        Compute the size of the bitarray
        """
        return -ceil((n * log(p, e)) / (log(2, e) ** 2))

    def compute_k(self, n: int, m: int) -> int:
        """
        Compute the number of hash functions
        """
        return ceil((m * log(2, e)) / n)

    def add(self, item: str) -> bool:
        """
        Add the input item to the filter.
        """
        for h in self.hash_functions:
            position = h(item) % self.m
            self.bitarray[position] = 1

        return True

    def check(self, item: str) -> bool:
        """
        Check if the input item is already present into the filter
        """
        for h in self.hash_functions:
            position = h(item) % self.m
            if not self.bitarray[position]:
                return False
        return True

    def delete(self, item: str):
        """
        Delete the item from the filter
        """
        raise NotImplementedError("Bloom filter doesn't support delete operation!")
