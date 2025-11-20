import random

import mmh3


class CuckooFilter:
    def __init__(
        self,
        capacity: int,
        fingerprint_length: int,
        max_kicks: int,
    ):
        self.capacity = capacity

        self.fingerprint_length = fingerprint_length

        # the maximum value for the fingerprint
        self.max_fingerprint_value = (1 << self.fingerprint_length) - 1

        # each bucket contains the specified number of entries,
        # and we can collapse these entries in a single bitarray
        # then we can check for each distinct bucket through bits
        # shifting
        self.buckets = [0 for _ in range(capacity)]

        self.max_kicks = max_kicks

    def _hash(self, item: str | int | bytes) -> int:
        if isinstance(item, int):
            item = item.to_bytes(length=8)
        return mmh3.hash(item)

    def fingerprint(self, item: str) -> int:
        # hash the item with our hash function
        digest = self._hash(item)

        # extract the fingerprint from the least significant bits,
        # through the AND operation with the max_fingerprint_value
        f = digest & self.max_fingerprint_value

        # return the fingerprint increased of 1, to avoid 0 values
        # which might be annoying in other operations
        f = f + 1 if f == 0 else f

        # return the fingerprint
        return f

    def is_empty(self, bucket_index: int) -> bool:
        return self.buckets[bucket_index] == 0

    def add_to_bucket(self, bucket_index: int, f: int):
        self.buckets[bucket_index] = f

    def get_fingerprint(self, bucket_index: int) -> int:
        return self.buckets[bucket_index]

    def swap(self, bucket_index: int, f: int) -> int:
        kicked_f = self.get_fingerprint(bucket_index)
        self.add_to_bucket(bucket_index, f)
        return kicked_f

    def clear_bucket(self, bucket_index: int):
        self.buckets[bucket_index] = 0

    def add(self, item: str) -> bool:
        f = self.fingerprint(item)
        hashed_signature = self._hash(f)
        i1 = self._hash(item) % self.capacity
        i2 = (i1 ^ hashed_signature) % self.capacity

        # if any of indexes i1 or i2 gives an empty bucket, place
        # the fingerprint of the item there
        if self.is_empty(i1):
            self.add_to_bucket(i1, f)
            return True
        elif self.is_empty(i2):
            self.add_to_bucket(i2, f)
            return True
        else:
            # otherwise, select a random index among i1 and i2
            i = random.choice([i1, i2])

            for _ in range(self.max_kicks):
                # swap current fingerprint with another random
                # from the chosed bucket,
                # Now f is the just kicked off fingerprint
                f = self.swap(i, f)

                # hash the kicked off fingerprint
                hashed_signature = self._hash(f) % self.capacity

                # compute a new bucket index
                i = (i ^ hashed_signature) % self.capacity

                # if the bucket at index i has an empty position,
                # place the kicked out fingerprint there, otherwise
                # repeat the loop and another fingerprint will be
                # kicked out
                if self.is_empty(i):
                    self.add_to_bucket(i, f)
                    return True

            # Hashtable is now full
            return False

    def check(self, item: str) -> bool:
        f = self.fingerprint(item)
        hashed_signature = self._hash(f) % self.capacity
        i1 = self._hash(item) % self.capacity
        i2 = (i1 ^ hashed_signature) % self.capacity

        if f == self.buckets[i1] or f == self.buckets[i2]:
            return True
        return False

    def delete(self, item: str):
        f = self.fingerprint(item)
        hashed_signature = self._hash(f) % self.capacity
        i1 = self._hash(item) % self.capacity
        i2 = (i1 ^ hashed_signature) % self.capacity

        if f == self.buckets[i1]:
            self.clear_bucket(i1)
            return True
        if f == self.buckets[i2]:
            self.clear_bucket(i2)
            return True
        return False


def main():
    cf = CuckooFilter(100, 8, 50)

    print(" INSERT ".center(30, "-"))
    for s in ["Hello", "World", "!"]:
        print("Add: ", s, cf.add(s))

    print(" CHECK ".center(30, "-"))
    for s in ["Hello", "Sunshine"]:
        print("Check: ", s, cf.check(s))

    print(" DELETE ".center(30, "-"))
    for s in [
        "Hello",
    ]:
        print("Delete: ", s, cf.delete(s))

    print(" CHECK ".center(30, "-"))
    for s in ["Hello", "Sunshine"]:
        print("Check: ", s, cf.check(s))


if __name__ == "__main__":
    main()
