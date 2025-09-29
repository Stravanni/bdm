import math
import random
from collections import deque
from typing import Any, Callable, List, Optional, Set

# for replicability
random.seed(130397)


class GraphNode:
    def __init__(self, item: Optional[Any], level: int = 0) -> None:
        self.item: Optional[Any] = item
        self.level: int = level

        # In this way we do not consider a proper
        # semantic for incoming/outgoing edges on the node
        self.neighbors: List[Set[Any]] = [set() for _ in range(level + 1)]

    def __hash__(self) -> int:
        return hash(self.item)


def basic_distance(a: GraphNode, b: GraphNode):
    if a.item is None or b.item is None:
        return float("inf")
    return abs(a.item - b.item)


class HNSW:
    def __init__(
        self,
        max_connections: int,
        max_connections_lowest: int,
        ef_construction: int,
        normalization_factor: float,
        distance: Callable = basic_distance,
        verbose: bool = False,
    ) -> None:
        self.max_connections = max_connections
        self.max_connections_lowest = max_connections_lowest
        self.ef_construction = ef_construction
        self.normalization_factor = normalization_factor
        self.distance = distance
        self.verbose = verbose

        self.level = 0

        # a different solution might involve to keep
        # all the nodes in some data structure like a set
        # or a dictionary of sets
        #
        # self._layers = defaultdict(set)

        # entrance point
        self.ep: Optional[GraphNode] = GraphNode(None, self.level)

    def _random_level(self):
        return math.floor(
            -math.log(random.random(), math.e) * self.normalization_factor
        )

    def _nearest(self, q: GraphNode, points: Set[GraphNode]) -> GraphNode:
        return min(points, key=lambda w: self.distance(q, w))

    def _furthest(self, q: GraphNode, points: Set[GraphNode]) -> GraphNode:
        return max(points, key=lambda w: self.distance(q, w))

    def insert(self, q: Any):
        W = set()

        layer_i = self._random_level()
        L = self.level
        q = GraphNode(q, layer_i)

        ep = {self.ep}

        # first phase: descend down to the last layer
        # where the new item is not present
        for lc in range(L, min(L, layer_i), -1):
            W = self.search_layer(q, ep, lc, ef=1)
            ep = {self._nearest(q, W)}

        # Now, we have to add the node on each layer
        # to the lowest one
        for lc in range(min(L, layer_i), -1, -1):
            if self.ep.item is None:
                continue

            # select the number of connections
            # typically at lowest level more connections are allowed
            M = self.max_connections if lc > 0 else self.max_connections_lowest

            W = self.search_layer(q, ep, lc, ef=self.ef_construction)

            if not W:
                continue

            neighbors = self.select_neighbors_simple(q=q, candidates=W, k=M)
            assert len(neighbors) <= M

            # Update the connections, for the new node
            # and for its neighbors too
            q.neighbors[lc] = neighbors
            for e in neighbors:
                e.neighbors[lc].add(q)

                # If now a node has more than ef_construction
                # links, we have to shrink them
                if len(e.neighbors[lc]) > M:
                    e.neighbors[lc] = self.select_neighbors_simple(
                        q=e, candidates=e.neighbors[lc], k=M
                    )
                    assert len(e.neighbors[lc]) <= M

                    # It's possible that by shrinking this node's neighbors,
                    # the new node we were trying to connect will be discarded.
                    # In general, if the number of neighbors is large enough,
                    # this won't be a problem and other nodes will link to it eventually
                    #
                    # assert q in e.neighbors[lc]

            ep = W

        if ep is None or layer_i > L:
            self.level = layer_i
            self.ep = q
            if self.verbose:
                print(self.ep.neighbors)
                print(f"Update: ep-->{self.ep}, current level-->{self.level}")

    def select_neighbors_simple(
        self, q: GraphNode, candidates: Set[GraphNode], k: int, *args
    ) -> Set[GraphNode]:
        """
        The simplest selection method
        """
        candidates = {c for c in candidates if c.item != q.item}

        return set(sorted(candidates, key=lambda c: self.distance(q, c))[:k])

    def search_layer(self, q: GraphNode, ep: Set[GraphNode], level: int, ef: int):
        """
        Searches for nearest neighboors to the query on the specified layer.
        """
        visited: Set[GraphNode] = set(ep)
        candidates: Set[GraphNode] = set(ep)
        nearest_n: Set[GraphNode] = set(ep)

        while candidates:
            nearest = self._nearest(q, candidates)
            candidates.remove(nearest)

            furthest = self._furthest(q, nearest_n)

            if self.distance(q, nearest) > self.distance(q, furthest):
                break

            for e in nearest.neighbors[level]:
                if e not in visited:
                    visited.add(e)

                    furthest = self._furthest(q, nearest_n)

                    if (
                        self.distance(q, e) < self.distance(q, furthest)
                        or len(nearest_n) < ef
                    ):
                        candidates.add(e)
                        nearest_n.add(e)

                        if len(nearest_n) > ef:
                            nearest_n.remove(self._furthest(q, nearest_n))

        return nearest_n

    def knn(self, q: GraphNode, k: int, ef: int) -> List[Any]:
        W = set()

        ep = self.ep

        for lc in range(self.level, 0, -1):
            W = self.search_layer(q, {ep}, lc, ef=1)
            ep = self._nearest(q, W)

        W = self.search_layer(q, {ep}, level=0, ef=ef)
        results = self.select_neighbors_simple(q, W, k)
        results = [n.item for n in results]
        return results

    def traverse_hnsw_graph(self) -> List[GraphNode]:
        """
        Traverse the entire HNSW graph from a given entry point using BFS.
        """
        visited = set()
        queue = deque([self.ep])
        traversal_order = []

        while queue:
            current_node = queue.popleft()

            # Skip if already visited
            if current_node.item in visited:
                continue

            # Mark as visited and add to traversal order
            visited.add(current_node.item)
            traversal_order.append(current_node)

            # Explore neighbors at all layers of the current node
            for layer in range(len(current_node.neighbors)):
                for neighbor in current_node.neighbors[layer]:
                    if neighbor not in visited:
                        queue.append(neighbor)

        return traversal_order


if __name__ == "__main__":
    # max number of neighbors per layer
    M = 10

    # max number of neighbors in the lowest layer
    M_0 = M * 2

    # normalization factor
    mL = 1 / math.log(M, math.e)

    ef_construction = 16

    hsnw = HNSW(
        max_connections=M,
        max_connections_lowest=M_0,
        ef_construction=ef_construction,
        normalization_factor=mL,
        verbose=False,
    )

    MAX_RANGE = int(1e9)
    INDEX_SIZE = int(1e3)
    NUM_QUERIES = 100

    for q in random.sample(range(0, MAX_RANGE), INDEX_SIZE):
        hsnw.insert(q)

    visited = hsnw.traverse_hnsw_graph()
    print(f"#connected nodes: {len(visited)}")

    # this will increase the number of nearest neighbors
    # candidates considered during the search, as a multiple
    # of the K parameter
    ef_search = 16

    # query item
    q = GraphNode(item=30)

    # number of results to return
    k = 10

    knn = hsnw.knn(q, k, ef=ef_search)
