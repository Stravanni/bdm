import math
import random
import statistics
from typing import Any, List

from collections import defaultdict


# A better solution is to define this function as
# self.__str__ or as a display() function on the
# class SkipList itself
def display_skip_list(skip_list):
    layers = []
    current = skip_list._header.forward[0]

    # get all the low level items
    while current is not None:
        layers.append(
            [str(current)] * (current.level + 1)
            + [None] * (skip_list.level - current.level - 1)
        )

        current = current.forward[0]

    layers = list(zip(*layers))

    for level in range(skip_list.level - 1, -1, -1):
        print(f"#{level}#--", end="")
        if layers[level][0] is not None:
            print(">", end="")
        else:
            print("-", end="")
        for i, node in enumerate(layers[level]):
            if node is not None:
                print(node, end="")
            else:
                print("----", end="")

            if i == len(layers[level]) - 1:
                print("-->[]", end="")
                continue

            if layers[level][i + 1] is not None:
                print("-->", end="")
            else:
                print("---", end="")
        print()


def get_ground_truth(q: Any, nodes: List[Any], k):
    nodes = [n for n in nodes if n != q]
    return sorted(sorted(nodes, key=lambda x: abs(q - x))[:k])


def recall(ground_truth: List[Any], results: List[Any], k: int):
    return len(set(ground_truth) & set(results)) / k


def hnsw_experiments():
    """
    Run some experiments with HNSW
    Show if all nodes are effectively connected and recall scores
    """
    try:
        from tabulate import tabulate
    except ModuleNotFoundError:
        print("Module tabulate not found.")
        return

    try:
        from tqdm import tqdm
    except ModuleNotFoundError:
        print("Module tqdm not found.")
        return

    # import at this level avoid circular import error
    from hnsw import HNSW, GraphNode

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

    all_nodes = []
    for q in tqdm(
        random.sample(range(0, MAX_RANGE), INDEX_SIZE),
        desc="Insertion: ",
        total=INDEX_SIZE,
    ):
        hsnw.insert(q)
        all_nodes.append(q)

    all_nodes.sort()

    visited = hsnw.traverse_hnsw_graph()
    print(f"#connected nodes: {len(visited)}")

    # this will increase the number of nearest neighbors
    # candidates considered during the search, as a multiple
    # of the K parameter
    ef_search = 16

    k_values = [1, 3, 5, 10]
    results = defaultdict(list)

    for k in tqdm(k_values, desc="Query: "):
        for q in tqdm(
            random.sample(range(0, MAX_RANGE), NUM_QUERIES),
            desc=f"K = {k}: ",
            leave=False,
            position=2,
        ):
            knn = hsnw.knn(GraphNode(q), k, ef=ef_search)
            gt = get_ground_truth(q, all_nodes, k)

            r = recall(gt, knn, k)

            results[k].append(r)

    stats = []

    for k, scores in results.items():
        mean = statistics.mean(scores)
        stdev = statistics.stdev(scores)
        median = statistics.median(scores)

        stats.append((k, mean, stdev, median, min(scores), max(scores)))

    print("Recall@K results")
    print(
        tabulate(stats, headers=["K", "mean", "stdev", "mean", "min", "max"]), end="\n"
    )


if __name__ == "__main__":
    hnsw_experiments()
