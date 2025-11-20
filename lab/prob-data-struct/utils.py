import os
import random
import statistics
from pathlib import Path
from typing import Tuple

import polars as pl
from solutions.bloom_filter import BloomFilter
from solutions.count_min_sketch import CountMinSketch
from solutions.cuckoo_filter import CuckooFilter
from tabulate import tabulate

SEED = 1  # round(time())
pad_size = 110
random.seed(SEED)


def load_dataset(dataset_path: Path) -> Tuple[list, list, set]:
    df = pl.read_csv(dataset_path)  # .limit(10000)
    print(" URL Dataset head ".center(pad_size, "="))
    print(df.head())

    print(" URL Dataset description ".center(pad_size, "="))
    print(df.select("label", "result").describe())

    df = df.get_column("url")

    insert_fraction = 0.7
    query_fraction = 0.1

    insert_urls = df.sample(fraction=insert_fraction, seed=SEED).to_list()
    query_urls = df.sample(fraction=query_fraction, seed=SEED * 2).to_list()

    containment_ground_truth = set(insert_urls)

    return insert_urls, query_urls, containment_ground_truth


def test_filter(
    filter: BloomFilter | CuckooFilter,
    insert_urls: list,
    query_urls: list,
    ground_truth: set,
):
    queries_in_gt = len(ground_truth.intersection(query_urls))
    max_fp = len(query_urls) - queries_in_gt
    max_fp_rate = round(max_fp / len(query_urls), 5)

    # add URLs to the filter
    failed_inserts = 0
    for url in insert_urls:
        failed_inserts += not filter.add(url)

    fp = 0

    for url in query_urls:
        pred = filter.check(url)
        gt = url in ground_truth

        if pred and not gt:
            fp += 1

    fp_rate = round(fp / len(query_urls), 10)
    return (failed_inserts, fp_rate, fp, queries_in_gt, max_fp, max_fp_rate)


def test_cms(
    cms: CountMinSketch, insert_urls: list[str], query_urls: list[str], n: int
) -> tuple[float, float, float, float, float, float, float]:
    """
    We generate relative weight for each URL in the input list, in order to
    have elements more likely to be sampled than others. Then, we compare the
    counts of the sketch with a basic dict of counts, to check how much the
    expected counts diverges against the actual ones.
    The parameter n specify how many URLs we want to insert into the sketch.
    """

    weights = random.choices(range(100), k=len(insert_urls))
    total_weight = sum(weights)
    weights = [w / total_weight for w in weights]  # we normalize the weights

    sampled_urls = random.choices(insert_urls, weights, k=n)
    ground_truth: dict[str, int] = {}

    for url in sampled_urls:
        cms.add(url)

        if url in ground_truth:
            ground_truth[url] += 1
        else:
            ground_truth[url] = 1

    results = []

    for url in query_urls:
        pred = cms.check(url)
        gt = ground_truth[url] if url in ground_truth else 0
        results.append(pred - gt)  # CMS at most can overestimate the actual count

    return (
        statistics.mean(results),
        statistics.stdev(results),
        *statistics.quantiles(results),
    )


def main():
    """
    You can test also for time/space, but with these basic Python implementation
    such tests are not really useful
    """
    dataset_path = Path(os.path.dirname(__file__), "data", "urls.csv")
    assert dataset_path.exists(), (
        "You have to download the balanced_urls.csv dataset, or correct the given path!"
    )

    insert_urls, query_urls, containment_ground_truth = load_dataset(dataset_path)

    stats = []

    for n in [10_000, 100_000, 1_000_000]:
        for expected_fp_rate in [0.1, 0.01, 0.001]:
            bf = BloomFilter(n, expected_fp_rate)

            stats.append(
                [
                    n,
                    expected_fp_rate,
                    *test_filter(bf, insert_urls, query_urls, containment_ground_truth),
                ]
            )

    print(" TESTING BLOOM FILTER ".center(pad_size, "="))
    print(
        tabulate(
            stats,
            [
                "N",
                "FP-rate (expected)",
                "Insert Fails",
                "FP-rate",
                "FP",
                "Q U GT",
                "FP (max)",
                "FP-rate (max)",
            ],
        )
    )

    print("\n" + " TESTING COUNT MIN SKETCH ".center(pad_size, "="))

    stats = []
    n_urls = 10_000
    n_to_insert = 100_000
    for d in [3, 5, 10]:
        for w in [100, 1_000, 10_000]:
            cms = CountMinSketch(w, d)

            stats.append(
                [
                    d,
                    w,
                    *test_cms(
                        cms, insert_urls[:n_urls], query_urls[:n_urls], n=n_to_insert
                    ),
                ]
            )

    print(tabulate(stats, ["depth", "width", "mean", "stdev", "q1", "q2", "q3"]))


if __name__ == "__main__":
    main()
