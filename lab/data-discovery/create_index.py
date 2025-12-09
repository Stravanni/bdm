import os
import sys
from pathlib import Path

from blend import BLEND


def main():
    assert len(sys.argv) == 2, "Usage is: python create_index.py <modena | undata>"

    data_name = sys.argv[1]

    assert data_name in ["modena", "undata"], (
        "Usage is: python create_index.py <modena | undata>"
    )

    data_path = Path(os.path.dirname(__file__), "data", data_name)

    # the path where the datasets to index are stored locally
    data_lake_path = data_path.joinpath("data-lake")

    # the path where the BLEND index will be stored
    index_database_path = data_path.joinpath("index_blend.db")

    # set up the BLEND index
    index = BLEND(index_database_path)

    # we index all the tables in the given folder, processing the tables
    # in parallel with 4 workers
    # during ingestion, each cell value is cast casted to string without
    # any other processing
    index.create_index(data_lake_path, max_workers=4, verbose=True)


if __name__ == "__main__":
    main()
