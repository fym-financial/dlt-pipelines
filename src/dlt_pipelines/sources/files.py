"""Example file-based source."""

from __future__ import annotations

import csv
from collections.abc import Iterator
from pathlib import Path

import dlt

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CUSTOMERS_PATH = PROJECT_ROOT / "data" / "sample_customers.csv"


@dlt.resource(name="customers", write_disposition="replace")
def csv_customers(path: str | Path = DEFAULT_CUSTOMERS_PATH) -> Iterator[dict[str, str]]:
    """Yield customer rows from a CSV file."""
    with Path(path).open(newline="", encoding="utf-8") as csv_file:
        yield from csv.DictReader(csv_file)
