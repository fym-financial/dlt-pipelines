"""Pipeline factory and source dispatch."""

from __future__ import annotations

from collections.abc import Callable

import dlt

from dlt_pipelines.sources.api import heartland_inforced_policies, jsonplaceholder_posts
from dlt_pipelines.sources.files import csv_customers
from dlt_pipelines.sources.s3 import landed_csv_files

SourceFactory = Callable[[str | None], object]

SOURCE_FACTORIES: dict[str, SourceFactory] = {
    "api": lambda provider=None: jsonplaceholder_posts(),
    "csv": lambda provider=None: csv_customers(),
    "heartland": lambda provider=None: heartland_inforced_policies(),
    "s3": landed_csv_files,
}
SOURCE_CHOICES = tuple(sorted(SOURCE_FACTORIES))


def build_pipeline(
    *,
    pipeline_name: str = "dlt_pipelines",
    dataset_name: str = "raw",
) -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="postgres",
        dataset_name=dataset_name,
    )


def run_pipeline(
    *,
    source_name: str,
    provider: str | None = None,
    dataset_name: str = "raw",
    pipeline_name: str = "dlt_pipelines",
) -> object:
    try:
        source_factory = SOURCE_FACTORIES[source_name]
    except KeyError as exc:
        available = ", ".join(SOURCE_CHOICES)
        raise ValueError(f"Unknown source '{source_name}'. Use one of: {available}") from exc

    pipeline = build_pipeline(
        pipeline_name=pipeline_name,
        dataset_name=dataset_name,
    )
    return pipeline.run(source_factory(provider))
