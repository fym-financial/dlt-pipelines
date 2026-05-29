"""Example REST API source."""

from __future__ import annotations

import json
from typing import Any
from urllib.request import urlopen

import dlt


@dlt.resource(name="posts", write_disposition="replace")
def jsonplaceholder_posts(limit: int | None = None) -> list[dict[str, Any]]:
    """Fetch a small public API dataset.

    Replace the URL and response parsing with the real source API logic.
    """
    url = "https://jsonplaceholder.typicode.com/posts"
    with urlopen(url, timeout=30) as response:
        rows = json.load(response)

    if limit is not None:
        return rows[:limit]
    return rows
