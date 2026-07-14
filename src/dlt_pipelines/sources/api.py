"""Example REST API source."""

from __future__ import annotations

import json
from typing import Any
from urllib.request import Request, urlopen

import dlt

from dlt_pipelines.config import get_setting


HEARTLAND_POLICY_FIELDS = (
    "polNo",
    "agtCode",
    "agtFirstName",
    "agtLastName",
    "amrStatus",
    "appDate",
    "effDate",
    "birthDate",
    "premium",
    "plan",
    "productDesc",
    "type",
    "issueState",
    "entryDate",
    "firstName",
    "lastName",
    "share",
    "initialPaidDate",
    "clientAddress",
    "clientAddress2",
    "clientCity",
    "clientState",
    "clientZip",
    "clientEmail",
    "clientPhone",
    "paidToDate",
    "draftDay",
    "hnlStatus",
    "returnDescripton",
    "chargeBackDt",
    "upline",
    "issAge",
    "endDate",
    "appGuid",
    "appType",
    "writingSplit",
)
HEARTLAND_TEXT_COLUMNS = {
    field: {"data_type": "text", "nullable": True} for field in HEARTLAND_POLICY_FIELDS
}


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


@dlt.resource(
    name="heartland_inforced_policy_snapshot",
    write_disposition="replace",
    columns=HEARTLAND_TEXT_COLUMNS,
)
def heartland_inforced_policies(
    *,
    username: str | None = None,
    password: str | None = None,
    base_url: str | None = None,
) -> list[dict[str, str | None]]:
    """Fetch the current Heartland inforced-policy snapshot.

    The source API returns all business values as strings. Explicit DLT column
    hints preserve that contract in the raw schema instead of inferring numeric
    or date types from a particular response.
    """
    username = username or _heartland_setting("username", default="FYMUser")
    password = password or _heartland_setting("password", required=True)
    base_url = (base_url or _heartland_setting(
        "base_url",
        default="https://api.hnlicagent.com",
    )).rstrip("/")

    login_body = json.dumps({"Username": username, "Password": password}).encode()
    login_request = Request(
        f"{base_url}/api/auth/login",
        data=login_body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(login_request, timeout=30) as response:
        token = response.read().decode().strip().strip('"')
    if not token:
        raise RuntimeError("Heartland login returned an empty token.")

    policy_request = Request(
        f"{base_url}/api/FYM/GetPolicies",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        },
        method="GET",
    )
    with urlopen(policy_request, timeout=60) as response:
        rows = json.load(response)

    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise RuntimeError("Heartland policy response must be a JSON array of objects.")

    return [
        {
            field: None if row.get(field) is None else str(row[field])
            for field in HEARTLAND_POLICY_FIELDS
        }
        for row in rows
    ]


def _heartland_setting(
    name: str,
    *,
    default: str | None = None,
    required: bool = False,
) -> str:
    value = get_setting(
        f"HEARTLAND_API_{name.upper()}",
        ("sources", "heartland_api", name),
        default=default,
        required=required,
    )
    if value is None:
        raise RuntimeError(f"Missing Heartland API setting: {name}")
    return value
