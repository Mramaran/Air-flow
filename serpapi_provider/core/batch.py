"""Batch validation and the wait loop for a batch of asynchronous searches. No Airflow imports."""

from __future__ import annotations

import asyncio
from typing import Any, Protocol

from serpapi_provider.core.client import is_failed, is_finished, status_of
from serpapi_provider.core.errors import SerpApiAuthError, SerpApiError

MAX_CONSECUTIVE_ERRORS = 5


def validate_batch(searches: Any, max_batch_size: int = 50) -> list[dict[str, Any]]:
    """Check a batch before any request is sent. Returns the searches as a list of dicts."""
    if not isinstance(searches, (list, tuple)) or not searches:
        raise ValueError("'searches' must be a non-empty list of SerpApi parameter dictionaries.")
    if len(searches) > max_batch_size:
        raise ValueError(
            f"This batch has {len(searches)} searches and the limit is {max_batch_size}. "
            "Split it, or raise max_batch_size if the account's hourly limit allows."
        )
    checked: list[dict[str, Any]] = []
    for index, search in enumerate(searches):
        if not isinstance(search, dict):
            raise ValueError(f"Search {index} is not a dictionary of SerpApi parameters.")
        if not search.get("engine"):
            raise ValueError(f"Search {index} has no 'engine'.")
        if "no_cache" in search:
            raise ValueError(f"Search {index} sets 'no_cache', which SerpApi does not allow with async searches.")
        if "async" in search:
            raise ValueError(f"Search {index} sets 'async'. The operator sets it itself.")
        checked.append(dict(search))
    return checked


class _ArchiveReader(Protocol):
    async def archive(self, search_id: str) -> dict[str, Any]: ...


async def wait_for_searches(
    client: _ArchiveReader,
    search_ids: list[str],
    *,
    poll_interval: float = 2.0,
    max_concurrency: int = 10,
    timeout: float = 600.0,
) -> dict[str, Any]:
    """Poll the archive until every search has finished or failed, or the timeout passes.

    Only reads the archive, so running it twice costs nothing and submits nothing.
    Returns ids and statuses only, never result bodies:
    {"status": "done" | "timeout", "results": [{"search_id", "status", "error"}], "pending": [ids]}
    """
    order = list(dict.fromkeys(search_ids))
    pending = list(order)
    results: dict[str, dict[str, Any]] = {}
    errors: dict[str, int] = {}
    semaphore = asyncio.Semaphore(max(1, max_concurrency))
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    async def check(search_id: str) -> None:
        async with semaphore:
            try:
                body = await client.archive(search_id)
            except SerpApiAuthError:
                raise
            except SerpApiError as exc:
                errors[search_id] = errors.get(search_id, 0) + 1
                if errors[search_id] >= MAX_CONSECUTIVE_ERRORS:
                    results[search_id] = {"search_id": search_id, "status": "Error", "error": str(exc)}
                return
        errors.pop(search_id, None)
        status = status_of(body)
        if is_finished(status):
            results[search_id] = {"search_id": search_id, "status": status, "error": None}
        elif is_failed(status):
            results[search_id] = {
                "search_id": search_id,
                "status": status or "Error",
                "error": str(body.get("error") or "The search failed without a message."),
            }

    while pending:
        await asyncio.gather(*(check(search_id) for search_id in pending))
        pending = [search_id for search_id in pending if search_id not in results]
        if not pending or loop.time() + poll_interval > deadline:
            break
        await asyncio.sleep(poll_interval)

    return {
        "status": "done" if not pending else "timeout",
        "results": [results[search_id] for search_id in order if search_id in results],
        "pending": pending,
    }
