"""Trigger that waits for a batch of SerpApi searches without holding a worker."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from airflow.triggers.base import BaseTrigger, TriggerEvent

from serpapi_provider.core.batch import wait_for_searches
from serpapi_provider.core.client import AsyncSerpApiClient
from serpapi_provider.core.errors import SerpApiError
from serpapi_provider.hooks.serpapi import SerpApiHook, client_settings


async def _get_connection(conn_id: str) -> Any:
    """Read a connection from inside the triggerer's event loop."""
    aget = getattr(SerpApiHook, "aget_connection", None)
    if aget is not None:
        return await aget(conn_id)
    from asgiref.sync import sync_to_async

    return await sync_to_async(SerpApiHook.get_connection)(conn_id)


class SerpApiBatchTrigger(BaseTrigger):
    """Polls SerpApi's search archive until every search in a batch has finished or failed.

    It only reads the archive. Airflow may run a trigger more than once; a second
    run submits nothing and costs nothing. The API key is never serialised: the
    trigger carries the connection id and reads the key when it runs.
    """

    def __init__(
        self,
        serpapi_conn_id: str,
        search_ids: list[str],
        poll_interval: float = 2.0,
        max_concurrency: int = 10,
        timeout: float = 600.0,
    ) -> None:
        super().__init__()
        self.serpapi_conn_id = serpapi_conn_id
        self.search_ids = list(search_ids)
        self.poll_interval = poll_interval
        self.max_concurrency = max_concurrency
        self.timeout = timeout

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "serpapi_provider.triggers.batch.SerpApiBatchTrigger",
            {
                "serpapi_conn_id": self.serpapi_conn_id,
                "search_ids": self.search_ids,
                "poll_interval": self.poll_interval,
                "max_concurrency": self.max_concurrency,
                "timeout": self.timeout,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            api_key, base_url = client_settings(await _get_connection(self.serpapi_conn_id))
            async with AsyncSerpApiClient(api_key, base_url=base_url) as client:
                event = await wait_for_searches(
                    client,
                    self.search_ids,
                    poll_interval=self.poll_interval,
                    max_concurrency=self.max_concurrency,
                    timeout=self.timeout,
                )
        except SerpApiError as exc:
            event = {"status": "error", "message": str(exc), "results": [], "pending": self.search_ids}
        yield TriggerEvent(event)
