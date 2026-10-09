"""Batch search operator: submit many searches, release the worker, collect the rows."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

try:  # Airflow 3
    from airflow.sdk import BaseOperator
except ImportError:  # pragma: no cover - older Airflow
    from airflow.models.baseoperator import BaseOperator

try:  # Airflow 3.1 and later
    from airflow.sdk.exceptions import AirflowException, AirflowFailException, AirflowSkipException
except ImportError:  # pragma: no cover - Airflow 3.0
    from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException

from serpapi_provider.core.batch import validate_batch
from serpapi_provider.core.extractors import extract_rows, has_extractor
from serpapi_provider.core.quota import check_quota
from serpapi_provider.hooks.serpapi import SerpApiHook
from serpapi_provider.triggers.batch import SerpApiBatchTrigger


class SerpApiBatchSearchOperator(BaseOperator):
    """Run a batch of SerpApi searches without holding a worker while they finish.

    The worker checks the quota, submits every search with ``async=true`` and then
    defers. One trigger waits for all of them in the triggerer. When they are done
    the task resumes, reads each result from SerpApi's search archive (which is not
    counted as a search) and returns flat rows.

    :param searches: a list of SerpApi parameter dictionaries, one per search.
        Each returns its first page. Templated.
    :param serpapi_conn_id: the SerpApi connection. Default ``serpapi_default``.
    :param extract: return flat rows when the engine has a row extractor (default).
        Otherwise, or when False, return the raw responses.
    :param reserve_searches: searches to keep back for other work. The run is
        refused if it would dip into them.
    :param max_batch_size: refuse a larger batch before any request. Default 50.
    :param on_insufficient_quota: ``"fail"`` (default) or ``"skip"``.
    :param on_search_error: ``"fail"`` (default) fails the task if any search
        failed; ``"skip"`` returns the rows of the searches that succeeded.
    :param poll_interval: seconds between archive checks in the trigger.
    :param max_concurrency: simultaneous archive requests in the trigger.
    :param timeout: seconds the trigger waits before giving up.
    """

    template_fields: Sequence[str] = ("searches",)
    ui_color = "#e8f1ff"

    def __init__(
        self,
        *,
        searches: list[dict[str, Any]],
        serpapi_conn_id: str = SerpApiHook.default_conn_name,
        extract: bool = True,
        reserve_searches: int = 0,
        max_batch_size: int = 50,
        on_insufficient_quota: str = "fail",
        on_search_error: str = "fail",
        poll_interval: float = 2.0,
        max_concurrency: int = 10,
        timeout: float = 600.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        for name, value in (("on_insufficient_quota", on_insufficient_quota), ("on_search_error", on_search_error)):
            if value not in ("fail", "skip"):
                raise ValueError(f"{name} must be 'fail' or 'skip', not {value!r}.")
        self.searches = searches
        self.serpapi_conn_id = serpapi_conn_id
        self.extract = extract
        self.reserve_searches = reserve_searches
        self.max_batch_size = max_batch_size
        self.on_insufficient_quota = on_insufficient_quota
        self.on_search_error = on_search_error
        self.poll_interval = poll_interval
        self.max_concurrency = max_concurrency
        self.timeout = timeout

    def execute(self, context: Any) -> Any:
        searches = validate_batch(self.searches, self.max_batch_size)
        hook = SerpApiHook(self.serpapi_conn_id)

        decision = check_quota(hook.quota(), wanted=len(searches), reserve=self.reserve_searches)
        self.log.info("Quota guard: %s", decision.reason)
        if not decision.ok:
            message = f"Quota guard refused this run. {decision.reason}"
            if self.on_insufficient_quota == "skip":
                raise AirflowSkipException(message)
            raise AirflowFailException(message)

        submitted: list[dict[str, Any]] = []
        for search in searches:
            submitted.append({"search_id": hook.submit_async(search), "search": search})
        self.log.info(
            "Submitted %d searches. Search ids: %s",
            len(submitted),
            ", ".join(item["search_id"] for item in submitted),
        )

        self.defer(
            trigger=SerpApiBatchTrigger(
                serpapi_conn_id=self.serpapi_conn_id,
                search_ids=[item["search_id"] for item in submitted],
                poll_interval=self.poll_interval,
                max_concurrency=self.max_concurrency,
                timeout=self.timeout,
            ),
            method_name="execute_complete",
            kwargs={"submitted": submitted},
        )

    def execute_complete(
        self, context: Any, event: dict[str, Any] | None = None, submitted: list[dict[str, Any]] | None = None
    ) -> Any:
        event = event or {}
        submitted = submitted or []
        status = event.get("status")

        if status == "error":
            raise AirflowException(f"SerpApi batch failed while waiting: {event.get('message')}")
        if status == "timeout":
            pending = event.get("pending") or []
            raise AirflowException(
                f"Timed out after {self.timeout:.0f}s with {len(pending)} of {len(submitted)} searches unfinished. "
                f"Pending search ids: {', '.join(pending)}. SerpApi keeps them in its archive for 31 days."
            )
        if status != "done":
            raise AirflowException(f"Unexpected trigger event: {event!r}")

        searches_by_id: dict[str, dict[str, Any]] = {}
        for item in submitted:
            searches_by_id.setdefault(item["search_id"], item["search"])

        hook = SerpApiHook(self.serpapi_conn_id)
        output: list[Any] = []
        failed: list[str] = []
        for result in event.get("results") or []:
            search_id = result["search_id"]
            search = searches_by_id.get(search_id, {})
            label = f"{search.get('engine')} q={search.get('q')!r} (id {search_id})"
            if result.get("error"):
                failed.append(f"{label}: {result['error']}")
                continue
            body = hook.archive(search_id)
            engine = search.get("engine")
            if self.extract and has_extractor(engine):
                output.extend(extract_rows(engine, body, query=search.get("q")))
            else:
                output.append(body)

        if failed:
            summary = f"{len(failed)} of {len(searches_by_id)} searches failed:\n  " + "\n  ".join(failed)
            if self.on_search_error == "fail":
                raise AirflowException(summary)
            self.log.warning("%s", summary)

        self.log.info("Collected %d %s from %d searches.", len(output), "rows" if self.extract else "responses",
                      len(searches_by_id) - len(failed))
        return output
