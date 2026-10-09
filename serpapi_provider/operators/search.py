"""Single search operator. For one search the plain route is just as short; see the README."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

try:  # Airflow 3
    from airflow.sdk import BaseOperator
except ImportError:  # pragma: no cover - older Airflow
    from airflow.models.baseoperator import BaseOperator

from serpapi_provider.core.extractors import extract_rows, has_extractor
from serpapi_provider.hooks.serpapi import SerpApiHook


class SerpApiSearchOperator(BaseOperator):
    """Run one SerpApi search on the worker and return its first page.

    :param search: a SerpApi parameter dictionary. Templated.
    :param serpapi_conn_id: the SerpApi connection. Default ``serpapi_default``.
    :param extract: return flat rows when the engine has a row extractor (default).
        Otherwise, or when False, return the raw response.
    """

    template_fields: Sequence[str] = ("search",)
    ui_color = "#e8f1ff"

    def __init__(
        self,
        *,
        search: dict[str, Any],
        serpapi_conn_id: str = SerpApiHook.default_conn_name,
        extract: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.search = search
        self.serpapi_conn_id = serpapi_conn_id
        self.extract = extract

    def execute(self, context: Any) -> Any:
        if not isinstance(self.search, dict) or not self.search.get("engine"):
            raise ValueError("'search' must be a dictionary of SerpApi parameters that includes 'engine'.")
        body = SerpApiHook(self.serpapi_conn_id).search(self.search)
        engine = self.search["engine"]
        if self.extract and has_extractor(engine):
            rows = extract_rows(engine, body, query=self.search.get("q"))
            self.log.info("Search returned %d rows.", len(rows))
            return rows
        return body
