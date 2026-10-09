"""Turn a raw SerpApi response into flat rows with stable column names. No Airflow imports."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

Row = dict[str, Any]


def _shopping(body: dict[str, Any]) -> list[Row]:
    rows: list[Row] = []
    for index, item in enumerate(body.get("shopping_results") or [], start=1):
        rows.append(
            {
                "position": item.get("position", index),
                "title": item.get("title"),
                "product_id": item.get("product_id"),
                "source": item.get("source"),
                "price": item.get("price"),
                "extracted_price": item.get("extracted_price"),
                "old_price": item.get("old_price"),
                "extracted_old_price": item.get("extracted_old_price"),
                "rating": item.get("rating"),
                "reviews": item.get("reviews"),
                "delivery": item.get("delivery"),
                "link": item.get("link") or item.get("product_link"),
            }
        )
    return rows


EXTRACTORS: dict[str, Callable[[dict[str, Any]], list[Row]]] = {
    "google_shopping": _shopping,
}


def has_extractor(engine: str | None) -> bool:
    return engine in EXTRACTORS


def extract_rows(
    engine: str,
    body: dict[str, Any],
    *,
    query: str | None = None,
    fetched_at: str | None = None,
) -> list[Row]:
    """Rows for one response. Every row also carries search_id, engine, query and fetched_at."""
    try:
        extractor = EXTRACTORS[engine]
    except KeyError:
        raise KeyError(f"No row extractor for engine '{engine}'. Known engines: {sorted(EXTRACTORS)}") from None

    metadata = body.get("search_metadata") or {}
    parameters = body.get("search_parameters") or {}
    common = {
        "search_id": metadata.get("id"),
        "engine": engine,
        "query": query if query is not None else parameters.get("q"),
        "fetched_at": fetched_at
        or metadata.get("processed_at")
        or metadata.get("created_at")
        or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }
    return [{**common, **row} for row in extractor(body)]
