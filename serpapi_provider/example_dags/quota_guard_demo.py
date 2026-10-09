"""
### Quota guard (example)

The same 20-search batch as `price_tracker`, but told to keep 100,000 searches in
reserve. No account has that many, so the quota guard **refuses the run before a
single search is sent** and the task log says exactly why.

This is what happens on a real account when a run would use more searches than are
left this month or this hour.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG

from serpapi_provider.demo import PRODUCTS, shopping_searches
from serpapi_provider.operators.batch import SerpApiBatchSearchOperator

with DAG(
    dag_id="quota_guard_demo",
    schedule=None,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["serpapi", "example"],
    doc_md=__doc__,
):
    SerpApiBatchSearchOperator(
        task_id="fetch_prices_refused",
        searches=shopping_searches(PRODUCTS),
        reserve_searches=100_000,
        retries=0,
    )
