"""
### Price tracker (example)

Fetches Google Shopping prices for 20 products in **one batch**, loads them into a
SQLite table and logs the lowest price per product.

`fetch_prices` submits every search to SerpApi at once and then **defers**: it holds
no worker while SerpApi does the work. One trigger waits for all 20 results.

Before it spends anything, the quota guard reads SerpApi's free account endpoint and
refuses the run if the account cannot afford 20 searches.

Trigger it by hand. In the default (replay) stack it needs no key and spends nothing.
"""

from __future__ import annotations

from datetime import datetime, timezone

from airflow.sdk import DAG, task

from serpapi_provider.demo import DB_PATH, PRODUCTS, lowest_prices, save_rows, shopping_searches
from serpapi_provider.operators.batch import SerpApiBatchSearchOperator

with DAG(
    dag_id="price_tracker",
    schedule=None,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["serpapi", "example"],
    doc_md=__doc__,
):
    fetch_prices = SerpApiBatchSearchOperator(
        task_id="fetch_prices",
        searches=shopping_searches(PRODUCTS),
    )

    @task
    def load_prices(rows: list[dict]) -> str:
        run_date = datetime.now(timezone.utc).date().isoformat()
        count = save_rows(rows, run_date)
        print(f"Loaded {count} rows for {run_date} into {DB_PATH} (table: prices).")
        return run_date

    @task
    def report_lowest_prices(run_date: str) -> None:
        lines = [f"{'Product':<32} {'Lowest price':>14}  Seller", "-" * 70]
        for query, price, source, _title in lowest_prices(run_date):
            lines.append(f"{query:<32} {'₹{:,.0f}'.format(price):>14}  {source}")
        print(f"Lowest price per product on {run_date}:\n" + "\n".join(lines))

    report_lowest_prices(load_prices(fetch_prices.output))
