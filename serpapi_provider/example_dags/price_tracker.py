"""
### Price tracker (example)

Fetches Google Shopping prices for 20 products in **one batch**, loads them into a
SQLite table and logs the spread of prices returned for each product.

`fetch_prices` submits every search to SerpApi at once and then **defers**: it holds
no worker while SerpApi does the work. One trigger waits for all 20 results.

Before it spends anything, the quota guard reads SerpApi's free account endpoint and
refuses the run if the account cannot afford 20 searches.

Trigger it by hand. In the default (replay) stack it needs no key and spends nothing.
"""

from __future__ import annotations

from datetime import datetime, timezone

from airflow.sdk import DAG, task

from serpapi_provider.demo import DB_PATH, PRODUCTS, price_spread, save_rows, shopping_searches
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
    def report_price_spread(run_date: str) -> None:
        def rupees(value: float) -> str:
            return f"₹{value:,.0f}"

        lines = [f"{'Query':<30} {'Offers':>6} {'Lowest':>11} {'Median':>11} {'Highest':>11}", "-" * 73]
        for row in price_spread(run_date):
            lines.append(
                f"{row['query']:<30} {row['offers']:>6} {rupees(row['lowest']):>11} "
                f"{rupees(row['median']):>11} {rupees(row['highest']):>11}"
            )
        lines.append("")
        lines.append("These are the offers Google Shopping returned for each query. They include")
        lines.append("accessories and other models, so 'lowest' is not the price of the named product.")
        print(f"Price spread per query on {run_date}:\n" + "\n".join(lines))

    report_price_spread(load_prices(fetch_prices.output))
