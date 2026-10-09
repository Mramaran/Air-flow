"""
### Price drop alert (example)

Waits until the median Google Shopping price for one query **crosses below** a
threshold, then runs a downstream task. It watches the median because real results
mix in accessories and other models, which makes the lowest price meaningless.

The sensor remembers the previous check, so it fires on the crossing and not on
every run where the price happens to be low. It checks at most once an hour,
because SerpApi serves identical searches from its cache for an hour, and it logs
how many searches a day that can cost.

For this demo `fire_on_first_check=True`, so you do not have to wait for a real
price move: if the price is already under the line on the first check, it fires.
Trigger it a second time and it will keep waiting, because the price has not
crossed again.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG, task

from serpapi_provider.sensors.threshold import SerpApiThresholdSensor

PRODUCT = "iPhone 16 128GB"
THRESHOLD_RUPEES = 80_000

with DAG(
    dag_id="price_drop_alert",
    schedule=None,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["serpapi", "example"],
    doc_md=__doc__,
):
    price_drops = SerpApiThresholdSensor(
        task_id="wait_for_price_drop",
        search={"engine": "google_shopping", "q": PRODUCT, "gl": "in", "hl": "en"},
        field="extracted_price",
        aggregate="median",
        threshold=THRESHOLD_RUPEES,
        direction="below",
        fire_on_first_check=True,
        timeout=timedelta(minutes=2).total_seconds(),
        retries=0,
    )

    @task
    def send_alert() -> None:
        print(f"Price alert: the median offer for {PRODUCT!r} has fallen below ₹{THRESHOLD_RUPEES:,}.")

    price_drops >> send_alert()
