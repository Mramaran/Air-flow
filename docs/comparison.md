# The plain route and this provider, side by side

The job: every morning, get today's Google Shopping prices for 20 products and load them into a table.

Airflow can already do this with no extra package. This page shows that version first, because it is the right choice more often than not, and then what changes with the provider.

## The plain route

One mapped task per product, each calling SerpApi with its official Python library.

```python
from airflow.sdk import DAG, task
from airflow.sdk.bases.hook import BaseHook

PRODUCTS = ["iPhone 16 128GB", "Google Pixel 10 128GB", ...]   # 20 of them

with DAG(dag_id="prices_plain", schedule="@daily", ...):

    @task
    def fetch_price(product: str) -> list[dict]:
        import serpapi

        key = BaseHook.get_connection("serpapi_http").password
        results = serpapi.Client(api_key=key).search(
            engine="google_shopping", q=product, gl="in", hl="en"
        )
        return [
            {
                "query": product,
                "title": item.get("title"),
                "source": item.get("source"),
                "extracted_price": item.get("extracted_price"),
            }
            for item in results.get("shopping_results", [])
        ]

    @task
    def load(all_rows: list[list[dict]]) -> None:
        ...

    load(fetch_price.expand(product=PRODUCTS))
```

This is about 25 lines and it works. For one product, or five, stop here.

## The provider

```python
from airflow.sdk import DAG, task
from serpapi_provider.operators.batch import SerpApiBatchSearchOperator

with DAG(dag_id="prices_batch", schedule="@daily", ...):
    fetch = SerpApiBatchSearchOperator(
        task_id="fetch_prices",
        searches=[{"engine": "google_shopping", "q": p, "gl": "in", "hl": "en"} for p in PRODUCTS],
        reserve_searches=50,
    )

    @task
    def load(rows: list[dict]) -> None:
        ...

    load(fetch.output)
```

## What is different

| | Plain route | Provider | Why it matters |
|---|---|---|---|
| **Worker slots while SerpApi works** | 20 task instances, each holding a slot for the length of its search | 0. One task submits, then defers | A search takes about 2 seconds on average by SerpApi's own published figure. With the plain route that wait happens on your workers, 20 times, plus Airflow's start-up cost for each task instance. |
| **Task instances** | 20 | 1 | Fewer rows in the grid, fewer log files, less scheduler work. |
| **Quota check before spending** | None, unless you add a task for it | Built in, for the month and for the hour | On a 250-search plan, a run that cannot finish should not start. |
| **A batch bigger than the hourly limit** | Runs until SerpApi answers HTTP 429, leaving some tasks done and some failed | Refused before the first request | Half a snapshot is worse than none. |
| **Which searches failed** | Read 20 task states | One message that lists each failed search and its error; `on_search_error="skip"` returns the rest | Easier to act on. |
| **Where the key lives** | Wherever you put it. Here, a generic connection's password | A SerpApi connection type with its own form and Test button | One obvious place, and the form's Test button checks it for free. |
| **The key in logs** | SerpApi takes the key in the URL. Any HTTP library logging requests at debug or info level will print it | The client never logs a URL, and a test checks it | A leaked key is a spent quota. |
| **Rows** | You flatten the JSON yourself | Stable columns for Google Shopping, Google Search and Google Jobs | Less code to keep in step with the response shape. |

## What is the same

- **Cost in searches.** 20 searches either way. Archive lookups and the account check are not counted by SerpApi.
- **Retries.** A retry repeats the searches in both versions, and in both SerpApi's one-hour cache makes an identical repeat free.
- **The data.** The provider returns what SerpApi returns. It does not change or enrich it.

## What the provider costs you

- **A triggerer.** Deferral needs one running. `airflow standalone` starts one; some small deployments do not.
- **Latency.** Asynchronous searches are for throughput. The trigger polls every 2 seconds, so a batch finishes a moment later than the fastest single search would.
- **A dependency.** One more package to install and keep working across Airflow upgrades.
- **Not for Ludicrous Speed accounts.** SerpApi's documentation says not to use `async` with that option.

## A price-drop check, both ways

Plain route: an `HttpSensor` with a `response_check` that reads the lowest price. It is about ten lines. It succeeds on every poke while the price is under the line, and if you poke every five minutes it asks SerpApi 288 times a day for an answer that only changes hourly.

Provider: `SerpApiThresholdSensor` remembers the last check and fires on the crossing only. It will not check more than once an hour, and its log states the most it can spend in a day.
