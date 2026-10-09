# airflow-provider-serpapi

> A community project. **Not an official SerpApi package.**
> Built for the SerpApi India Hackathon 2026, Track 2 (Open-Source Integrations).

An [Apache Airflow](https://airflow.apache.org/) provider for [SerpApi](https://serpapi.com/). It gives Airflow a SerpApi connection type and three building blocks for pipelines that collect search data:

| Class | What it does |
|---|---|
| `SerpApiBatchSearchOperator` | Submits many searches at once, then **releases the worker** while one trigger waits for all the results. Checks your search quota first. |
| `SerpApiThresholdSensor` | Succeeds when a number from a search **crosses** a line, such as a price falling below ₹60,000. Fires on the crossing, not on every run where the price is low. |
| `SerpApiSearchOperator` | Runs one search and returns flat rows. |
| `SerpApiHook` | The connection type, with its own form in Airflow and support for Airflow's connection test. |

No language model is involved anywhere.

## Try it in a minute, with no key

You need Docker. The default stack runs against a small replay server, so it needs no SerpApi account and spends no searches.

```bash
git clone https://github.com/Mramaran/Air-flow.git
```

```bash
cd Air-flow
```

```bash
docker compose up --build
```

Open <http://localhost:8080>. No login is needed. Then:

1. Trigger **`price_tracker`**. Watch `fetch_prices` turn **deferred**: 20 searches are in flight and no worker is held. Open the log of `report_price_spread` to see the offers, lowest, median and highest price for each query.
2. Trigger **`quota_guard_demo`**. It fails in a few seconds, before any search is sent, and its log says why.
3. Trigger **`price_drop_alert`**. The sensor fires and the alert task runs. Trigger it again: it now waits, because the price has not crossed the line a second time.
4. Go to **Admin → Connections → Add Connection** and pick **SerpApi**. The form shows an API key and an optional base URL in place of the generic host, login and port fields.

Everything the replay server returns is marked `"_replay": true`. For the 20 products in the example pipelines it replays real Google Shopping results captured from SerpApi on October 9, 2026. For any other query it returns placeholder offers from sellers called "Replay Store".

## Run it live with your own key

```bash
cp .env.example .env
```

Paste your key into `.env` after `SERPAPI_KEY=`, then start the stack with the live override:

```bash
docker compose -f docker-compose.yml -f docker-compose.live.yml up --build
```

Every new search is now counted against your account. `price_tracker` uses up to 20. The `.env` file is git-ignored.

## Install in your own Airflow

Requires Airflow 3.0 or later, with a triggerer running.

```bash
pip install "git+https://github.com/Mramaran/Air-flow.git"
```

Create a connection of type **SerpApi** with your key in the **API key** field, or set it from the environment:

```bash
export AIRFLOW_CONN_SERPAPI_DEFAULT='{"conn_type": "serpapi", "password": "<your key>"}'
```

## Usage

### A batch of searches

```python
from serpapi_provider.operators.batch import SerpApiBatchSearchOperator

fetch = SerpApiBatchSearchOperator(
    task_id="fetch_prices",
    searches=[
        {"engine": "google_shopping", "q": "iPhone 16 128GB", "gl": "in"},
        {"engine": "google_shopping", "q": "Google Pixel 10 128GB", "gl": "in"},
        # ... up to 50
    ],
    reserve_searches=50,          # keep 50 searches back for other work
    on_search_error="skip",       # return what succeeded; default is "fail"
)
```

What happens when it runs:

1. **Validate.** An empty batch, more than 50 searches, or a search with `no_cache` is refused before any request.
2. **Guard.** It reads SerpApi's account endpoint, which is free, and refuses the run if the batch does not fit in the searches left this month (minus your reserve) or this hour.
3. **Submit.** Each search is sent with `async=true`. SerpApi answers at once with a search id.
4. **Defer.** The task hands the ids to a trigger and gives up its worker slot.
5. **Wait.** In the triggerer, one trigger polls SerpApi's search archive for all the ids, ten at a time.
6. **Collect.** When every search has finished, the task resumes, reads each result from the archive and returns flat rows.

The trigger only ever reads the archive. If Airflow runs it twice, nothing is submitted twice. The API key is never stored with the trigger; it carries the connection id.

### A price-crossing sensor

```python
from serpapi_provider.sensors.threshold import SerpApiThresholdSensor

wait = SerpApiThresholdSensor(
    task_id="wait_for_price_drop",
    search={"engine": "google_shopping", "q": "iPhone 16 128GB", "gl": "in"},
    field="extracted_price",
    aggregate="median",           # steadier than "min"; see the note below
    threshold=80_000,
    direction="below",
)
```

- It keeps the previous check in the Airflow Variable `serpapi_threshold__<dag_id>__<task_id>` and succeeds only when the value was not past the line before and is now.
- SerpApi serves identical searches from its cache for an hour, so a `poke_interval` under 3600 seconds is raised to 3600. The log states the most searches the sensor can spend in a day (24 at the default).
- It runs in reschedule mode, so it holds a worker only while it checks.
- On its first check it records the value and does not fire, unless you pass `fire_on_first_check=True`.
- `aggregate` can be `min`, `max`, `median` or `first`. Real shopping results for a product name include accessories, refurbished units and other models. In the live run, the lowest offer for "iPhone 16 128GB" was ₹7,999 and the median was ₹66,549, so `median` is the safer choice.

### One search

```python
from serpapi_provider.operators.search import SerpApiSearchOperator

jobs = SerpApiSearchOperator(
    task_id="data_engineer_jobs",
    search={"engine": "google_jobs", "q": "data engineer chennai", "gl": "in"},
)
```

### The hook

```python
from serpapi_provider.hooks.serpapi import SerpApiHook

hook = SerpApiHook()                      # uses the serpapi_default connection
status = hook.quota()                     # searches left, used this hour, hourly limit
body = hook.search({"engine": "google", "q": "apache airflow"})
```

## Row extractors

With `extract=True` (the default) the operators return flat rows with stable column names. Every row also has `search_id`, `engine`, `query` and `fetched_at`.

| Engine | Columns |
|---|---|
| `google_shopping` | `position`, `title`, `product_id`, `source`, `price`, `extracted_price`, `old_price`, `extracted_old_price`, `rating`, `reviews`, `delivery`, `link` |
| `google` | `position`, `title`, `link`, `domain`, `displayed_link`, `snippet`, `date` |
| `google_jobs` | `job_id`, `title`, `company_name`, `location`, `via`, `posted_at`, `schedule_type`, `salary` |

Any other engine still works. The operators return its raw response.

## Why not just call SerpApi inside a task?

For one search, you should. It takes a few lines and this package adds nothing.

For many searches on a schedule, [docs/comparison.md](docs/comparison.md) puts the plain version and this one side by side. In short:

| | Plain: one mapped task per search | This provider |
|---|---|---|
| Worker slots held while SerpApi works | One per search | None; the task is deferred |
| Task instances for 20 searches | 20 | 1 |
| Quota checked before spending | Only if you write it | Yes, month and hour |
| A batch larger than the hourly limit | Fails part-way with HTTP 429 | Refused before the first request |
| Which searches failed | Spread over 20 task logs | One message that names each |
| The key | You fetch it and keep it out of logs yourself | Stored in a connection; the client never logs a URL |

### When not to use this

- You run one search, or a handful. Use SerpApi's own Python library in a task.
- Your Airflow has no triggerer. Deferral needs one.
- You need results in the same second. Asynchronous searches are for throughput, not latency.
- Your account has SerpApi's Ludicrous Speed option. SerpApi's documentation says not to use `async` with it.

## How the key is kept safe

SerpApi takes the key as a URL parameter, so the key is part of every request URL. This package therefore:

- sends every request through one client that never logs a URL, and silences `httpx`'s request logging, which would print it;
- removes the key from any error text SerpApi sends back;
- reports a network failure by its type only, because the exception text can contain the URL;
- never serialises the key into a trigger.

Tests check each of these, and one test fails if any committed file contains a string shaped like a SerpApi key.

## Tests

```bash
python -m venv .venv && .venv/bin/pip install -e ".[dev]" && .venv/bin/pytest
```

That runs the core tests: the client, quota check, extractors, crossing logic, batch wait loop and replay server. They need no Airflow, no key and no network.

The Airflow classes are tested inside the demo container, against an in-process replay server:

```bash
docker compose exec airflow python -m pytest /opt/provider/tests/airflow -q
```

## Has it been run against the real SerpApi?

Yes, on October 9, 2026, on a free plan. [docs/async-check.md](docs/async-check.md) has the measurements. In short:

- Asynchronous search is accepted on the free plan. A Google Shopping search went from `Processing` to `Success` in about 2 seconds.
- Each new asynchronous search was counted as one search. Archive lookups and account reads were not counted.
- Repeating the same search within the hour returned the same search id at once and was not counted.
- One live run of `price_tracker` submitted 20 searches, deferred, and collected 786 rows. The account's count fell by 14, because 6 of the 20 had been run earlier that hour and came from SerpApi's cache.
- No task log from that run contained the API key.

The results of that run are what the replay server now serves for the 20 example products.

## Known limits

- A single search gains nothing from this package over a few lines of plain code.
- No search-data vendor publishes an Airflow provider, and no public request for one was found. The need is inferred from pipelines that call SerpApi from Airflow by hand.
- A batch larger than the hourly allowance is refused. It is not spread across hours.
- The quota guard reads the account once before a run. Another process using the same key at that moment can still exhaust it.
- A task retry submits the batch again and relies on SerpApi's one-hour cache to make the repeat free.
- The sensor's memory is an Airflow Variable. Deleting it resets the crossing state.
- Row extractors cover three engines.
- Search results are not product matches. Google Shopping answers a product name with accessories, refurbished units and other models as well. The provider returns what SerpApi returns and does not match offers to one exact product, so the example pipeline reports the spread of prices per query, not "the price".
- SerpApi's account endpoint updated a few seconds after a search finished. Two runs started back to back can both pass the quota guard on the same count.
- The connection form layout was checked on Airflow 3.3.2 only.
- In Airflow 3.3.2 the **Test Connection** button in the connections list fails with "Bad Request" for any saved connection that has no port, including Airflow's own HTTP type: the interface sends port 0 and the API rejects the mismatch with the stored empty port. Test from the command line instead: `airflow connections test <connection id>`.

## Project layout

```
serpapi_provider/
  core/            plain Python, no Airflow: client, quota, extractors, batch wait loop, crossing logic
  hooks/           SerpApiHook and the connection type
  operators/       batch and single search operators
  triggers/        the batch trigger
  sensors/         the threshold sensor
  example_dags/    price_tracker, quota_guard_demo, price_drop_alert
replay/            the stand-in SerpApi server and its saved responses
scripts/           async_check.py and capture_fixtures.py, for use with a real key
tests/unit/        core tests
tests/airflow/     tests that need Airflow
docs/              comparison, demo script, design spec and plan
```

## About this project

- Designed on October 5 and built from October 9, 2026, during the hackathon. It did not exist before.
- Built with an AI coding assistant, which wrote the code, tests and documentation under the team's direction. The design spec and the build plan are in `docs/superpowers/`.
- SerpApi is used for every piece of data the package handles: the search, search-archive and account endpoints.

## Licence

Apache-2.0. See [LICENSE](LICENSE).
