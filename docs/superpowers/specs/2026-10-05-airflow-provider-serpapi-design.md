# Airflow provider for SerpApi: design

- **Date:** 2026-10-05
- **Status:** design approved in chat, awaiting review of this written spec
- **For:** SerpApi India Hackathon 2026, Track 2 (Open-Source Integrations)
- **Deadline:** October 10, 2026 at 23:59 IST. Target submission time is 18:00 IST that day.
- **Builder:** one builder working in a single session with an AI coding assistant. All work is sequential.
- **Package name:** `airflow-provider-serpapi`. A community project, not an official SerpApi package. The README says so in its first lines.

## 1. Purpose

Data engineers already call SerpApi from Apache Airflow, with hand-written requests inside a task. That works for one search. It does not work well for many: each search holds a worker for about two seconds, nothing watches the search quota, and a condition check repeated every few minutes spends searches on results SerpApi would have served from its cache.

This package makes SerpApi a first-class part of an Airflow pipeline. Its value is in three things the plain route does not give:

1. A batch of searches submitted at once, with the worker released while one background trigger waits for all of them.
2. A quota guard that refuses to start a run the account cannot afford.
3. A sensor that fires when a value crosses a line, checks no more often than SerpApi's cache makes useful, and states its cost.

## 2. Scope

**In scope**

- A SerpApi connection type for Airflow.
- A hook, a single-search operator, a batch-search operator with its trigger, a quota guard and a threshold sensor.
- Row extractors for three engines: Google Shopping, Google Search and Google Jobs.
- An example pipeline that tracks product prices into Postgres.
- A replay server so the example pipeline and the tests run with no API key.
- A one-command demo with Docker Compose.
- A written side-by-side comparison with the plain route, and a "when not to use this" section.

**Out of scope**

- Any language model.
- Engines beyond the three. Other engines still work through the operators; they return the raw response.
- Publishing to PyPI or any registry. The package installs from GitHub.
- Airflow 2. The package targets Airflow 3.3.
- Pacing a batch across several hours. A batch larger than the hourly allowance is refused.
- A custom deferrable trigger for the sensor. Airflow's reschedule mode already frees the worker between checks.
- Asset-based (event-driven) scheduling.

## 3. Facts this design rests on

Verified from public documentation on 2026-10-04 and 2026-10-05.

**SerpApi**

- Search: `GET https://serpapi.com/search.json` with `engine`, the engine's parameters and `api_key`.
- Asynchronous search: add `async=true`. The response returns at once with `search_metadata.id`. `async` must not be combined with `no_cache`.
- Search archive: `GET https://serpapi.com/searches/{search_id}.json`. Status is `Queued`, `Processing`, `Success` or an error. SerpApi's own example code also treats `Cached` as finished. Results are kept 31 days.
- Account: `GET https://serpapi.com/account.json`. It is free and returns the searches left and the hourly limit.
- Cache: identical searches within one hour are served from cache and are not counted.
- Free plan: 250 searches a month and 50 an hour. HTTP 429 means the hourly limit was passed or the account has no searches left.
- A normal Google search takes about 2 seconds on average, by SerpApi's own figures.
- SerpApi's official Python library has no asyncio client, so it cannot be used inside an Airflow trigger.

**Airflow**

- Current version is 3.3.2. It supports Python 3.10 to 3.14 and runs only on POSIX systems, so on this machine it runs in Docker.
- `airflow standalone` starts the scheduler, DAG processor, API server and triggerer in one container.
- A third-party package registers itself through the `apache_airflow_provider` entry point and can declare a connection type.
- A deferrable operator hands off to a trigger that runs in the triggerer. A trigger must be asynchronous and serialisable, and may run more than once.
- A sensor in reschedule mode holds a worker only while it checks.

**Not yet verified, settled by the day-1 gate (section 13)**

- Whether asynchronous search is allowed on the free plan, and whether it is counted as one search.
- What an asynchronous request returns when a cached result exists.
- Whether Airflow 3.3 shows a third-party connection type with custom fields in its connection form.

## 4. Architecture

Core logic is plain Python with no Airflow imports, so it can be tested anywhere. The Airflow classes are thin wrappers around it.

```
airflow-provider-serpapi/
  pyproject.toml
  serpapi_provider/
    __init__.py               provider info for Airflow's entry point
    core/
      client.py               HTTP client, sync and async, built on httpx
      errors.py               error classes
      quota.py                quota arithmetic
      extractors.py           raw result to flat rows, per engine
      threshold.py            crossing logic for the sensor
    hooks/serpapi.py          SerpApiHook and the connection type
    operators/search.py       SerpApiSearchOperator
    operators/batch.py        SerpApiBatchSearchOperator
    triggers/batch.py         SerpApiBatchTrigger
    sensors/threshold.py      SerpApiThresholdSensor
    example_dags/price_tracker.py
  replay/server.py            stand-in SerpApi that serves saved responses
  tests/
    unit/                     core modules, no Airflow needed
    airflow/                  hook, operators, trigger, sensor, run inside the container
    fixtures/                 saved SerpApi responses
  docker/Dockerfile
  docker-compose.yml
  scripts/day1_async_check.py
  scripts/capture_fixtures.py
  docs/comparison.md
```

**Units and their boundaries**

| Unit | Does | Depends on |
|---|---|---|
| `core/client` | Sends one request to the search, archive or account endpoint. Maps HTTP and API errors to error classes. Never logs the key. Offers the same calls in sync and async form. | `httpx` |
| `core/quota` | Given the account's numbers and a count of searches wanted, says whether the run may start and why not. | nothing |
| `core/extractors` | Turns one raw response into a list of flat rows. One function per engine. | nothing |
| `core/threshold` | Given the previous state, a new value, a threshold and a direction, says whether the line was crossed and returns the new state. | nothing |
| `hooks/serpapi` | Reads the Airflow connection, builds a client, exposes search, paged search, asynchronous submit, archive lookup and account. Backs the connection form and its Test button. | `core/client` |
| `operators/search` | Runs one search or a paged search and returns rows or the raw response. | hook, `core/extractors` |
| `operators/batch` | Checks the quota, submits the batch, defers, then collects results and returns rows. | hook, `core/quota`, trigger, `core/extractors` |
| `triggers/batch` | Waits, without holding a worker, until every search in the batch has finished or failed. | `core/client` (async) |
| `sensors/threshold` | Runs a search at most once per cache period, extracts a number and succeeds when it crosses the threshold. | hook, `core/extractors`, `core/threshold` |
| `replay/server` | Answers the three endpoints from saved files. | standard library |

## 5. Connection

- Connection type: `serpapi`. Default connection id: `serpapi_default`.
- The API key is stored in the connection's password field, so Airflow encrypts it and masks it in logs.
- The host field is optional and defaults to `https://serpapi.com`. Pointing it at the replay server runs everything offline.
- The connection form's Test button calls the account endpoint, which is free.

## 6. The client and the key

- All requests go through `core/client`. No other module builds a SerpApi URL.
- SerpApi takes the key as a query parameter, so the key is part of the URL. The client therefore never logs a URL, and sets the `httpx` logger to warnings only, because `httpx` logs every request URL at info level.
- Error messages are built from SerpApi's `error` text with the key removed if it appears.
- Errors map as follows:

| Situation | Error class | Effect in Airflow |
|---|---|---|
| HTTP 401, invalid key | `SerpApiAuthError` | Task fails without retry |
| HTTP 429 | `SerpApiRateLimitError` | Task fails and Airflow's normal retries apply |
| Other HTTP error or network failure | `SerpApiHttpError` | Task fails and retries apply |
| `error` saying the search returned no results | none | Treated as success with zero rows |
| Any other `error` in the body | `SerpApiSearchError` | Reported per search |

## 7. Quota guard

Before spending anything, the batch operator and the sensor read the account endpoint.

- Monthly check: searches left, minus a configurable reserve, must be at least the number of searches the run wants.
- Hourly check: the hourly limit, minus searches already made this hour, must be at least that number.
- If either fails, the task fails with a message naming the shortfall. With `on_insufficient_quota="skip"` it is skipped.
- The count wanted is an upper bound. Searches SerpApi serves from cache are free and cannot be known in advance.

## 8. Batch search operator

`SerpApiBatchSearchOperator(searches=[...], conn_id=..., extractor=..., ...)`

1. **Validate.** Reject an empty batch, a batch above `max_batch_size` (default 50), and any search that sets `no_cache`.
2. **Guard.** Run the quota guard for the size of the batch.
3. **Submit.** Send each search with `async=true` from the worker and collect the search ids. The ids are written to the task log.
4. **Defer.** Hand the ids to `SerpApiBatchTrigger` and release the worker.
5. **Wait.** The trigger polls the archive for all ids concurrently, until each is `Success`, `Cached` or an error, or until its timeout. Defaults: a poll every 2 seconds, at most 10 simultaneous requests, a timeout of 600 seconds. It returns only ids and statuses, never result bodies.
6. **Collect.** Back on a worker, fetch each finished result from the archive, which is free, and run the extractor.
7. **Return.** A list of rows. Each row carries the search id, engine, query and fetch time.

Behaviour worth stating:

- **One page each.** Every search in a batch returns its first page. Paging belongs to the single search operator.
- **Trigger replay.** A trigger may run more than once. It only reads the archive, so a replay costs nothing and submits nothing.
- **Task retry.** A retry runs step 3 again. Within an hour, SerpApi serves identical searches from its cache at no cost. The day-1 gate confirms this holds for asynchronous requests.
- **Partial failure.** With `on_search_error="fail"` (the default), the task fails after collecting, and the message lists each failed search. With `"skip"`, it returns rows for the searches that succeeded and logs the rest.
- **Timeout.** The task fails and logs the ids still pending. They stay in SerpApi's archive for 31 days.

If the day-1 gate shows asynchronous search is not usable on the free plan, step 3 moves into the trigger: it runs normal searches concurrently and the worker is still released. Only one of the two modes is built.

## 9. Single search operator

`SerpApiSearchOperator(search={...}, max_pages=1, extractor=..., conn_id=...)`

Runs one search on the worker. With `max_pages` above 1 it follows SerpApi's pagination. It returns rows when an extractor is given and the raw response otherwise. It exists for simple cases and for completeness; the README says plainly that the plain route does this in a few lines too.

## 10. Threshold sensor

`SerpApiThresholdSensor(search={...}, extractor=..., field=..., aggregate="min", threshold=..., direction="below", conn_id=...)`

- Each check runs the search, extracts rows, and reduces `field` over the rows with `aggregate` (`min`, `max` or `first`) to one number.
- **Crossing, not level.** The sensor succeeds when the value meets the condition and the previous observed value did not. The previous state is kept in an Airflow Variable named `serpapi_threshold__<dag_id>__<task_id>`. A value that stays past the line does not fire again until it has gone back and crossed again.
- **First observation.** With no previous state, the sensor records the value and does not fire, unless `fire_on_first_check=True`.
- **Cache-aware.** `poke_interval` below 3600 seconds is raised to 3600, with a log line saying why: SerpApi would return the same cached result.
- **Cost stated.** On start it logs the most searches it can spend in a day.
- **Mode.** It defaults to reschedule mode, so it holds a worker only while checking.
- It runs the quota guard for one search before each check.

## 11. Extractors

Each extractor takes one raw response and returns flat rows with stable column names. Missing fields become `None`.

| Engine | Source list | Columns |
|---|---|---|
| `google_shopping` | `shopping_results` | `position`, `title`, `product_id`, `source`, `price`, `extracted_price`, `old_price`, `extracted_old_price`, `rating`, `reviews`, `delivery`, `link` |
| `google` | `organic_results` | `position`, `title`, `link`, `domain`, `displayed_link`, `snippet`, `date` |
| `google_jobs` | `jobs_results` | `job_id`, `title`, `company_name`, `location`, `via`, `posted_at`, `schedule_type`, `salary` |

Every row also carries `search_id`, `engine`, `query` and `fetched_at`.

## 12. Example pipeline and demo

`price_tracker` does this:

1. A batch of about 20 Google Shopping searches for named products.
2. Load the rows into a Postgres table with an upsert keyed on the run's date, the query and the position, so a re-run of the same day does not duplicate rows and each day adds one snapshot.
3. A threshold sensor on one product's lowest price.
4. A downstream task that runs when the sensor fires.

The demo is one command: `docker compose up`. It starts one Airflow container (`airflow standalone`, with the package installed) and Postgres.

**Replay.** `docker compose --profile replay up` also starts the replay server and points the connection at it. The whole pipeline then runs with no key and no searches. The replay server serves saved responses for the search, archive and account endpoints, and reports asynchronous searches as `Processing` for a few seconds before `Success`, so the deferred state is visible.

## 13. Day-1 gate

Run before the main build. The team creates the SerpApi account and starts Docker Desktop first.

| Test | What it does | Cost | Result decides |
|---|---|---|---|
| **G1 Comparison** | Write the price pipeline the plain way against a saved response. Write the provider's public interface as stubs. Write the 3-minute demo script. | 0 searches | Pass if the script has at least three on-screen moments the plain version cannot produce. If not, stop and tell the team. |
| **G2 Async** | `scripts/day1_async_check.py`: read the account, submit one asynchronous Google Shopping search, poll the archive every second and log each status and the time to finish, read the account again, then submit the identical search again and repeat. | Up to 4 searches | If asynchronous search is accepted and counted once, section 8 is built as written. If not, the concurrent mode in section 8 is built. The result of the repeat is recorded in the README. |
| **G3 Environment** | Start one Airflow 3.3.2 container with `airflow standalone`. Install a skeleton of the package with a connection type and a toy trigger that sleeps 10 seconds. | 0 searches, 3 hours at most | Pass if the interface loads, "SerpApi" appears in the connection form, the toy task shows as deferred and then succeeds, and the container uses under 4 GB. If only the form fails, the connection falls back to the generic type with documented fields. If the task does not defer within the time limit, stop and tell the team. |

## 14. Testing

- **Unit tests** for `core/` with `pytest`, using `httpx.MockTransport` and files in `tests/fixtures/`. They need no Airflow, no key and no network.
- **Airflow tests** for the hook, operators, trigger and sensor, run inside the container against the replay server or mocked transports.
- A test asserts that no log record and no error message produced by the client contains the key.
- A test fails if any file under `tests/fixtures/` or `replay/` contains a 64-character hex string, which is the shape of a SerpApi key.
- A manual run-through of the demo script in replay mode before recording.

## 15. Search budget

One free account, 250 searches.

| Use | Searches |
|---|---|
| Day-1 gate | 5 |
| Capturing fixtures and build checks | 20 |
| Live batches while developing | 30 |
| One live batch and sensor run for the video | 25 |
| Reserve | 30 |
| **Planned total** | **110** |
| Unplanned | 140 |

## 16. Schedule

| Date | Work |
|---|---|
| Oct 5 | Spec, plan, day-1 gate. |
| Oct 6 | Client, errors, quota, extractors. Hook and connection type. Single search operator. |
| Oct 7 | Batch operator and trigger. Replay server. |
| Oct 8 | Threshold sensor. Example pipeline. Docker Compose demo. |
| Oct 9 | README, comparison document, secret scan, demo script. The team records the video. |
| Oct 10 | Check the repository and video links in a private window. The team submits by 18:00 IST. |

If work slips, features are cut in this order: Google Jobs and Google Search extractors, single search operator's paging, threshold sensor. The batch operator, the quota guard, the connection type and the replay demo are the minimum.

## 17. Submission checklist

From the hackathon rules:

- Public GitHub repository with code and setup instructions. Licence: Apache-2.0.
- Demo video under 3 minutes showing the project running locally, on a public or unlisted link.
- Project description, Track 2, and an explanation of SerpApi usage.
- Lead participant and teammate details.
- Disclosure that the project was started during the hackathon and that AI tools were used and how.
- Rules and Terms accepted. "Submit project" selected, not only a saved draft.

## 18. Known limits

- A single search gains nothing from this package over a few lines of plain code. The README says so.
- No evidence was found that anyone has asked for an Airflow provider for SerpApi. No search-data vendor has one.
- The sensor's memory is an Airflow Variable. Clearing it resets the crossing state.
- The quota guard reads the account once before a run. Another process using the same key at the same moment can still exhaust it.
- Row extractors cover three engines.
