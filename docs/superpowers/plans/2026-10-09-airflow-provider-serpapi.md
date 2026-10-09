# Airflow Provider for SerpApi Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task, inline, in this session. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `airflow-provider-serpapi`, a package that gives Apache Airflow a SerpApi connection type, a batch search operator that releases the worker while results are fetched, and a quota guard, with an offline replay demo, before the hackathon deadline.

**Architecture:** Core logic (`serpapi_provider/core/`) is plain Python on `httpx` with no Airflow imports and is unit-tested on the host. Thin Airflow classes (hook, operators, trigger) wrap it and are tested inside one `airflow standalone` Docker container. A small replay server stands in for SerpApi so the demo and tests need no key.

**Tech Stack:** Python 3.10+, httpx, Apache Airflow 3.3.2 (Docker image `apache/airflow:3.3.2`), pytest, Docker Compose, standard-library `http.server` and `sqlite3`.

**Spec:** `docs/superpowers/specs/2026-10-05-airflow-provider-serpapi-design.md`. Read it before starting any task.

## Time left, and what that changes

This plan was written at 22:15 IST on October 9. The deadline is October 10 at 23:59 IST, and the target is 18:00 IST. Four of the six scheduled days were not used, so the spec's cut order applies now, not later.

**Built (the spec's stated minimum, plus the smallest useful extras)**
- Connection type and hook
- Batch search operator and its trigger
- Quota guard
- Google Shopping extractor
- A minimal single search operator (no paging)
- Replay server and the one-command demo
- Example pipeline writing a price table
- README and the side-by-side comparison

**Stretch, only if everything above is done and verified by 10:00 IST on October 10**
- Threshold sensor
- Google Search and Google Jobs extractors

**Cut**
- Paging in the single search operator
- Postgres. The example pipeline writes a SQLite table with the standard library, so the demo needs one container fewer.

Because the builder and the planner are the same session, steps give interfaces, test cases and commands, and do not repeat full code listings. This is a deliberate departure from the usual plan format, made to save hours.

## Global Constraints

- No language model anywhere in the product.
- `SERPAPI_KEY` lives only in `.env`, which is git-ignored. It is never committed, never logged, never printed. SerpApi takes the key in the URL, so no module logs a URL and the `httpx` logger is set to warnings only.
- Every SerpApi request goes through `serpapi_provider/core/client.py`.
- Tests need no key and no network beyond localhost.
- Replay data is labelled as replay wherever it is shown.
- `async` is never combined with `no_cache`.
- Batch limit: 50 searches. Trigger defaults: poll every 2 seconds, 10 simultaneous requests, 600-second timeout.
- Search budget for the whole build: 50 of 250 (async check 4, fixtures 6, two live runs of 20).
- Licence Apache-2.0. The README's first lines say this is a community project, not an official SerpApi package.
- Commit at the end of every task. Branch `main`.

## What the team must do (nobody else can)

| When (IST) | Action |
|---|---|
| Now | Start Docker Desktop and leave it running. |
| Now | Create the SerpApi account at serpapi.com. Copy `.env.example` to `.env` and paste the key after `SERPAPI_KEY=`. Do not paste the key in chat. |
| By 08:00 Oct 10 | Create an empty public GitHub repository named `airflow-provider-serpapi` and give its URL. |
| 12:00 to 16:00 Oct 10 | Record the demo video from `docs/demo-script.md` (under 3 minutes) and upload it as unlisted. |
| By 18:00 Oct 10 | Open both links in a private window, fill in the submission form and select "Submit project". |

## File Structure

```
pyproject.toml                         package metadata, entry point, pytest config
LICENSE  README.md  .env.example
serpapi_provider/
  __init__.py                          get_provider_info(), __version__
  core/errors.py                       SerpApiError and subclasses
  core/client.py                       SerpApiClient (sync), AsyncSerpApiClient
  core/quota.py                        QuotaStatus, QuotaDecision, check_quota
  core/extractors.py                   extract_rows, EXTRACTORS
  hooks/serpapi.py                     SerpApiHook
  operators/search.py                  SerpApiSearchOperator
  operators/batch.py                   SerpApiBatchSearchOperator
  triggers/batch.py                    SerpApiBatchTrigger
  example_dags/price_tracker.py        the demo pipeline
  example_dags/quota_guard_demo.py     shows a refused run
replay/server.py                       stand-in SerpApi
replay/fixtures/                       saved responses
tests/unit/                            host-run tests for core/ and replay/
tests/airflow/                         container-run tests
docker/Dockerfile  docker-compose.yml
scripts/day1_async_check.py  scripts/capture_fixtures.py
docs/comparison.md  docs/demo-script.md  docs/async-check.md
```

---

### Task 1: Repository and package skeleton (22:15 to 22:45)

**Files:** Create `pyproject.toml`, `LICENSE`, `.env.example`, `serpapi_provider/__init__.py` and empty `__init__.py` files for each sub-package, `tests/unit/test_smoke.py`.

**Interfaces produced:**
- `serpapi_provider.__version__ = "0.1.0"`
- `serpapi_provider.get_provider_info() -> dict` with keys `package-name`, `name`, `description`, `versions`, and `connection-types: [{"connection-type": "serpapi", "hook-class-name": "serpapi_provider.hooks.serpapi.SerpApiHook"}]`
- Entry point group `apache_airflow_provider`, name `provider_info`, target `serpapi_provider:get_provider_info`

- [ ] `git init -b main` in the project folder.
- [ ] Write `pyproject.toml`: name `airflow-provider-serpapi`, `requires-python >=3.10`, dependency `httpx>=0.27`, optional extra `airflow = ["apache-airflow>=3.0"]`, optional extra `dev = ["pytest>=8"]`, the entry point above, `[tool.pytest.ini_options] testpaths = ["tests/unit"]`.
- [ ] Write the package skeleton and `get_provider_info`.
- [ ] Create a host virtual environment: `py -3.12 -m venv .venv`, then `.venv/Scripts/python -m pip install -e ".[dev]"`.
- [ ] Test: `get_provider_info()` names the hook class and the `serpapi` connection type. Run `.venv/Scripts/python -m pytest -q`. Expected: 1 passed.
- [ ] Commit: `chore: package skeleton and provider entry point`.

### Task 2: Errors and HTTP client (22:45 to 23:45)

**Files:** Create `serpapi_provider/core/errors.py`, `serpapi_provider/core/client.py`, `tests/unit/test_client.py`.

**Interfaces produced:**
- `class SerpApiError(Exception)`; subclasses `SerpApiAuthError`, `SerpApiRateLimitError`, `SerpApiHttpError`, `SerpApiSearchError`
- `FINISHED = {"Success", "Cached"}`; `is_finished(status: str) -> bool`; `is_failed(status: str) -> bool` (anything that is not `Queued`, `Processing` or finished)
- `class SerpApiClient(api_key: str, base_url: str = "https://serpapi.com", timeout: float = 30.0, transport=None)` with
  `search(params: dict) -> dict`, `submit_async(params: dict) -> str`, `archive(search_id: str) -> dict`, `account() -> dict`, `close()`
- `class AsyncSerpApiClient(...)` with the same constructor and `async archive(search_id) -> dict`, `async search(params) -> dict`, `async aclose()`
- `status_of(body: dict) -> str` reading `search_metadata.status`

**Test cases (all with `httpx.MockTransport`, key `"k" * 64`):**
- `search` sends `engine`, the given params and `api_key` to `/search.json` and returns the body.
- `submit_async` adds `async=true` and returns `search_metadata.id`; it raises `ValueError` if `no_cache` is present.
- `archive` requests `/searches/<id>.json`.
- `account` requests `/account.json`.
- HTTP 401 raises `SerpApiAuthError`; 429 raises `SerpApiRateLimitError`; 500 raises `SerpApiHttpError`; a transport exception raises `SerpApiHttpError`.
- A body whose `error` says the search "hasn't returned any results" is returned, not raised.
- Any other `error` in a 200 body raises `SerpApiSearchError`.
- No raised message and no log record captured during any of the above contains the key.
- The async client's `archive` returns the same body as the sync one.

- [ ] Write the failing tests. Run `.venv/Scripts/python -m pytest tests/unit/test_client.py -q`. Expected: import error.
- [ ] Implement `errors.py` and `client.py`. At import, set `logging.getLogger("httpx").setLevel(logging.WARNING)`. Build error text from the response's `error` field with the key replaced by `***`.
- [ ] Run the tests. Expected: all pass.
- [ ] Commit: `feat: SerpApi HTTP client with key-safe errors`.

### Task 3: Quota check and Shopping extractor (23:45 to 00:30)

**Files:** Create `serpapi_provider/core/quota.py`, `serpapi_provider/core/extractors.py`, `tests/unit/test_quota.py`, `tests/unit/test_extractors.py`, `replay/fixtures/google_shopping.json`.

**Interfaces produced:**
- `@dataclass QuotaStatus(searches_left: int, used_this_hour: int, hourly_limit: int)` with `QuotaStatus.from_account(body: dict)`; it reads `total_searches_left` and falls back to `plan_searches_left`, and reads `this_hour_searches` and `account_rate_limit_per_hour`.
- `@dataclass QuotaDecision(ok: bool, reason: str)`
- `check_quota(status: QuotaStatus, wanted: int, reserve: int = 0) -> QuotaDecision`
- `extract_rows(engine: str, body: dict, *, query: str | None = None) -> list[dict]`
- `EXTRACTORS: dict[str, Callable[[dict], list[dict]]]`, with `google_shopping` registered
- Shopping columns: `position, title, product_id, source, price, extracted_price, old_price, extracted_old_price, rating, reviews, delivery, link`. Every row also has `search_id, engine, query, fetched_at`.

**Test cases:**
- Quota: enough on both checks is ok; monthly shortfall names the numbers; reserve is subtracted; hourly shortfall names the numbers; `wanted` of 0 is ok; `from_account` handles a missing `total_searches_left`.
- Extractor: the fixture yields one row per item with every column present; a missing field becomes `None`; a body with no `shopping_results` yields `[]`; an unknown engine raises `KeyError` with the list of known engines.

- [ ] Hand-build `replay/fixtures/google_shopping.json` in the documented response shape, 8 items, with `"_replay": true` at the top level.
- [ ] Write the failing tests, implement, run `.venv/Scripts/python -m pytest -q`. Expected: all pass.
- [ ] Commit: `feat: quota check and Google Shopping row extractor`.

### Task 4: Replay server (00:30 to 01:30)

**Files:** Create `replay/server.py`, `replay/__init__.py`, `tests/unit/test_replay.py`.

**Interfaces produced:**
- `make_server(host="127.0.0.1", port=0, processing_seconds=4.0, searches_left=250, hourly_limit=50) -> ThreadingHTTPServer`
- `python -m replay.server --port 8089` runs it. Environment overrides: `REPLAY_PROCESSING_SECONDS`, `REPLAY_SEARCHES_LEFT`.
- `GET /search.json`: without `async`, returns the engine's fixture with `search_metadata.status = "Success"`. With `async=true`, returns only `search_metadata` with an id derived from the parameters and status `Processing`.
- `GET /searches/<id>.json`: `Processing` until `processing_seconds` after submission, then the fixture with status `Success`. An unknown id returns 404.
- `GET /account.json`: `total_searches_left`, `plan_searches_left`, `this_hour_searches`, `account_rate_limit_per_hour`. Each non-cached search lowers the count.
- Prices in the fixture are shifted by a number derived from `q`, so different queries show different prices. Every body carries `"_replay": true`.

**Test cases (server on a random port, driven by `SerpApiClient`):**
- A plain search returns rows the extractor accepts.
- An async submit returns an id; the archive says `Processing`, then `Success` after the delay (use `processing_seconds=0.3`).
- The same parameters give the same id.
- The account count falls by one per new search.
- With `searches_left=3`, `check_quota` for 20 is refused.

- [ ] Write the failing tests, implement, run `.venv/Scripts/python -m pytest -q`. Expected: all pass.
- [ ] Commit: `feat: replay server standing in for SerpApi`.

### Task 5: Docker environment, hook and connection type (01:30 to 03:30)

Needs Docker Desktop running. This task is gate test G3 from the spec.

**Files:** Create `docker/Dockerfile`, `docker-compose.yml`, `serpapi_provider/hooks/serpapi.py`, `tests/airflow/test_hook.py`, `tests/airflow/conftest.py`.

**Interfaces produced:**
- `class SerpApiHook(BaseHook)` with `conn_name_attr = "serpapi_conn_id"`, `default_conn_name = "serpapi_default"`, `conn_type = "serpapi"`, `hook_name = "SerpApi"`
- `SerpApiHook(serpapi_conn_id="serpapi_default")`; methods `get_client() -> SerpApiClient`, `search(params)`, `submit_async(params)`, `archive(search_id)`, `account()`, `quota() -> QuotaStatus`, `test_connection() -> tuple[bool, str]`
- `get_ui_field_behaviour()` hides `port`, `schema`, `login`, `extra` and relabels `password` to "API key" and `host` to "Base URL (optional)"
- `client_settings(conn) -> tuple[str, str]` returning key and base URL, shared with the trigger

**Compose design:**
- Service `airflow`: built from `docker/Dockerfile` (`FROM apache/airflow:3.3.2`, then `pip install -e /opt/provider[dev]`), the repository bind-mounted at `/opt/provider`, command `standalone`, port 8080.
- Environment: `AIRFLOW__CORE__LOAD_EXAMPLES=False`, `AIRFLOW__CORE__DAGS_FOLDER=/opt/provider/serpapi_provider/example_dags`, `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True`, and `AIRFLOW_CONN_SERPAPI_DEFAULT` as a JSON connection built from `SERPAPI_KEY` in `.env`.
- Service `replay` under profile `replay`: runs `python -m replay.server --host 0.0.0.0 --port 8089`. A compose override file `docker-compose.replay.yml` points `AIRFLOW_CONN_SERPAPI_DEFAULT` at `http://replay:8089` with a dummy key.

- [ ] Write the Dockerfile and compose files. Run `docker compose -f docker-compose.yml -f docker-compose.replay.yml --profile replay up -d --build`.
- [ ] Verify: `http://localhost:8080` loads. `docker compose exec airflow airflow providers list` shows `airflow-provider-serpapi`. `docker stats --no-stream` shows the container under 4 GB.
- [ ] Implement the hook. Import `BaseHook` from `airflow.sdk.bases.hook`, falling back to `airflow.hooks.base`.
- [ ] Tests in the container (`docker compose exec airflow pytest /opt/provider/tests/airflow -q`): the hook builds a client from a connection; `test_connection` returns `(True, ...)` against the replay server; `quota()` returns a `QuotaStatus`.
- [ ] Verify by hand: the connection form in the interface lists "SerpApi" with the relabelled fields. If it does not, record that in the README and keep the environment-variable connection.
- [ ] Commit: `feat: SerpApi hook, connection type and Docker demo environment`.

**If Docker cannot be started by 02:30:** install Airflow in the Debian WSL distribution instead (`python3 -m venv`, `pip install apache-airflow==3.3.2`, `airflow standalone`) and tell the team the one-command demo is at risk.

### Task 6: Trigger, batch operator and single search operator (03:30 to 06:00)

**Files:** Create `serpapi_provider/triggers/batch.py`, `serpapi_provider/operators/batch.py`, `serpapi_provider/operators/search.py`, `tests/airflow/test_trigger.py`, `tests/airflow/test_batch_operator.py`, `tests/airflow/test_search_operator.py`.

**Interfaces produced:**
- `class SerpApiBatchTrigger(BaseTrigger)`: `__init__(serpapi_conn_id: str, search_ids: list[str], poll_interval: float = 2.0, max_concurrency: int = 10, timeout: float = 600.0)`; `serialize()` returns the class path and those arguments, never the key; `run()` yields one `TriggerEvent({"status": "done" | "timeout", "results": [{"search_id", "status", "error"}], "pending": [...]})`.
- `class SerpApiBatchSearchOperator(BaseOperator)`: `__init__(*, searches: list[dict], serpapi_conn_id="serpapi_default", extract: bool = True, reserve_searches: int = 0, max_batch_size: int = 50, on_insufficient_quota: "fail" | "skip" = "fail", on_search_error: "fail" | "skip" = "fail", poll_interval=2.0, timeout=600.0, **kwargs)`; `template_fields = ("searches",)`; `execute(context)` validates, guards, submits and defers; `execute_complete(context, event)` collects and returns rows.
- `class SerpApiSearchOperator(BaseOperator)`: `__init__(*, search: dict, serpapi_conn_id="serpapi_default", extract: bool = True, **kwargs)`; returns rows, or the raw body when `extract` is false or the engine has no extractor.

**Test cases (in the container, against the replay server or a mocked client):**
- Trigger: with the replay delay at 0.5 seconds it yields `done` with every id finished; with a 0.2-second timeout it yields `timeout` and lists the pending ids; `serialize()` contains no key; a failed search is reported with its error and does not stop the others.
- Batch operator: an empty batch, a batch of 51 and a search with `no_cache` each raise `ValueError` before any request; a quota shortfall raises `AirflowFailException`, or `AirflowSkipException` with `on_insufficient_quota="skip"`; `execute` raises `TaskDeferred` carrying a `SerpApiBatchTrigger` with one id per search; `execute_complete` with a `done` event returns rows that carry `search_id`, `engine`, `query` and `fetched_at`; with a failed search it raises under `"fail"` and returns the rest under `"skip"`; a `timeout` event raises and names the pending ids.
- Single search operator: returns rows for `google_shopping`; returns the raw body for an engine with no extractor.

- [ ] Write the failing tests, implement, run `docker compose exec airflow pytest /opt/provider/tests/airflow -q`. Expected: all pass.
- [ ] In the trigger, read the connection with `await BaseHook.aget_connection(...)` when that exists, otherwise `await sync_to_async(BaseHook.get_connection)(...)`.
- [ ] Commit: `feat: batch search operator with deferral and quota guard`.

### Task 7: Example pipelines and the end-to-end replay run (06:00 to 07:30)

**Files:** Create `serpapi_provider/example_dags/price_tracker.py`, `serpapi_provider/example_dags/quota_guard_demo.py`, `tests/airflow/test_example_dags.py`.

**Design:**
- `price_tracker`: a list of 20 product queries with `gl=in`; `SerpApiBatchSearchOperator`; a `@task` that writes the rows to SQLite at `/opt/provider/data/prices.db` in table `prices`, with primary key `(run_date, query, position)` and `INSERT OR REPLACE`; a `@task` that logs the lowest price per query as a table. Manual trigger only.
- `quota_guard_demo`: the same batch with `reserve_searches=100000`, so the guard refuses the run and the log shows why.

- [ ] Write the DAG files. Test: both import with no errors (`DagBag`) and have the expected task ids.
- [ ] Run `docker compose exec airflow airflow dags test price_tracker` in replay mode. Expected: success, 20 queries in the table.
- [ ] Trigger `price_tracker` from the interface and confirm the batch task shows as deferred, then succeeds. Trigger `quota_guard_demo` and confirm it fails fast with the quota message.
- [ ] Commit: `feat: example price tracker and quota guard pipelines`.

### Task 8: Live checks with the real key (07:30 to 09:00)

Needs `SERPAPI_KEY` in `.env`. This task is gate test G2 from the spec. Budget: 50 searches.

**Files:** Create `scripts/day1_async_check.py`, `scripts/capture_fixtures.py`, `docs/async-check.md`.

- [ ] `day1_async_check.py`: read the account, submit one async Google Shopping search, poll the archive every second logging each status and the elapsed time, read the account again, submit the identical search again and repeat. It prints numbers only, never the key. Up to 4 searches.
- [ ] Write the results to `docs/async-check.md`: accepted or not, searches counted, time to finish, what the repeat returned.
- [ ] **If async is refused or not counted once:** change the trigger to run normal searches concurrently, as section 8 of the spec allows, and rerun Task 6's tests.
- [ ] `capture_fixtures.py`: save real responses for 6 of the demo queries into `replay/fixtures/`, with `search_metadata` and `search_parameters` stripped of anything account-specific. Run the secret-scan test afterwards.
- [ ] Start the stack without the replay override and run `price_tracker` once live. Record the searches left before and after.
- [ ] Commit: `docs: live async check; test: real fixtures`.

**If there is no key by 09:00:** skip this task, ship in replay mode, and state in the README that the live path was not exercised.

### Task 9: README, comparison, demo script and final checks (09:00 to 11:00)

**Files:** Create `README.md`, `docs/comparison.md`, `docs/demo-script.md`, `tests/unit/test_no_secrets.py`.

- [ ] `README.md`: one-paragraph purpose; the "community project, not official" line; a 60-second quick start in replay mode; live setup with `.env`; the three classes with one example each; the measured async-check results; "when not to use this"; known limits from the spec; licence; how AI tools were used; that the project was started during the hackathon.
- [ ] `docs/comparison.md`: the same 20-product pipeline written the plain way, beside the provider version, and a table of what the plain version lacks: worker slots held, no quota guard, no single place for the key, no per-search error report.
- [ ] `docs/demo-script.md`: a timed script under 3 minutes. Connection form and Test button; trigger `price_tracker`; the deferred state; the price table; `quota_guard_demo` refusing; the live run's searches-left before and after; the comparison table.
- [ ] `test_no_secrets.py`: fails if any tracked file contains a 64-character hex string or the value of `SERPAPI_KEY` when that variable is set.
- [ ] Run `.venv/Scripts/python -m pytest -q` and `docker compose exec airflow pytest /opt/provider/tests/airflow -q`. Expected: all pass.
- [ ] From a fresh clone in a temporary folder, follow the README's quick start exactly and confirm it works.
- [ ] Commit: `docs: README, comparison and demo script`.

### Task 10: Publish and hand over (11:00 to 12:00)

- [ ] Add the team's GitHub repository as `origin` and push `main`. If the push asks for a sign-in, the team completes it.
- [ ] Open the repository in a private browser window and confirm it loads without signing in and shows no key.
- [ ] Give the team the submission text: project description, Track 2, how SerpApi is used, the AI-tools disclosure.

### Stretch A: Threshold sensor (only if Task 10 is done by 12:00)

**Files:** `serpapi_provider/core/threshold.py`, `serpapi_provider/sensors/threshold.py`, `tests/unit/test_threshold.py`, `tests/airflow/test_sensor.py`.

**Interfaces:** `crossed(previous_met: bool | None, value: float, threshold: float, direction: "below" | "above", fire_on_first_check: bool = False) -> tuple[bool, bool]` returning whether to fire and the new state. `SerpApiThresholdSensor` as in section 10 of the spec, with state in the Variable `serpapi_threshold__<dag_id>__<task_id>` and `poke_interval` raised to 3600.

### Stretch B: Google Search and Google Jobs extractors

Add both to `EXTRACTORS` with the columns in section 11 of the spec, each with a fixture and a test.

---

## Verification before calling it done

- Both test suites pass.
- A fresh clone runs the replay demo from the README alone.
- No key in the repository, the logs or the video.
- The README states plainly what was and was not exercised live.
