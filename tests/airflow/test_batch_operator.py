import asyncio
import json

import pytest

try:
    from airflow.sdk.exceptions import AirflowException, AirflowFailException, AirflowSkipException, TaskDeferred
except ImportError:
    from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException, TaskDeferred

from replay.server import FAIL_QUERY
from serpapi_provider.operators.batch import SerpApiBatchSearchOperator
from serpapi_provider.triggers.batch import SerpApiBatchTrigger

CONN_ID = "serpapi_test"


def searches(*queries):
    return [{"engine": "google_shopping", "q": query, "gl": "in"} for query in queries]


def operator(**kwargs):
    kwargs.setdefault("task_id", "fetch")
    kwargs.setdefault("serpapi_conn_id", CONN_ID)
    kwargs.setdefault("poll_interval", 0.1)
    return SerpApiBatchSearchOperator(**kwargs)


def defer(op):
    """Run execute() and return the deferral it raises."""
    with pytest.raises(TaskDeferred) as excinfo:
        op.execute({})
    return excinfo.value


def run_trigger(trigger):
    async def first_event():
        async for event in trigger.run():
            return event.payload

    return asyncio.run(first_event())


def run_to_completion(op):
    deferred = defer(op)
    event = run_trigger(deferred.trigger)
    return op.execute_complete({}, event=event, **deferred.kwargs)


def test_execute_submits_the_batch_and_defers_to_the_trigger(replay):
    server = replay()

    deferred = defer(operator(searches=searches("tv", "phone", "laptop")))

    assert isinstance(deferred.trigger, SerpApiBatchTrigger)
    assert len(deferred.trigger.search_ids) == 3
    assert deferred.method_name == "execute_complete"
    assert [item["search"]["q"] for item in deferred.kwargs["submitted"]] == ["tv", "phone", "laptop"]
    assert server.state.searches_left == 247


def test_the_trigger_never_serialises_the_key(replay):
    replay()

    path, kwargs = defer(operator(searches=searches("tv"))).trigger.serialize()

    assert path == "serpapi_provider.triggers.batch.SerpApiBatchTrigger"
    assert set(kwargs) == {"serpapi_conn_id", "search_ids", "poll_interval", "max_concurrency", "timeout"}
    assert "replay-key" not in json.dumps(kwargs)


def test_the_whole_cycle_returns_rows_for_every_search(replay):
    replay(processing_seconds=0.3)

    rows = run_to_completion(operator(searches=searches("tv", "phone", "laptop")))

    assert len(rows) == 24
    assert {row["query"] for row in rows} == {"tv", "phone", "laptop"}
    assert all({"search_id", "engine", "fetched_at", "extracted_price"} <= set(row) for row in rows)


def test_extract_false_returns_the_raw_responses(replay):
    replay(processing_seconds=0)

    bodies = run_to_completion(operator(searches=searches("tv", "phone"), extract=False))

    assert len(bodies) == 2
    assert all("shopping_results" in body for body in bodies)


def test_a_trigger_run_twice_spends_nothing_more(replay):
    server = replay(processing_seconds=0)
    deferred = defer(operator(searches=searches("tv", "phone")))
    left_after_submit = server.state.searches_left

    first = run_trigger(deferred.trigger)
    second = run_trigger(deferred.trigger)

    assert first == second
    assert server.state.searches_left == left_after_submit


@pytest.mark.parametrize(
    ("bad", "message"),
    [
        ([], "non-empty"),
        ([{"engine": "google_shopping", "q": "tv", "no_cache": True}], "no_cache"),
        ([{"engine": "google_shopping", "q": "tv"}] * 51, "limit is 50"),
    ],
)
def test_a_bad_batch_is_refused_before_any_request(replay, bad, message):
    server = replay()

    with pytest.raises(ValueError, match=message):
        operator(searches=bad).execute({})

    assert server.state.searches == {}


def test_the_quota_guard_fails_a_run_the_account_cannot_afford(replay):
    server = replay(searches_left=3)

    with pytest.raises(AirflowFailException, match="wants 5 searches but the account has 3 left"):
        operator(searches=searches("a", "b", "c", "d", "e")).execute({})

    assert server.state.searches_left == 3


def test_the_quota_guard_can_skip_and_honours_the_reserve(replay):
    server = replay(searches_left=10)

    with pytest.raises(AirflowSkipException, match="keeping 8 in reserve"):
        operator(searches=searches("a", "b", "c"), reserve_searches=8, on_insufficient_quota="skip").execute({})

    assert server.state.searches_left == 10


def test_a_failed_search_fails_the_task_and_names_it(replay):
    replay(processing_seconds=0)

    with pytest.raises(AirflowException, match="1 of 2 searches failed") as excinfo:
        run_to_completion(operator(searches=searches("tv", FAIL_QUERY)))

    assert FAIL_QUERY in str(excinfo.value)
    assert "set up to fail" in str(excinfo.value)


def test_on_search_error_skip_returns_the_rest(replay):
    replay(processing_seconds=0)

    rows = run_to_completion(operator(searches=searches("tv", FAIL_QUERY), on_search_error="skip"))

    assert len(rows) == 8
    assert {row["query"] for row in rows} == {"tv"}


def test_a_timeout_fails_and_names_the_pending_ids(replay):
    replay(processing_seconds=30)
    op = operator(searches=searches("tv"), timeout=0.3)
    deferred = defer(op)

    event = run_trigger(deferred.trigger)

    assert event["status"] == "timeout"
    with pytest.raises(AirflowException, match="Timed out") as excinfo:
        op.execute_complete({}, event=event, **deferred.kwargs)
    assert deferred.trigger.search_ids[0] in str(excinfo.value)


def test_a_bad_option_is_refused_when_the_operator_is_built():
    with pytest.raises(ValueError, match="on_search_error"):
        operator(searches=searches("tv"), on_search_error="ignore")
