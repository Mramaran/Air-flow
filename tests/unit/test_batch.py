import asyncio

import pytest

from serpapi_provider.core.batch import validate_batch, wait_for_searches
from serpapi_provider.core.errors import SerpApiAuthError, SerpApiHttpError

SEARCH = {"engine": "google_shopping", "q": "tv"}


class FakeArchive:
    """Answers each id from a scripted list of statuses, repeating the last one."""

    def __init__(self, script):
        self.script = {search_id: list(steps) for search_id, steps in script.items()}
        self.calls = []
        self.in_flight = 0
        self.max_in_flight = 0

    async def archive(self, search_id):
        self.calls.append(search_id)
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        await asyncio.sleep(0)
        self.in_flight -= 1
        steps = self.script[search_id]
        step = steps.pop(0) if len(steps) > 1 else steps[0]
        if isinstance(step, Exception):
            raise step
        body = {"search_metadata": {"id": search_id, "status": step}}
        if step == "Error":
            body["error"] = "Invalid engine."
        return body


def wait(client, ids, **kwargs):
    kwargs.setdefault("poll_interval", 0.01)
    return asyncio.run(wait_for_searches(client, ids, **kwargs))


def test_validate_accepts_a_good_batch_and_copies_it():
    searches = [SEARCH, {"engine": "google", "q": "x"}]

    checked = validate_batch(searches)

    assert checked == searches
    assert checked[0] is not searches[0]


@pytest.mark.parametrize(
    ("searches", "message"),
    [
        ([], "non-empty"),
        (None, "non-empty"),
        ("google", "non-empty"),
        (["google"], "not a dictionary"),
        ([{"q": "x"}], "no 'engine'"),
        ([{**SEARCH, "no_cache": True}], "no_cache"),
        ([{**SEARCH, "async": True}], "sets 'async'"),
    ],
)
def test_validate_refuses_bad_batches(searches, message):
    with pytest.raises(ValueError, match=message):
        validate_batch(searches)


def test_validate_refuses_a_batch_over_the_limit():
    with pytest.raises(ValueError, match="51 searches and the limit is 50"):
        validate_batch([SEARCH] * 51)
    assert len(validate_batch([SEARCH] * 50)) == 50


def test_wait_returns_done_when_every_search_finishes():
    client = FakeArchive({"a": ["Processing", "Success"], "b": ["Queued", "Processing", "Cached"]})

    event = wait(client, ["a", "b"])

    assert event == {
        "status": "done",
        "results": [
            {"search_id": "a", "status": "Success", "error": None},
            {"search_id": "b", "status": "Cached", "error": None},
        ],
        "pending": [],
    }
    assert client.calls.count("a") == 2
    assert client.calls.count("b") == 3


def test_wait_reports_a_failed_search_without_stopping_the_others():
    client = FakeArchive({"a": ["Error"], "b": ["Processing", "Success"]})

    event = wait(client, ["a", "b"])

    assert event["status"] == "done"
    assert event["results"][0] == {"search_id": "a", "status": "Error", "error": "Invalid engine."}
    assert event["results"][1]["status"] == "Success"


def test_wait_times_out_and_lists_what_is_still_pending():
    client = FakeArchive({"a": ["Success"], "b": ["Processing"]})

    event = wait(client, ["a", "b"], timeout=0.05)

    assert event["status"] == "timeout"
    assert event["pending"] == ["b"]
    assert [result["search_id"] for result in event["results"]] == ["a"]


def test_wait_retries_a_passing_error_and_gives_up_after_five_in_a_row():
    recovers = FakeArchive({"a": [SerpApiHttpError("HTTP 500"), SerpApiHttpError("HTTP 500"), "Success"]})
    never = FakeArchive({"a": [SerpApiHttpError("HTTP 404")]})

    assert wait(recovers, ["a"])["results"][0]["status"] == "Success"
    event = wait(never, ["a"])

    assert event["status"] == "done"
    assert event["results"] == [{"search_id": "a", "status": "Error", "error": "HTTP 404"}]
    assert never.calls.count("a") == 5


def test_wait_stops_at_once_on_a_bad_key():
    client = FakeArchive({"a": [SerpApiAuthError("Invalid API key.")]})

    with pytest.raises(SerpApiAuthError):
        wait(client, ["a"])


def test_wait_limits_simultaneous_requests_and_ignores_duplicate_ids():
    ids = [f"id{n}" for n in range(12)]
    client = FakeArchive({search_id: ["Success"] for search_id in ids})

    event = wait(client, ids + ["id0"], max_concurrency=3)

    assert client.max_in_flight <= 3
    assert len(event["results"]) == 12
    assert len(client.calls) == 12
