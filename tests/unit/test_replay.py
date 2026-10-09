import threading
import time

import pytest

from replay.server import FAIL_QUERY, make_server, search_id_for
from serpapi_provider.core.client import SerpApiClient, status_of
from serpapi_provider.core.errors import SerpApiHttpError, SerpApiRateLimitError, SerpApiSearchError
from serpapi_provider.core.extractors import extract_rows
from serpapi_provider.core.quota import QuotaStatus, check_quota

SEARCH = {"engine": "google_shopping", "q": "iphone 16 128gb", "gl": "in"}


@pytest.fixture
def replay():
    def start(**kwargs):
        server = make_server(processing_seconds=kwargs.pop("processing_seconds", 0.3), **kwargs)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        servers.append(server)
        return SerpApiClient("replay-key", base_url=f"http://127.0.0.1:{server.server_address[1]}")

    servers = []
    yield start
    for server in servers:
        server.shutdown()
        server.server_close()


def test_a_plain_search_returns_rows_the_extractor_accepts(replay):
    body = replay().search(SEARCH)
    rows = extract_rows("google_shopping", body)

    assert body["_replay"] is True
    assert status_of(body) == "Success"
    assert len(rows) == 8
    assert rows[0]["query"] == "iphone 16 128gb"
    assert rows[0]["title"] == "iphone 16 128gb (replay offer 1)"
    assert rows[0]["search_id"] == search_id_for(SEARCH)


def test_an_async_search_is_processing_and_then_succeeds(replay):
    client = replay(processing_seconds=0.3)

    search_id = client.submit_async(SEARCH)
    first = client.archive(search_id)
    time.sleep(0.4)
    second = client.archive(search_id)

    assert status_of(first) == "Processing"
    assert "shopping_results" not in first
    assert status_of(second) == "Success"
    assert len(second["shopping_results"]) == 8
    assert "async" not in second["search_parameters"]


def test_the_same_parameters_give_the_same_id_and_are_not_counted_twice(replay):
    client = replay()

    first = client.submit_async(SEARCH)
    second = client.submit_async(dict(reversed(list(SEARCH.items()))))

    assert first == second
    assert client.account()["total_searches_left"] == 249


def test_different_queries_get_different_prices(replay):
    client = replay()

    a = extract_rows("google_shopping", client.search({**SEARCH, "q": "pixel 10"}))
    b = extract_rows("google_shopping", client.search({**SEARCH, "q": "galaxy s26"}))

    assert a[0]["extracted_price"] != b[0]["extracted_price"]
    assert a[0]["price"].startswith("₹")


def test_the_account_count_falls_by_one_per_new_search(replay):
    client = replay()

    before = client.account()
    client.search({**SEARCH, "q": "one"})
    client.search({**SEARCH, "q": "two"})
    client.search({**SEARCH, "q": "two"})
    after = client.account()

    assert before["total_searches_left"] == 250
    assert after["total_searches_left"] == 248
    assert after["this_hour_searches"] == 2
    assert after["account_rate_limit_per_hour"] == 50


def test_a_low_quota_is_refused_by_the_quota_check(replay):
    client = replay(searches_left=3)

    decision = check_quota(QuotaStatus.from_account(client.account()), wanted=20)

    assert not decision.ok
    assert "has 3 left" in decision.reason


def test_running_out_of_searches_is_a_rate_limit_error(replay):
    client = replay(searches_left=1)
    client.search({**SEARCH, "q": "one"})

    with pytest.raises(SerpApiRateLimitError):
        client.search({**SEARCH, "q": "two"})


def test_the_fail_query_fails_as_a_search_error_and_as_an_archive_status(replay):
    client = replay(processing_seconds=0)

    with pytest.raises(SerpApiSearchError, match="set up to fail"):
        client.search({**SEARCH, "q": FAIL_QUERY})
    archived = client.archive(client.submit_async({**SEARCH, "q": FAIL_QUERY}))

    assert status_of(archived) == "Error"
    assert "set up to fail" in archived["error"]


def test_an_engine_with_no_saved_response_is_an_error(replay):
    with pytest.raises(SerpApiSearchError, match="no saved response"):
        replay().search({"engine": "google_flights", "q": "x"})


def test_an_unknown_search_id_is_not_found(replay):
    with pytest.raises(SerpApiHttpError, match="404"):
        replay().archive("doesnotexist")
