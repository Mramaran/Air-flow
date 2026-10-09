import asyncio
import logging

import httpx
import pytest

from serpapi_provider.core.client import (
    AsyncSerpApiClient,
    SerpApiClient,
    is_failed,
    is_finished,
    status_of,
)
from serpapi_provider.core.errors import (
    SerpApiAuthError,
    SerpApiHttpError,
    SerpApiRateLimitError,
    SerpApiSearchError,
)

KEY = "k" * 64


def client_with(handler):
    return SerpApiClient(KEY, transport=httpx.MockTransport(handler))


def json_response(body, status=200):
    return httpx.Response(status, json=body)


def test_search_sends_params_and_key_and_returns_the_body():
    seen = {}

    def handler(request):
        seen["path"] = request.url.path
        seen["params"] = dict(request.url.params)
        return json_response({"shopping_results": [{"title": "A"}]})

    body = client_with(handler).search({"engine": "google_shopping", "q": "iphone 16", "gl": "in"})

    assert seen["path"] == "/search.json"
    assert seen["params"] == {"engine": "google_shopping", "q": "iphone 16", "gl": "in", "api_key": KEY}
    assert body == {"shopping_results": [{"title": "A"}]}


def test_none_values_are_dropped_and_booleans_are_lowercase():
    seen = {}

    def handler(request):
        seen.update(dict(request.url.params))
        return json_response({})

    client_with(handler).search({"engine": "google", "q": "x", "location": None, "safe": True})

    assert "location" not in seen
    assert seen["safe"] == "true"


def test_submit_async_adds_async_true_and_returns_the_search_id():
    seen = {}

    def handler(request):
        seen.update(dict(request.url.params))
        return json_response({"search_metadata": {"id": "abc123", "status": "Processing"}})

    search_id = client_with(handler).submit_async({"engine": "google_shopping", "q": "tv"})

    assert seen["async"] == "true"
    assert search_id == "abc123"


def test_submit_async_refuses_no_cache_before_any_request():
    def handler(request):
        raise AssertionError("no request should be sent")

    with pytest.raises(ValueError, match="no_cache"):
        client_with(handler).submit_async({"engine": "google", "q": "x", "no_cache": True})


def test_submit_async_without_an_id_is_a_search_error():
    with pytest.raises(SerpApiSearchError, match="no search id"):
        client_with(lambda request: json_response({"search_metadata": {}})).submit_async(
            {"engine": "google", "q": "x"}
        )


def test_archive_requests_the_search_by_id():
    seen = {}

    def handler(request):
        seen["path"] = request.url.path
        return json_response({"search_metadata": {"id": "abc123", "status": "Success"}})

    body = client_with(handler).archive("abc123")

    assert seen["path"] == "/searches/abc123.json"
    assert status_of(body) == "Success"


def test_archive_returns_a_failed_search_instead_of_raising():
    body = client_with(
        lambda request: json_response({"search_metadata": {"status": "Error"}, "error": "Invalid engine."})
    ).archive("abc123")

    assert status_of(body) == "Error"
    assert body["error"] == "Invalid engine."


def test_archive_rejects_an_id_that_could_change_the_path():
    with pytest.raises(ValueError):
        client_with(lambda request: json_response({})).archive("../account")


def test_account_requests_the_account_endpoint():
    seen = {}

    def handler(request):
        seen["path"] = request.url.path
        return json_response({"total_searches_left": 240})

    assert client_with(handler).account() == {"total_searches_left": 240}
    assert seen["path"] == "/account.json"


@pytest.mark.parametrize(
    ("status", "error_class"),
    [(401, SerpApiAuthError), (429, SerpApiRateLimitError), (500, SerpApiHttpError), (404, SerpApiHttpError)],
)
def test_http_errors_map_to_error_classes(status, error_class):
    with pytest.raises(error_class):
        client_with(lambda request: json_response({"error": "nope"}, status)).search({"engine": "google"})


def test_a_network_failure_is_an_http_error_without_the_url():
    def handler(request):
        raise httpx.ConnectError(f"cannot connect to {request.url}")

    with pytest.raises(SerpApiHttpError) as excinfo:
        client_with(handler).search({"engine": "google", "q": "x"})

    assert KEY not in str(excinfo.value)
    assert excinfo.value.__cause__ is None


def test_an_empty_result_is_returned_not_raised():
    body = {"search_metadata": {"status": "Success"}, "error": "Google hasn't returned any results for this query."}

    assert client_with(lambda request: json_response(body)).search({"engine": "google", "q": "zzzz"}) == body


def test_any_other_error_in_the_body_is_a_search_error():
    with pytest.raises(SerpApiSearchError, match="Unsupported"):
        client_with(lambda request: json_response({"error": "Unsupported `foo` engine."})).search(
            {"engine": "foo"}
        )


def test_a_body_that_is_not_json_is_an_http_error():
    with pytest.raises(SerpApiHttpError, match="not a JSON object"):
        client_with(lambda request: httpx.Response(200, text="<html>")).search({"engine": "google"})


def test_a_missing_key_is_refused():
    with pytest.raises(SerpApiAuthError):
        SerpApiClient("")


def test_the_key_never_appears_in_errors_or_logs(caplog):
    caplog.set_level(logging.DEBUG)

    def echo_key(request):
        return json_response({"error": f"Invalid API key: {request.url.params['api_key']}"}, 401)

    with pytest.raises(SerpApiAuthError) as excinfo:
        client_with(echo_key).search({"engine": "google", "q": "x"})
    client_with(lambda request: json_response({"ok": True})).search({"engine": "google", "q": "x"})

    assert KEY not in str(excinfo.value)
    assert "***" in str(excinfo.value)
    assert all(KEY not in record.getMessage() for record in caplog.records)


def test_status_helpers():
    assert is_finished("Success") and is_finished("Cached")
    assert not is_finished("Processing")
    assert is_failed("Error") and is_failed("")
    assert not is_failed("Queued") and not is_failed("Processing") and not is_failed("Success")
    assert status_of({}) == ""


def test_the_async_client_matches_the_sync_one():
    def handler(request):
        if request.url.path == "/searches/abc123.json":
            return json_response({"search_metadata": {"id": "abc123", "status": "Success"}})
        if request.url.path == "/account.json":
            return json_response({"total_searches_left": 7})
        params = dict(request.url.params)
        assert params["api_key"] == KEY
        if params.get("async") == "true":
            return json_response({"search_metadata": {"id": "abc123", "status": "Processing"}})
        return json_response({"shopping_results": []})

    async def run():
        async with AsyncSerpApiClient(KEY, transport=httpx.MockTransport(handler)) as client:
            return (
                await client.search({"engine": "google_shopping", "q": "tv"}),
                await client.submit_async({"engine": "google_shopping", "q": "tv"}),
                await client.archive("abc123"),
                await client.account(),
            )

    search, search_id, archive, account = asyncio.run(run())

    assert search == {"shopping_results": []}
    assert search_id == "abc123"
    assert status_of(archive) == "Success"
    assert account == {"total_searches_left": 7}
