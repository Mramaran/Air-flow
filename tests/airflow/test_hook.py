from types import SimpleNamespace

import pytest

from serpapi_provider.core.errors import SerpApiAuthError
from serpapi_provider.core.quota import QuotaStatus
from serpapi_provider.hooks.serpapi import SerpApiHook, client_settings

CONN_ID = "serpapi_test"


def conn(**kwargs):
    return SimpleNamespace(**{"conn_id": "c", "password": "secret", "host": None, "port": None, **kwargs})


def test_client_settings_defaults_to_serpapi_com():
    assert client_settings(conn()) == ("secret", "https://serpapi.com")


def test_client_settings_accepts_a_full_url_a_bare_host_and_a_port():
    assert client_settings(conn(host="http://replay:8089/")) == ("secret", "http://replay:8089")
    assert client_settings(conn(host="serpapi.com")) == ("secret", "https://serpapi.com")
    assert client_settings(conn(host="http://localhost", port=8089)) == ("secret", "http://localhost:8089")


def test_client_settings_refuses_a_connection_with_no_key():
    with pytest.raises(SerpApiAuthError, match="no API key"):
        client_settings(conn(password=""))


def test_the_hook_describes_its_connection_form():
    behaviour = SerpApiHook.get_ui_field_behaviour()

    assert SerpApiHook.conn_type == "serpapi"
    assert SerpApiHook.hook_name == "SerpApi"
    assert behaviour["relabeling"]["password"] == "API key"
    assert "login" in behaviour["hidden_fields"]


def test_the_hook_reads_the_connection_and_talks_to_serpapi(replay):
    replay()
    hook = SerpApiHook(CONN_ID)

    assert hook.quota() == QuotaStatus(searches_left=250, used_this_hour=0, hourly_limit=50)
    assert len(hook.search({"engine": "google_shopping", "q": "tv"})["shopping_results"]) == 8
    search_id = hook.submit_async({"engine": "google_shopping", "q": "phone"})
    assert hook.archive(search_id)["search_metadata"]["id"] == search_id


def test_test_connection_reports_the_searches_left(replay):
    replay(searches_left=42)

    ok, message = SerpApiHook(CONN_ID).test_connection()

    assert ok is True
    assert "42 searches left" in message


def test_test_connection_reports_failure_without_raising(replay, monkeypatch):
    replay()
    monkeypatch.setenv(
        f"AIRFLOW_CONN_{CONN_ID.upper()}",
        '{"conn_type": "serpapi", "password": "replay-key", "host": "http://127.0.0.1:9"}',
    )

    ok, message = SerpApiHook(CONN_ID, timeout=2).test_connection()

    assert ok is False
    assert "Could not reach SerpApi" in message
    assert "replay-key" not in message
