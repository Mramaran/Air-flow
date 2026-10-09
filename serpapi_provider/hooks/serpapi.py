"""Airflow hook and connection type for SerpApi."""

from __future__ import annotations

from typing import Any

try:  # Airflow 3
    from airflow.sdk.bases.hook import BaseHook
except ImportError:  # pragma: no cover - older Airflow
    from airflow.hooks.base import BaseHook

from serpapi_provider.core.client import DEFAULT_BASE_URL, SerpApiClient
from serpapi_provider.core.errors import SerpApiAuthError, SerpApiError
from serpapi_provider.core.quota import QuotaStatus


def client_settings(conn: Any) -> tuple[str, str]:
    """The API key and base URL held by an Airflow connection.

    The key is the connection's password. The host is optional and defaults to
    https://serpapi.com; pointing it at the replay server runs everything offline.
    """
    api_key = (getattr(conn, "password", None) or "").strip()
    if not api_key:
        raise SerpApiAuthError(
            f"Connection '{getattr(conn, 'conn_id', '?')}' has no API key. "
            "Put the SerpApi key in the connection's password field."
        )
    host = (getattr(conn, "host", None) or "").strip()
    if not host:
        base_url = DEFAULT_BASE_URL
    elif "://" in host:
        base_url = host
    else:
        base_url = "https://" + host
    port = getattr(conn, "port", None)
    if port and base_url.count(":") < 2:
        base_url = f"{base_url.rstrip('/')}:{port}"
    return api_key, base_url.rstrip("/")


class SerpApiHook(BaseHook):
    """Talks to SerpApi using the key stored in an Airflow connection.

    :param serpapi_conn_id: the connection holding the key. Default ``serpapi_default``.
    :param timeout: seconds to wait for one HTTP request.
    """

    conn_name_attr = "serpapi_conn_id"
    default_conn_name = "serpapi_default"
    conn_type = "serpapi"
    hook_name = "SerpApi"

    def __init__(self, serpapi_conn_id: str = default_conn_name, timeout: float = 30.0) -> None:
        super().__init__()
        self.serpapi_conn_id = serpapi_conn_id
        self.timeout = timeout
        self._client: SerpApiClient | None = None

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Show only the fields that mean something for SerpApi in the connection form."""
        return {
            "hidden_fields": ["port", "schema", "login", "extra"],
            "relabeling": {"password": "API key", "host": "Base URL (optional)"},
            "placeholders": {"host": DEFAULT_BASE_URL, "password": "Your SerpApi private key"},
        }

    def get_conn(self) -> SerpApiClient:
        if self._client is None:
            api_key, base_url = client_settings(self.get_connection(self.serpapi_conn_id))
            self._client = SerpApiClient(api_key, base_url=base_url, timeout=self.timeout)
        return self._client

    def search(self, params: dict[str, Any]) -> dict[str, Any]:
        """Run one search and wait for its result."""
        return self.get_conn().search(params)

    def submit_async(self, params: dict[str, Any]) -> str:
        """Submit a search with async=true and return its search id."""
        return self.get_conn().submit_async(params)

    def archive(self, search_id: str) -> dict[str, Any]:
        """Look up a search by id. SerpApi does not count this as a search."""
        return self.get_conn().archive(search_id)

    def account(self) -> dict[str, Any]:
        """Account usage. SerpApi does not count this as a search."""
        return self.get_conn().account()

    def quota(self) -> QuotaStatus:
        return QuotaStatus.from_account(self.account())

    def test_connection(self) -> tuple[bool, str]:
        """Back the connection form's Test button. Uses the free account endpoint."""
        try:
            status = self.quota()
        except SerpApiError as exc:
            return False, str(exc)
        return True, (
            f"Connected. {status.searches_left} searches left; "
            f"{status.used_this_hour} of {status.hourly_limit} used this hour."
        )
