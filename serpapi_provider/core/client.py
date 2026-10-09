"""HTTP client for SerpApi. The only module that builds SerpApi URLs. No Airflow imports.

SerpApi takes the API key as a query parameter, so the key is part of every URL.
Nothing here logs a URL, and error text has the key removed.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import httpx

from serpapi_provider.core.errors import (
    SerpApiAuthError,
    SerpApiHttpError,
    SerpApiRateLimitError,
    SerpApiSearchError,
)

# httpx logs every request URL at INFO level, and the URL contains the key.
logging.getLogger("httpx").setLevel(logging.WARNING)

DEFAULT_BASE_URL = "https://serpapi.com"
FINISHED = frozenset({"Success", "Cached"})
IN_PROGRESS = frozenset({"Queued", "Processing"})

_NO_RESULTS = "hasn't returned any results"
_SEARCH_ID = re.compile(r"[A-Za-z0-9_-]+")


def status_of(body: dict[str, Any]) -> str:
    """The search status in a SerpApi response, or an empty string."""
    return str((body.get("search_metadata") or {}).get("status") or "")


def is_finished(status: str) -> bool:
    return status in FINISHED


def is_failed(status: str) -> bool:
    """True for any status that is neither finished nor still in progress."""
    return status not in FINISHED and status not in IN_PROGRESS


def _redact(text: str, api_key: str) -> str:
    return text.replace(api_key, "***") if api_key else text


def _to_query_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _query(params: dict[str, Any], api_key: str) -> dict[str, str]:
    query = {key: _to_query_value(value) for key, value in params.items() if value is not None}
    query["api_key"] = api_key
    return query


def _check_search_id(search_id: str) -> str:
    if not _SEARCH_ID.fullmatch(search_id or ""):
        raise ValueError("A SerpApi search id may contain only letters, digits, '-' and '_'.")
    return search_id


def _async_params(params: dict[str, Any]) -> dict[str, Any]:
    if "no_cache" in params:
        raise ValueError("SerpApi does not allow 'no_cache' together with 'async'. Remove 'no_cache'.")
    return {**params, "async": True}


def _search_id_from(body: dict[str, Any]) -> str:
    search_id = (body.get("search_metadata") or {}).get("id")
    if not search_id:
        raise SerpApiSearchError("SerpApi accepted the search but returned no search id.")
    return str(search_id)


def _handle(response: httpx.Response, api_key: str, raise_on_search_error: bool) -> dict[str, Any]:
    try:
        body = response.json()
    except ValueError:
        body = None
    message = body.get("error") if isinstance(body, dict) else None
    message = _redact(str(message), api_key) if message else None

    code = response.status_code
    if code == 401:
        raise SerpApiAuthError(message or "SerpApi rejected the API key (HTTP 401).")
    if code == 429:
        raise SerpApiRateLimitError(
            message or "SerpApi returned HTTP 429: the hourly limit was passed or no searches are left."
        )
    if code >= 400:
        raise SerpApiHttpError(f"SerpApi returned HTTP {code}" + (f": {message}" if message else "."))
    if not isinstance(body, dict):
        raise SerpApiHttpError("SerpApi returned a body that is not a JSON object.")
    if message and raise_on_search_error and _NO_RESULTS not in message:
        raise SerpApiSearchError(message)
    return body


def _require_key(api_key: str) -> str:
    if not api_key:
        raise SerpApiAuthError("No SerpApi API key was provided.")
    return api_key


def _unreachable(exc: Exception) -> SerpApiHttpError:
    # The exception text can include the request URL, which contains the key. Report the type only.
    return SerpApiHttpError(f"Could not reach SerpApi ({type(exc).__name__}).")


class SerpApiClient:
    """Blocking client. Use it on an Airflow worker or in a script."""

    def __init__(
        self,
        api_key: str,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = 30.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._api_key = _require_key(api_key)
        self._base_url = (base_url or DEFAULT_BASE_URL).rstrip("/")
        self._http = httpx.Client(timeout=timeout, transport=transport)

    def _get(self, path: str, params: dict[str, Any], raise_on_search_error: bool = True) -> dict[str, Any]:
        try:
            response = self._http.get(self._base_url + path, params=_query(params, self._api_key))
        except httpx.HTTPError as exc:
            raise _unreachable(exc) from None
        return _handle(response, self._api_key, raise_on_search_error)

    def search(self, params: dict[str, Any]) -> dict[str, Any]:
        """Run one search and wait for its result."""
        return self._get("/search.json", params)

    def submit_async(self, params: dict[str, Any]) -> str:
        """Submit a search with async=true and return its search id at once."""
        return _search_id_from(self._get("/search.json", _async_params(params)))

    def archive(self, search_id: str) -> dict[str, Any]:
        """Look up a search by id. A failed search is returned, not raised; read its status."""
        return self._get(f"/searches/{_check_search_id(search_id)}.json", {}, raise_on_search_error=False)

    def account(self) -> dict[str, Any]:
        """Account usage. SerpApi does not count this call as a search."""
        return self._get("/account.json", {})

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "SerpApiClient":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


class AsyncSerpApiClient:
    """Asynchronous client, for use inside an Airflow trigger."""

    def __init__(
        self,
        api_key: str,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._api_key = _require_key(api_key)
        self._base_url = (base_url or DEFAULT_BASE_URL).rstrip("/")
        self._http = httpx.AsyncClient(timeout=timeout, transport=transport)

    async def _get(
        self, path: str, params: dict[str, Any], raise_on_search_error: bool = True
    ) -> dict[str, Any]:
        try:
            response = await self._http.get(self._base_url + path, params=_query(params, self._api_key))
        except httpx.HTTPError as exc:
            raise _unreachable(exc) from None
        return _handle(response, self._api_key, raise_on_search_error)

    async def search(self, params: dict[str, Any]) -> dict[str, Any]:
        return await self._get("/search.json", params)

    async def submit_async(self, params: dict[str, Any]) -> str:
        return _search_id_from(await self._get("/search.json", _async_params(params)))

    async def archive(self, search_id: str) -> dict[str, Any]:
        return await self._get(
            f"/searches/{_check_search_id(search_id)}.json", {}, raise_on_search_error=False
        )

    async def account(self) -> dict[str, Any]:
        return await self._get("/account.json", {})

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> "AsyncSerpApiClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()
