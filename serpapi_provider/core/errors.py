"""Errors raised by the SerpApi client. No Airflow imports."""


class SerpApiError(Exception):
    """Base class for every error this package raises about SerpApi."""


class SerpApiAuthError(SerpApiError):
    """The API key is missing or SerpApi rejected it (HTTP 401). Retrying will not help."""


class SerpApiRateLimitError(SerpApiError):
    """HTTP 429: the hourly limit was passed or the account has no searches left."""


class SerpApiHttpError(SerpApiError):
    """Any other HTTP error, an unreadable body, or a network failure."""


class SerpApiSearchError(SerpApiError):
    """SerpApi answered, but the search itself failed."""
