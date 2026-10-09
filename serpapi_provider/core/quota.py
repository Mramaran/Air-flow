"""Quota arithmetic. Decides whether a run may spend the searches it wants. No Airflow imports."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class QuotaStatus:
    """The numbers the guard needs, read from SerpApi's free account endpoint."""

    searches_left: int
    used_this_hour: int
    hourly_limit: int

    @classmethod
    def from_account(cls, body: dict[str, Any]) -> "QuotaStatus":
        left = body.get("total_searches_left")
        if left is None:
            left = body.get("plan_searches_left")
        return cls(
            searches_left=int(left or 0),
            used_this_hour=int(body.get("this_hour_searches") or 0),
            hourly_limit=int(body.get("account_rate_limit_per_hour") or 0),
        )

    @property
    def left_this_hour(self) -> int:
        return max(self.hourly_limit - self.used_this_hour, 0)


@dataclass(frozen=True)
class QuotaDecision:
    ok: bool
    reason: str


def check_quota(status: QuotaStatus, wanted: int, reserve: int = 0) -> QuotaDecision:
    """Say whether `wanted` searches fit in the month and in the hour.

    `wanted` is an upper bound: searches SerpApi serves from its cache are free
    and cannot be known in advance. `reserve` searches are kept back for other work.
    """
    if wanted <= 0:
        return QuotaDecision(True, "No searches wanted.")

    spendable = status.searches_left - reserve
    if spendable < wanted:
        held = f" after keeping {reserve} in reserve" if reserve else ""
        return QuotaDecision(
            False,
            f"This run wants {wanted} searches but the account has {status.searches_left} left{held} "
            f"({max(spendable, 0)} usable).",
        )

    if status.left_this_hour < wanted:
        return QuotaDecision(
            False,
            f"This run wants {wanted} searches but only {status.left_this_hour} are left this hour "
            f"({status.used_this_hour} of {status.hourly_limit} used).",
        )

    return QuotaDecision(
        True,
        f"{wanted} searches wanted; {status.searches_left} left this month, "
        f"{status.left_this_hour} left this hour.",
    )
