"""Crossing logic for the threshold sensor. No Airflow imports."""

from __future__ import annotations

from typing import Any

DIRECTIONS = ("below", "above")
AGGREGATES = ("min", "max", "first")

# SerpApi serves identical searches from its cache for an hour, so checking more
# often than this returns the same answer.
CACHE_SECONDS = 3600


def meets(value: float, threshold: float, direction: str) -> bool:
    """Whether the value is past the line."""
    if direction == "below":
        return value < threshold
    if direction == "above":
        return value > threshold
    raise ValueError(f"direction must be one of {DIRECTIONS}, not {direction!r}.")


def crossed(
    previous_met: bool | None,
    value: float,
    threshold: float,
    direction: str = "below",
    fire_on_first_check: bool = False,
) -> tuple[bool, bool]:
    """Decide whether the value has just crossed the line.

    Returns (fire, now_met). It fires when the value is past the line now and was
    not at the previous check. A value that stays past the line does not fire
    again. With no previous check it only fires if fire_on_first_check is set.
    """
    now_met = meets(value, threshold, direction)
    if previous_met is None:
        return (now_met and fire_on_first_check), now_met
    return (now_met and not previous_met), now_met


def reduce_values(rows: list[dict[str, Any]], field: str, aggregate: str = "min") -> float | None:
    """One number from a column of rows, or None if no row has a number there."""
    if aggregate not in AGGREGATES:
        raise ValueError(f"aggregate must be one of {AGGREGATES}, not {aggregate!r}.")
    numbers = [
        float(row[field])
        for row in rows
        if isinstance(row.get(field), (int, float)) and not isinstance(row.get(field), bool)
    ]
    if not numbers:
        return None
    if aggregate == "min":
        return min(numbers)
    if aggregate == "max":
        return max(numbers)
    return numbers[0]


def effective_poke_interval(requested: float) -> float:
    """Never check more often than SerpApi's cache makes useful."""
    return max(float(requested), float(CACHE_SECONDS))


def searches_per_day(poke_interval: float) -> int:
    """The most searches a sensor with this interval can spend in a day."""
    return int(86400 // effective_poke_interval(poke_interval))
