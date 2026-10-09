"""Sensor that succeeds when a number from a SerpApi search crosses a threshold."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any

try:  # Airflow 3
    from airflow.sdk import BaseSensorOperator, Variable
except ImportError:  # pragma: no cover - older Airflow
    from airflow.models import Variable
    from airflow.sensors.base import BaseSensorOperator

from serpapi_provider.core.extractors import extract_rows, has_extractor
from serpapi_provider.core.quota import check_quota
from serpapi_provider.core.threshold import (
    AGGREGATES,
    CACHE_SECONDS,
    DIRECTIONS,
    crossed,
    effective_poke_interval,
    reduce_values,
    searches_per_day,
)
from serpapi_provider.hooks.serpapi import SerpApiHook


class SerpApiThresholdSensor(BaseSensorOperator):
    """Succeed when a value from a SerpApi search crosses a line, for example a price drop.

    Each check runs the search, turns the response into rows, and reduces one column
    to a single number. The sensor succeeds when that number is past the threshold
    now and was not at the previous check, so a value that simply stays past the line
    does not fire again. The previous state is kept in the Airflow Variable
    ``serpapi_threshold__<dag_id>__<task_id>``.

    SerpApi serves identical searches from its cache for an hour, so a
    ``poke_interval`` under 3600 seconds is raised to 3600. The sensor defaults to
    reschedule mode, so it holds a worker only while it checks.

    :param search: a SerpApi parameter dictionary. Its engine needs a row extractor. Templated.
    :param field: the row column to read. Default ``extracted_price``.
    :param aggregate: ``"min"`` (default), ``"max"`` or ``"first"`` across the rows.
    :param threshold: the line. Templated.
    :param direction: ``"below"`` (default) or ``"above"``.
    :param fire_on_first_check: succeed on the very first check if the value is
        already past the line. Default False: the first check only records the state.
    :param serpapi_conn_id: the SerpApi connection. Default ``serpapi_default``.
    :param reserve_searches: searches to keep back. A check that would dip into them is skipped.
    """

    template_fields: Sequence[str] = ("search", "threshold")
    ui_color = "#e8f1ff"

    def __init__(
        self,
        *,
        search: dict[str, Any],
        threshold: float,
        field: str = "extracted_price",
        aggregate: str = "min",
        direction: str = "below",
        fire_on_first_check: bool = False,
        serpapi_conn_id: str = SerpApiHook.default_conn_name,
        reserve_searches: int = 0,
        **kwargs: Any,
    ) -> None:
        if direction not in DIRECTIONS:
            raise ValueError(f"direction must be one of {DIRECTIONS}, not {direction!r}.")
        if aggregate not in AGGREGATES:
            raise ValueError(f"aggregate must be one of {AGGREGATES}, not {aggregate!r}.")
        requested = float(kwargs.get("poke_interval", CACHE_SECONDS))
        kwargs["poke_interval"] = effective_poke_interval(requested)
        kwargs.setdefault("mode", "reschedule")
        super().__init__(**kwargs)
        self.requested_poke_interval = requested
        self.search = search
        self.threshold = threshold
        self.field = field
        self.aggregate = aggregate
        self.direction = direction
        self.fire_on_first_check = fire_on_first_check
        self.serpapi_conn_id = serpapi_conn_id
        self.reserve_searches = reserve_searches

    def state_key(self, context: Any) -> str:
        dag_id = getattr(self, "dag_id", None) or (context.get("dag").dag_id if context.get("dag") else "adhoc")
        return f"serpapi_threshold__{dag_id}__{self.task_id}"

    def load_state(self, key: str) -> dict[str, Any] | None:
        raw = Variable.get(key, default=None)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except (TypeError, ValueError):
            return None

    def save_state(self, key: str, state: dict[str, Any]) -> None:
        Variable.set(key, json.dumps(state))

    def poke(self, context: Any) -> bool:
        engine = self.search.get("engine") if isinstance(self.search, dict) else None
        if not has_extractor(engine):
            raise ValueError(f"The threshold sensor needs an engine with a row extractor; got {engine!r}.")
        if self.requested_poke_interval < CACHE_SECONDS:
            self.log.info(
                "poke_interval was raised from %.0fs to %ds: SerpApi caches identical searches for an hour.",
                self.requested_poke_interval,
                CACHE_SECONDS,
            )
        self.log.info(
            "This sensor spends at most %d searches a day.", searches_per_day(float(self.poke_interval))
        )

        hook = SerpApiHook(self.serpapi_conn_id)
        decision = check_quota(hook.quota(), wanted=1, reserve=self.reserve_searches)
        if not decision.ok:
            self.log.warning("Quota guard skipped this check. %s", decision.reason)
            return False

        rows = extract_rows(engine, hook.search(self.search), query=self.search.get("q"))
        value = reduce_values(rows, self.field, self.aggregate)
        if value is None:
            self.log.warning("No number found in column %r across %d rows. Will check again.", self.field, len(rows))
            return False

        threshold = float(self.threshold)
        key = self.state_key(context)
        previous = self.load_state(key)
        previous_met = previous.get("met") if previous else None
        fire, now_met = crossed(previous_met, value, threshold, self.direction, self.fire_on_first_check)
        self.save_state(
            key,
            {"met": now_met, "value": value, "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds")},
        )

        self.log.info(
            "%s of %s is %s; threshold is %s %s. Previously past the line: %s. %s",
            self.aggregate,
            self.field,
            f"{value:,.2f}",
            self.direction,
            f"{threshold:,.2f}",
            "first check" if previous_met is None else previous_met,
            "Crossed: firing." if fire else "Not a new crossing.",
        )
        return fire
