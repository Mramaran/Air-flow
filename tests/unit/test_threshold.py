import pytest

from serpapi_provider.core.threshold import (
    crossed,
    effective_poke_interval,
    meets,
    reduce_values,
    searches_per_day,
)


def test_meets_below_and_above_are_strict():
    assert meets(49_999, 50_000, "below")
    assert not meets(50_000, 50_000, "below")
    assert meets(50_001, 50_000, "above")
    assert not meets(50_000, 50_000, "above")


def test_meets_refuses_an_unknown_direction():
    with pytest.raises(ValueError, match="direction"):
        meets(1, 2, "sideways")


def test_it_fires_when_the_value_crosses_the_line():
    assert crossed(False, 49_000, 50_000, "below") == (True, True)
    assert crossed(False, 51_000, 50_000, "above") == (True, True)


def test_it_does_not_fire_again_while_the_value_stays_past_the_line():
    assert crossed(True, 48_000, 50_000, "below") == (False, True)


def test_it_re_arms_after_the_value_goes_back():
    fire, met = crossed(True, 52_000, 50_000, "below")
    assert (fire, met) == (False, False)
    assert crossed(met, 49_500, 50_000, "below") == (True, True)


def test_the_first_check_records_the_state_and_does_not_fire_by_default():
    assert crossed(None, 49_000, 50_000, "below") == (False, True)
    assert crossed(None, 51_000, 50_000, "below") == (False, False)


def test_the_first_check_can_be_told_to_fire():
    assert crossed(None, 49_000, 50_000, "below", fire_on_first_check=True) == (True, True)
    assert crossed(None, 51_000, 50_000, "below", fire_on_first_check=True) == (False, False)


def test_reduce_values_min_max_first():
    rows = [{"p": 30.0}, {"p": 10}, {"p": None}, {"p": 20.5}, {}]

    assert reduce_values(rows, "p", "min") == 10.0
    assert reduce_values(rows, "p", "max") == 30.0
    assert reduce_values(rows, "p", "first") == 30.0


def test_reduce_values_ignores_non_numbers_and_returns_none_when_empty():
    assert reduce_values([{"p": "₹10"}, {"p": True}, {"p": None}], "p") is None
    assert reduce_values([], "p") is None


def test_reduce_values_refuses_an_unknown_aggregate():
    with pytest.raises(ValueError, match="aggregate"):
        reduce_values([{"p": 1}], "p", "median")


def test_the_poke_interval_is_raised_to_the_cache_period():
    assert effective_poke_interval(60) == 3600
    assert effective_poke_interval(7200) == 7200


def test_searches_per_day():
    assert searches_per_day(60) == 24
    assert searches_per_day(3600) == 24
    assert searches_per_day(21_600) == 4
