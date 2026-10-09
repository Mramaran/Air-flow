import pytest

from serpapi_provider.sensors.threshold import SerpApiThresholdSensor

CONN_ID = "serpapi_test"
SEARCH = {"engine": "google_shopping", "q": "tv", "gl": "in"}


class MemorySensor(SerpApiThresholdSensor):
    """Keeps the crossing state in memory so the test needs no Airflow Variable store."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.store = {}

    def load_state(self, key):
        return self.store.get(key)

    def save_state(self, key, state):
        self.store[key] = state


def sensor(**kwargs):
    kwargs.setdefault("task_id", "watch")
    kwargs.setdefault("serpapi_conn_id", CONN_ID)
    kwargs.setdefault("search", SEARCH)
    return MemorySensor(**kwargs)


@pytest.fixture
def lowest(replay):
    from serpapi_provider.core.client import SerpApiClient

    server = replay()
    client = SerpApiClient("replay-key", base_url=f"http://127.0.0.1:{server.server_address[1]}")
    return min(item["extracted_price"] for item in client.search(SEARCH)["shopping_results"])


def test_the_first_check_records_the_value_and_does_not_fire(lowest):
    s = sensor(threshold=lowest + 1)

    assert s.poke({}) is False
    (state,) = s.store.values()
    assert state["met"] is True
    assert state["value"] == lowest


def test_it_fires_when_the_value_crosses_the_line(lowest):
    s = sensor(threshold=lowest - 1)
    assert s.poke({}) is False

    s.threshold = lowest + 1

    assert s.poke({}) is True


def test_it_does_not_fire_again_while_the_value_stays_past_the_line(lowest):
    s = sensor(threshold=lowest + 1, fire_on_first_check=True)

    assert s.poke({}) is True
    assert s.poke({}) is False


def test_direction_above_and_aggregate_max(lowest):
    s = sensor(threshold=0, direction="above", aggregate="max", fire_on_first_check=True)

    assert s.poke({}) is True
    (state,) = s.store.values()
    assert state["value"] > lowest


def test_the_state_key_names_the_dag_and_task():
    from datetime import datetime

    from airflow.sdk import DAG

    with DAG(dag_id="my_prices", schedule=None, start_date=datetime(2026, 10, 1)):
        s = sensor(threshold=1)

    assert s.state_key({}) == "serpapi_threshold__my_prices__watch"


def test_a_short_poke_interval_is_raised_to_an_hour_and_the_mode_is_reschedule():
    s = sensor(threshold=1, poke_interval=60)

    assert s.poke_interval == 3600
    assert s.mode == "reschedule"
    assert sensor(threshold=1, poke_interval=7200).poke_interval == 7200


def test_the_quota_guard_skips_a_check_it_cannot_afford(replay):
    server = replay(searches_left=5)
    s = sensor(threshold=10**9, fire_on_first_check=True, reserve_searches=5)

    assert s.poke({}) is False
    assert server.state.searches_left == 5
    assert s.store == {}


def test_an_engine_with_no_extractor_is_refused(replay):
    replay()

    with pytest.raises(ValueError, match="row extractor"):
        sensor(threshold=1, search={"engine": "google_flights", "q": "x"}).poke({})


@pytest.mark.parametrize("bad", [{"direction": "sideways"}, {"aggregate": "median"}])
def test_bad_options_are_refused_when_the_sensor_is_built(bad):
    with pytest.raises(ValueError):
        sensor(threshold=1, **bad)
