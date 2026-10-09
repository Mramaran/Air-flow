from pathlib import Path

import pytest

from serpapi_provider.operators.search import SerpApiSearchOperator

CONN_ID = "serpapi_test"

EXAMPLES = Path(__file__).parents[2] / "serpapi_provider" / "example_dags"


def test_the_search_operator_returns_rows(replay):
    replay()

    rows = SerpApiSearchOperator(
        task_id="one", serpapi_conn_id=CONN_ID, search={"engine": "google_shopping", "q": "tv"}
    ).execute({})

    assert len(rows) == 8
    assert rows[0]["query"] == "tv"


def test_the_search_operator_returns_the_raw_response_when_asked(replay):
    replay()

    body = SerpApiSearchOperator(
        task_id="one", serpapi_conn_id=CONN_ID, search={"engine": "google_shopping", "q": "tv"}, extract=False
    ).execute({})

    assert "shopping_results" in body


def test_the_search_operator_refuses_a_search_with_no_engine():
    with pytest.raises(ValueError, match="engine"):
        SerpApiSearchOperator(task_id="one", serpapi_conn_id=CONN_ID, search={"q": "tv"}).execute({})


def test_the_example_dags_import_cleanly_with_the_expected_tasks():
    from airflow.dag_processing.dagbag import DagBag

    bag = DagBag(dag_folder=str(EXAMPLES))

    assert bag.import_errors == {}
    assert set(bag.dags["price_tracker"].task_ids) == {"fetch_prices", "load_prices", "report_price_spread"}
    assert set(bag.dags["quota_guard_demo"].task_ids) == {"fetch_prices_refused"}


def test_the_price_table_upserts_one_snapshot_per_day(tmp_path):
    from serpapi_provider.demo import price_spread, save_rows

    db = str(tmp_path / "prices.db")
    rows = [
        {"query": "tv", "position": 1, "title": "TV A", "source": "Shop A", "extracted_price": 500.0},
        {"query": "tv", "position": 2, "title": "TV B", "source": "Shop B", "extracted_price": 450.0},
        {"query": "phone", "position": 1, "title": "Phone", "source": "Shop C", "extracted_price": 900.0},
    ]

    assert save_rows(rows, "2026-10-10", db) == 3
    assert save_rows(rows, "2026-10-10", db) == 3
    save_rows(rows, "2026-10-11", db)

    assert price_spread("2026-10-10", db) == [
        {"query": "phone", "offers": 1, "lowest": 900.0, "median": 900.0, "highest": 900.0},
        {"query": "tv", "offers": 2, "lowest": 450.0, "median": 475.0, "highest": 500.0},
    ]
    import sqlite3

    with sqlite3.connect(db) as connection:
        assert connection.execute("SELECT COUNT(*) FROM prices").fetchone()[0] == 6
