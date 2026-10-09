import json
from pathlib import Path

import pytest

from serpapi_provider.core.extractors import EXTRACTORS, extract_rows, has_extractor

FIXTURE = Path(__file__).parents[2] / "replay" / "fixtures" / "google_shopping.json"
SHOPPING_COLUMNS = {
    "position", "title", "product_id", "source", "price", "extracted_price",
    "old_price", "extracted_old_price", "rating", "reviews", "delivery", "link",
}
COMMON_COLUMNS = {"search_id", "engine", "query", "fetched_at"}


def shopping_body():
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def test_shopping_gives_one_row_per_item_with_every_column():
    rows = extract_rows("google_shopping", shopping_body(), query="iphone 16")

    assert len(rows) == 8
    for row in rows:
        assert set(row) == SHOPPING_COLUMNS | COMMON_COLUMNS
    assert rows[0]["extracted_price"] == 52990.0
    assert rows[0]["source"] == "Replay Store A"
    assert rows[0]["link"] == "https://example.com/replay/offer-1"


def test_a_missing_field_becomes_none():
    rows = extract_rows("google_shopping", shopping_body())

    assert rows[1]["old_price"] is None
    assert rows[5]["rating"] is None


def test_common_columns_come_from_the_response_and_the_query_argument_wins():
    body = shopping_body()
    body["search_metadata"]["id"] = "abc123"
    body["search_parameters"]["q"] = "from the response"

    from_response = extract_rows("google_shopping", body)[0]
    overridden = extract_rows("google_shopping", body, query="mine", fetched_at="2026-10-10")[0]

    assert from_response["search_id"] == "abc123"
    assert from_response["engine"] == "google_shopping"
    assert from_response["query"] == "from the response"
    assert from_response["fetched_at"] == "2026-10-09 17:00:02 UTC"
    assert overridden["query"] == "mine"
    assert overridden["fetched_at"] == "2026-10-10"


def test_position_falls_back_to_the_order_in_the_list():
    rows = extract_rows("google_shopping", {"shopping_results": [{"title": "a"}, {"title": "b"}]})

    assert [row["position"] for row in rows] == [1, 2]


def test_link_falls_back_from_link_to_product_link():
    rows = extract_rows(
        "google_shopping",
        {"shopping_results": [{"link": "https://a.example"}, {"product_link": "https://b.example"}]},
    )

    assert [row["link"] for row in rows] == ["https://a.example", "https://b.example"]


def test_a_response_with_no_results_gives_no_rows():
    assert extract_rows("google_shopping", {"error": "Google hasn't returned any results for this query."}) == []


def test_an_unknown_engine_names_the_known_ones():
    with pytest.raises(KeyError, match="google_shopping"):
        extract_rows("google_flights", {})


def test_has_extractor():
    assert has_extractor("google_shopping")
    assert not has_extractor("google_flights")
    assert not has_extractor(None)
    assert set(EXTRACTORS) >= {"google_shopping"}
