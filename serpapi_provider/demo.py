"""Shared pieces of the example pipelines: the product list and the SQLite price table.

Kept outside example_dags/ so that both pipelines can import it without one
pipeline file defining the other's DAG a second time.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

PRODUCTS = [
    "iPhone 16 128GB",
    "Samsung Galaxy S25 256GB",
    "OnePlus 13 256GB",
    "Google Pixel 10 128GB",
    "Nothing Phone 3a",
    "Redmi Note 14 Pro 5G",
    "MacBook Air M4 13 inch",
    "Dell XPS 13 laptop",
    "HP Pavilion 15 laptop",
    "Lenovo IdeaPad Slim 5",
    "Sony WH-1000XM5 headphones",
    "Apple AirPods Pro 2",
    "boAt Airdopes 141",
    "JBL Flip 6 speaker",
    "Samsung 55 inch 4K smart TV",
    "LG 1.5 ton split AC",
    "Philips air fryer 4.1L",
    "Prestige induction cooktop",
    "Canon EOS R50 camera",
    "Apple Watch Series 10",
]

DB_PATH = os.environ.get("SERPAPI_DEMO_DB", "/tmp/serpapi_prices.db")

COLUMNS = [
    "run_date", "query", "position", "title", "source", "price", "extracted_price",
    "old_price", "extracted_old_price", "rating", "reviews", "delivery", "link",
    "search_id", "fetched_at",
]

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS prices (
    run_date TEXT NOT NULL,
    query TEXT NOT NULL,
    position INTEGER NOT NULL,
    title TEXT, source TEXT, price TEXT, extracted_price REAL,
    old_price TEXT, extracted_old_price REAL, rating REAL, reviews INTEGER,
    delivery TEXT, link TEXT, search_id TEXT, fetched_at TEXT,
    PRIMARY KEY (run_date, query, position)
)
"""


def shopping_searches(products: list[str]) -> list[dict]:
    """One Google Shopping search per product, for India."""
    return [{"engine": "google_shopping", "q": product, "gl": "in", "hl": "en"} for product in products]


def save_rows(rows: list[dict], run_date: str, db_path: str = DB_PATH) -> int:
    """Upsert one day's snapshot. Running the same day again replaces rows instead of adding them."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as db:
        db.execute(CREATE_TABLE)
        db.executemany(
            f"INSERT OR REPLACE INTO prices ({', '.join(COLUMNS)}) VALUES ({', '.join('?' * len(COLUMNS))})",
            [[run_date if column == "run_date" else row.get(column) for column in COLUMNS] for row in rows],
        )
    return len(rows)


def lowest_prices(run_date: str, db_path: str = DB_PATH) -> list[tuple]:
    """(query, lowest price, seller, title) per product for one day's snapshot."""
    with sqlite3.connect(db_path) as db:
        return db.execute(
            """
            SELECT p.query, p.extracted_price, p.source, p.title
            FROM prices p
            JOIN (
                SELECT query, MIN(extracted_price) AS lowest
                FROM prices WHERE run_date = ? AND extracted_price IS NOT NULL GROUP BY query
            ) m ON m.query = p.query AND m.lowest = p.extracted_price
            WHERE p.run_date = ?
            GROUP BY p.query
            ORDER BY p.query
            """,
            (run_date, run_date),
        ).fetchall()
