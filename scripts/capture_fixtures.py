"""Save real SerpApi responses for some of the demo products, for the replay server to serve.

Each new search is counted against your account (repeats within an hour are free).
The default is 6 searches. Nothing is saved that identifies your account.

    python scripts/capture_fixtures.py            # first 6 demo products
    python scripts/capture_fixtures.py --count 20 # all of them
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from _env import ROOT, make_client

from replay.server import slug
from serpapi_provider.core.quota import QuotaStatus
from serpapi_provider.demo import PRODUCTS, shopping_searches

KEEP_METADATA = ("id", "status", "created_at", "processed_at", "total_time_taken")
DROP_KEYS = {"thumbnail", "thumbnails", "source_icon"}


def dropped(key: str) -> bool:
    """Inline images, SerpApi follow-up links and paging tokens: large, and not needed for replay."""
    return key in DROP_KEYS or key.startswith("serpapi_") or key.endswith("_token")


def strip(value):
    if isinstance(value, dict):
        return {key: strip(item) for key, item in value.items() if not dropped(key)}
    if isinstance(value, list):
        return [strip(item) for item in value]
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--count", type=int, default=6)
    args = parser.parse_args()

    target = ROOT / "replay" / "fixtures" / "google_shopping"
    target.mkdir(parents=True, exist_ok=True)

    with make_client() as client:
        before = QuotaStatus.from_account(client.account()).searches_left
        for search in shopping_searches(PRODUCTS[: args.count]):
            body = client.search(search)
            metadata = body.get("search_metadata") or {}
            saved = {
                "_replay": True,
                "_captured_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                "search_metadata": {key: metadata[key] for key in KEEP_METADATA if key in metadata},
                "search_parameters": body.get("search_parameters") or search,
                "shopping_results": strip(body.get("shopping_results") or []),
            }
            path = target / f"{slug(search['q'])}.json"
            path.write_text(json.dumps(saved, ensure_ascii=False, indent=1) + "\n", encoding="utf-8", newline="\n")
            print(f"  saved {len(saved['shopping_results']):3d} results  {path.relative_to(ROOT)}")
        after = QuotaStatus.from_account(client.account()).searches_left
    print(f"Searches counted: {before - after}. Left: {after}.")


if __name__ == "__main__":
    main()
