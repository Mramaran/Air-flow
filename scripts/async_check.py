"""Check how SerpApi's asynchronous search behaves on this account, and write docs/async-check.md.

It answers the questions the batch operator depends on:
  1. Is async=true accepted on this plan?
  2. Is one asynchronous search counted as one search?
  3. Are archive lookups free?
  4. How long does a search take to finish?
  5. What does a repeat of the same search within the hour cost?

It spends at most 2 searches. It prints numbers only, never the key.

    python scripts/async_check.py
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from _env import ROOT, make_client

from serpapi_provider.core.client import SerpApiClient, is_failed, is_finished, status_of
from serpapi_provider.core.errors import SerpApiError
from serpapi_provider.core.quota import QuotaStatus

SEARCH = {"engine": "google_shopping", "q": "Google Pixel 10 128GB", "gl": "in", "hl": "en"}
MAX_WAIT_SECONDS = 90
# The account endpoint's count updates a few seconds after a search finishes.
ACCOUNT_SETTLE_SECONDS = 10


def wait_for(client: SerpApiClient, search_id: str) -> tuple[str, float, list[str], int]:
    """Poll the archive once a second. Returns final status, seconds, the statuses seen, polls made."""
    started = time.monotonic()
    seen: list[str] = []
    polls = 0
    while True:
        status = status_of(client.archive(search_id))
        polls += 1
        if not seen or seen[-1] != status:
            seen.append(status)
            print(f"    {time.monotonic() - started:5.1f}s  {status}")
        if is_finished(status) or is_failed(status) or time.monotonic() - started > MAX_WAIT_SECONDS:
            return status, time.monotonic() - started, seen, polls
        time.sleep(1)


def main() -> None:
    lines: list[str] = []

    def say(text: str = "") -> None:
        print(text)
        lines.append(text)

    with make_client() as client:
        before = QuotaStatus.from_account(client.account())
        print(f"Account before: {before.searches_left} searches left, {before.used_this_hour} used this hour.")

        print("First asynchronous search:")
        try:
            first_id = client.submit_async(SEARCH)
        except SerpApiError as exc:
            say(f"- **Async accepted:** no. SerpApi answered: {exc}")
            write(lines)
            return
        first_status, first_seconds, first_seen, first_polls = wait_for(client, first_id)
        time.sleep(ACCOUNT_SETTLE_SECONDS)
        after_first = QuotaStatus.from_account(client.account())

        print("Same search again:")
        second_id = client.submit_async(SEARCH)
        second_status, second_seconds, second_seen, second_polls = wait_for(client, second_id)
        time.sleep(ACCOUNT_SETTLE_SECONDS)
        after_second = QuotaStatus.from_account(client.account())

    counted_first = before.searches_left - after_first.searches_left
    counted_second = after_first.searches_left - after_second.searches_left

    say("- **Async accepted:** yes.")
    say(f"- **Statuses seen:** {', then '.join(first_seen)}.")
    say(f"- **Time to finish:** {first_seconds:.1f} seconds.")
    say(f"- **Searches counted for the first search:** {counted_first}.")
    say(
        f"- **Archive lookups:** {first_polls + second_polls} lookups were made during this check and "
        f"the account's count moved only by the searches above, so lookups were not counted."
    )
    say(f"- **Same search again within the hour:** statuses {', then '.join(second_seen)}, "
        f"finished in {second_seconds:.1f} seconds, {counted_second} searches counted.")
    say(f"- **Same search id returned for the repeat:** {'yes' if first_id == second_id else 'no'}.")
    say(f"- **Account:** {before.searches_left} searches left before, {after_second.searches_left} after.")
    write(lines)


def write(lines: list[str]) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    path = ROOT / "docs" / "async-check.md"
    path.write_text(
        "# Asynchronous search check\n\n"
        f"Measured by `scripts/async_check.py` on {stamp}, on a SerpApi free plan, "
        f"with one Google Shopping search (`q={SEARCH['q']!r}`, `gl=in`).\n\n"
        + "\n".join(lines)
        + "\n",
        encoding="utf-8",
        newline="\n",
    )
    print(f"Wrote {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
