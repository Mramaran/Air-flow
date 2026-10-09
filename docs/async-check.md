# What was measured against the real SerpApi

Measured on October 9, 2026 between 17:30 and 17:45 UTC, on a SerpApi **free plan**, with Google Shopping searches for India (`gl=in`, `hl=en`). The single-search figures come from two runs of `scripts/async_check.py`; the batch figures come from one live run of the `price_tracker` example pipeline.

## Asynchronous search

| Question | Result |
|---|---|
| Is `async=true` accepted on the free plan? | Yes. |
| What statuses does a search go through? | `Processing`, then `Success`. |
| How long does one search take to finish? | 2.2 seconds in the first run, 2.0 seconds in the second. |
| Is one asynchronous search counted as one search? | Yes. The account went from 250 to 249 after the first new search and to 248 after the second. |
| Are archive lookups counted? | No. Dozens were made; the count moved only with new searches. |
| Are account reads counted? | No. |
| What happens when the same search is submitted again within the hour? | It returned the **same search id**, already at `Success`, and was not counted. |

## One finding that was not expected

The account endpoint's count **lags by a few seconds**. Read immediately after a search finished, it still showed the old number; a few seconds later it had updated. `scripts/async_check.py` now waits 10 seconds before reading it. For the quota guard this means two runs started back to back can both pass on the same count.

## The account's limits

The account endpoint reported `account_rate_limit_per_hour: 250` for this free-plan account. The quota guard uses whatever number the account reports, so it needed no change.

## One live batch

The `price_tracker` pipeline was run once with the live key:

- The quota guard logged: "20 searches wanted; 244 left this month, 244 left this hour."
- 20 searches were submitted in asynchronous mode and the task went to `deferred`.
- 786 rows were collected from the 20 searches.
- The whole pipeline, three tasks, finished within 40 seconds of being triggered.
- The account went from 244 to 230 searches left: **14 counted**. The other 6 had been run earlier in the same hour and came from SerpApi's cache.
- A scan of that run's task logs found no string shaped like an API key.

## What the results looked like

A product name returns more than that product. For "iPhone 16 128GB", Google Shopping returned 40 offers from ₹7,999 to ₹114,900 with a median of ₹66,549; the cheapest listings were for other models. For "Redmi Note 14 Pro 5G" the median was ₹599, because 24 of the 40 offers named an accessory such as a case or a skin. This is why the example pipeline reports the spread of prices per query and the sensor example watches the median.

## Searches spent

20 in total: 2 for the asynchronous checks, 4 for capturing results, 14 for the live batch. 230 of 250 were left afterwards.

## Not measured

- Batches larger than 20, and the behaviour at the hourly limit.
- Engines other than Google Shopping.
- A task retry that resubmits a batch after the one-hour cache has expired.
