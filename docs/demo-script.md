# Demo video script

Target length: 2 minutes 45 seconds. The rules allow up to 3 minutes and say video quality is not judged, so a plain screen recording with your voice is enough.

## Before you record

1. Start the stack and wait for it to come up:

   ```bash
   docker compose up --build
   ```

2. Open <http://localhost:8080> in a browser window sized for recording.
3. Open `README.md` on GitHub in a second tab, scrolled to the comparison table.
4. If you have a key and want to show a live run, also read "Optional live segment" at the end.
5. Do one practice run of every step so nothing is loading for the first time on camera.
6. Do not click **Test Connection** in the connections list on camera. In Airflow 3.3.2 that button fails for any connection saved without a port; see the README's known limits.

## Script

| Time | On screen | Say |
|---|---|---|
| 0:00–0:15 | The README title on GitHub | "This is airflow-provider-serpapi, a community Airflow provider for SerpApi, built for Track 2. People already call SerpApi from Airflow by hand. That is fine for one search. This package is for many searches on a schedule." |
| 0:15–0:35 | Airflow: Admin → Connections → Add Connection. Pick **SerpApi** from the type list. Show the **API key** and **Base URL (optional)** fields, then close the form without saving. | "It adds a SerpApi connection type. The key lives in an Airflow connection, not in pipeline code, and the form asks only for the key and an optional base URL." |
| 0:35–1:15 | DAGs list → **price_tracker** → Trigger. Open the grid. Point at `fetch_prices` when it turns **deferred**. | "This pipeline tracks prices for 20 products. One task submits all 20 searches to SerpApi in asynchronous mode, and then defers. Right now 20 searches are in flight and this task holds no worker. One trigger is waiting for all of them." |
| 1:15–1:35 | Click `fetch_prices` → Logs. Show the "Quota guard" line and the "Collected ... rows from 20 searches" line. Then open `report_price_spread` → Logs and show the table. | "Before it spent anything, the quota guard read the account and confirmed 20 searches fit this month and this hour. The results come back as flat rows and are loaded into a table. This run uses the bundled replay server, which replays real Google Shopping results I captured from SerpApi with my own key. Notice the spread: a product name also returns accessories and other models, so the pipeline reports lowest, median and highest." |
| 1:35–1:55 | DAGs list → **quota_guard_demo** → Trigger. It fails in seconds. Open its log and show the refusal line. | "Here the same batch is told to keep a hundred thousand searches in reserve. The guard refuses the run before a single search is sent, and says exactly why. On a real account this is what stops a run you cannot afford." |
| 1:55–2:25 | DAGs list → **price_drop_alert** → Trigger. Show the sensor succeed and `send_alert` run. Open the sensor log. Then trigger it again and show it waiting in **up_for_reschedule**. | "The sensor fires when the median price crosses a line, not every time it is below it. It remembers the last check. The second run waits, because the price has not crossed again. It checks at most once an hour, since SerpApi caches identical searches for an hour, and it logs the most it can spend in a day." |
| 2:25–2:45 | The comparison table in the README | "You can do all of this without a package, and for one search you should. For a batch, the plain route holds a worker per search and has no quota check. That difference is what this provider is for. There is no language model anywhere in it. Thank you." |

## Optional live segment

Use this in place of the 1:15–1:35 row if you have run the pipeline with your own key. It shows real results and the search count moving.

1. Stop the stack and start it with the live override:

   ```bash
   docker compose -f docker-compose.yml -f docker-compose.live.yml up --build
   ```

2. Open your SerpApi dashboard in another tab and note the searches left.
3. Trigger **price_tracker**. Show `fetch_prices` deferred, then the log of `report_price_spread`.
4. Refresh the SerpApi dashboard and show the count has fallen by at most 20.

Say: "This run used my own SerpApi key, live. My account's count fell by the number of new searches. Searches repeated within the hour come from SerpApi's cache and are free."

**Keep the key off camera.** Do not show `.env`, the SerpApi dashboard's key field, or the connection's edit form with the key filled in.

## After you record

- Upload as unlisted on YouTube, or as a shareable Drive link.
- Open the link in a private browser window to confirm it plays without signing in.
