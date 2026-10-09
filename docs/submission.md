# Submission form text

Copy these into the hackathon's submission form. Fill in the parts in *italics* yourself.

## Project name

airflow-provider-serpapi

## Track

Track 2: Open-Source Integrations

## Short description

A community Apache Airflow provider for SerpApi. It adds a SerpApi connection type, a batch search operator that releases the worker while results are fetched, a quota guard, and a sensor that fires when a value such as a price crosses a line.

## What it does and who it helps

Data engineers already call SerpApi from Airflow with hand-written requests inside a task. That works for one search. For many searches on a schedule it has three problems: every search holds a worker while SerpApi works, nothing checks the search quota before a run starts, and a condition checked every few minutes spends searches on answers SerpApi would have served from its one-hour cache.

This package addresses each one:

- **Batch search operator.** Submits up to 50 searches at once using SerpApi's asynchronous mode, then defers. One trigger waits for all of them, so no worker is held. Results come back as flat rows.
- **Quota guard.** Reads SerpApi's free account endpoint before spending anything and refuses a run that does not fit in the searches left this month or this hour.
- **Threshold sensor.** Succeeds when a number from a search crosses a threshold, remembers the previous check so it fires on the crossing only, and never checks more often than SerpApi's cache makes useful.
- **Connection type.** The key lives in an Airflow connection with its own form, never in pipeline code, and the client never logs a request URL, because SerpApi carries the key in the URL.

It also ships a replay server, so the example pipelines and all tests run with no key and spend no searches.

It contains no language model.

## How it uses SerpApi

Every piece of data the package handles comes from SerpApi. It uses three endpoints:

- the Search API, with `async=true` for batches, across Google Shopping, Google Search and Google Jobs;
- the Search Archive API, to collect the results of asynchronous searches;
- the Account API, for the quota guard and the connection test.

It was run against the real SerpApi on October 9, 2026 on a free plan: one batch of 20 Google Shopping searches was submitted in asynchronous mode, the task deferred, and 786 rows came back. The measurements are in `docs/async-check.md`.

## Links

- Repository: *paste the GitHub URL*
- Demo video: *paste the unlisted video link*

## Did the project exist before the hackathon?

No. It was designed on October 5, 2026 and built from October 9, 2026.

## AI tools used

*The form asks you to name the AI tools you used. Add the name here before you paste this. The rules say AI use does not affect judging.*

An AI coding assistant was used throughout: to compare candidate ideas, to check for existing projects, and to write the code, tests and documentation under the team's direction. No AI model is part of the product itself.

## Before you select "Submit project"

- Open the repository link in a private browser window. It must load without signing in.
- Open the video link in a private browser window. It must play without signing in.
- Check that no API key appears in the repository or the video.
- A saved draft is not an entry. Select **Submit project**.
