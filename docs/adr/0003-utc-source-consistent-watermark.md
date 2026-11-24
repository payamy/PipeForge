# 0003. UTC, source-consistent watermark

- Status: Accepted
- Date: 2026-11-25

## Context
Incremental checkpoints were the driver's naive `datetime.now()`. Driver clock skew, or a driver in Tehran
(UTC+03:30) reading a source in UTC, could skip rows (watermark ahead of the data) or duplicate them.

## Decision
- The watermark is the **max cursor value in the loaded batch**; for an empty full load it is the source's
  `now()` read in the same run. It is stored as a timezone-aware UTC instant.
- Incremental reads select rows where any cursor field is `>=` the watermark minus an optional look-back, and the
  load is an idempotent MERGE, so re-reading the boundary row is harmless.
- The Spark session time zone is UTC and JDBC sessions run `SET TIME ZONE 'UTC'`.

## Consequences
Results no longer depend on the driver clock or zone (the integration tests run the driver in `Asia/Tehran`).
Rows with NULL cursor values are never selected. Cursor fields must be timestamp, timestamptz or date. A row
committed late with an older timestamp can still be missed by a long transaction; `lookback_seconds` mitigates it.
Naive legacy checkpoints are interpreted as UTC.
