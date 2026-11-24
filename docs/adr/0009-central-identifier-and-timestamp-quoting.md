# 0009. Central identifier and timestamp quoting

- Status: Accepted
- Date: 2026-11-25

## Context
The full-refresh default was the pre-quoted `'"Id"'` while the Delta merge key was unquoted `Id`, and SQL was built
by interpolating table, column and checkpoint values.

## Decision
All quoting lives in `pipeforge.util.sql`: `pg_ident` (`"..."` with doubled quotes), `spark_ident` (backticks).
Callers pass plain names; legacy pre-quoted names are normalised. Timestamps in SQL are generated from `datetime`
objects as `TIMESTAMPTZ '...+00'` literals, never from free text.

## Consequences
Mixed-case and hostile names are handled in one place (covered by unit tests and a mixed-case integration table).
Callers who pre-quoted names keep working.
