# 0010. Duplicate keys and schema evolution

- Status: Accepted
- Date: 2026-11-25

## Context
A MERGE with duplicate keys in the source batch is ambiguous or fails obscurely. Schema changes in the source
should not silently change the lake schema.

## Decision
- `on_duplicate_keys: fail` (default) raises `DuplicateKeyError` with sample keys. `latest` keeps one row per key,
  ordered by the cursor fields, nulls last. Neither moves the checkpoint on failure.
- `schema_evolution` is opt-in. Off, new source columns are **not** added to the target. Delta ignores them in
  `UPDATE SET *`/`INSERT *` rather than raising; the integration test pins this. On, Delta auto-merge is enabled for
  that merge only. Full refreshes always replace the schema.

## Consequences
A silently ignored new column is possible when evolution is off; operators must opt in to see it. The behaviour is
documented in the README.
