# 0012. Breaking changes in 2.0.0

- Status: Accepted
- Date: 2026-11-25

## Context
Fixing the bugs required changing contracts: tuple returns hid failures, and checkpoint errors were invisible.

## Decision
Release as 2.0.0 with these breaking changes, listed in `CHANGELOG.md`:
- Sinks return rows written and raise `SinkError` instead of returning `(code, message)`.
- Checkpoints are JSON at a new key; legacy `.txt` files are still read and replaced on the next successful run.
- Table and column names are passed plain; the legacy pre-quoted style is still accepted.
- Checkpoint read errors fail the job instead of running a full refresh.
- Python is 3.10 or 3.11; pyspark and delta-spark moved to an extra.
- `scripts/` (which relied on `--packages`) and `requirements.txt` were removed in favour of `examples/` and
  `setup.py`.

## Consequences
Callers of the library must update error handling. The CLI and spec are new and carry no migration cost.
