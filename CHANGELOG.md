# Changelog

## 2.0.0

### Breaking
- Sinks now **return rows written and raise `SinkError`** instead of returning `(code, message)` tuples.
- `Checkpoint.get_checkpoint` / `set_checkpoint` **raise `CheckpointError`** on storage errors (they used to print and
  return `None`, which made `migrate()` silently run a FULL REFRESH).
- Checkpoints are stored as JSON at `checkpoints/<db>/<schema>/<table>.json` holding a timezone-aware UTC watermark.
  Legacy `<db>-<schema>-<table>.txt` checkpoints are still read (naive value interpreted as UTC) and replaced on the
  next successful run.
- Table/column names are passed **plain** (`Users`, `Id`). The legacy pre-quoted style (`'"Id"'`) is still accepted and
  normalised. Default `full_refresh_partition_field` is now `None` (no quoted default); use `read_partition_column`.
- `python_requires` is now `>=3.10,<3.12`. `pyspark` and `delta-spark` moved to the `pipeforge[spark]` extra.
- Standard stack: Spark 3.5.2, Delta 3.2.0, hadoop-aws 3.3.4 (AWS SDK bundle 1.12.262). Nothing is resolved from Maven at
  start-up unless `packages=` is passed explicitly. `scripts/` (which used `--packages`) was replaced by `examples/`.

### Fixed
- Incremental checkpoints used the driver's naive `datetime.now()`. The watermark is now the max cursor value of the
  loaded batch (source `now()` for an empty full load), stored as UTC. Reads use `>=` against it, so reruns are idempotent.
- `get_checkpoint` swallowed S3 errors; "not found" is now distinct from "error".
- `configure_s3` assumed `get_bucket_location` returns `None` for a missing bucket (it raises). It uses `head_bucket` and
  only creates buckets when `create_bucket=True`.
- `find_object_key_by_filename` listed the whole bucket on every read; now a direct `get_object` on a prefixed key.
- `_handle_transforms` mutated `self._transformers` (`pop(0)`), dropping the first transformer on repeated runs.
- Full refresh of an empty table crashed on `MIN/MAX = None`; `numPartitions < 1` is clamped to 1.
- Identifier quoting is centralised in `pipeforge.util.sql` (Postgres `"..."`, Spark `` `...` ``); timestamps in SQL are
  generated from `datetime` objects as `TIMESTAMPTZ` literals, never from free text.
- Delta MERGE fails with `DuplicateKeyError` on duplicate keys in a batch, or keeps the latest row per key
  (`on_duplicate_keys="latest"`). Schema evolution on MERGE is opt-in (`schema_evolution=True`).
- `PostgresSink.incremental_load` leaked connections on error and mishandled NaT; it now uses a context-managed connection.

### Added
- `pipeforge run|validate|check --spec FILE` CLI (spec also via `PIPEFORGE_SPEC_PATH` / inline `PIPEFORGE_SPEC`).
  Versioned JSON Schema: `schemas/job-spec.v1.schema.json`. Secrets are `{"env": ...}` / `{"file": ...}` references only.
  The last stdout line is `PIPEFORGE_RESULT {json}`; exit code 0 ok, 1 job failure, 2 invalid spec.
- Retries with exponential backoff for JDBC reads and checkpoint I/O; `validate_connection()` on connectors/checkpoints.
- `docker/Dockerfile` for the `pipeforge-job` image (FROM SparkLake `spark-base`, no `spark` extra).
- pytest unit and integration tests (Postgres + RustFS via docker compose) and GitHub Actions CI.
