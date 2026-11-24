# Task 1 report: PipeForge

Status: **code complete, pushed, tests green. PR not yet opened. Job image build not yet verified.**

## 1. Where the work is

| Item | Value |
|---|---|
| Repo | https://github.com/payamy/PipeForge |
| Local clone | `Domera/PipeForge` |
| Branch | `feature/job-cli-and-fixes` (cut from `main`; `main` was never touched) |
| PR | Not opened (`gh` is not installed). Open at https://github.com/payamy/PipeForge/pull/new/feature/job-cli-and-fixes |
| Version | 1.0.0 -> **2.0.0** |

Commits (oldest first):

| SHA | Message |
|---|---|
| `eac91b9` | Fix checkpoint, quoting, merge and connector bugs |
| `6c32b6d` | Add job CLI, versioned spec schema and pipeforge-job image |
| `a1f5337` | Add pytest suites, CI, changelog and docs; bump to 2.0.0 |
| `b34c0b6` | Add architecture decision records |

## 2. What I found before changing anything

PipeForge was a small package: Postgres connector -> Delta/Postgres sinks, S3/local checkpoints, a Spark session wrapper, a Vault client and a transformer chain. It had no CLI, no JSON spec and no real tests (three print-based scripts; two did not even match pytest's discovery pattern). I read every source file and confirmed bugs 1-8 and 11-12 from your list directly in the code. Bugs 9 and 10 were absences (no duplicate-key handling, no retries) rather than defects in existing lines.

Additional problems I found and fixed:
- `SparkWrapper` resolved Maven packages at start-up (`spark.jars.packages` / `configure_spark_with_delta_pip`), which violates the no-download-at-start rule.
- `PostgresSink.incremental_load` leaked its connection on error and mishandled `NaT`.
- Delta `full_load` and merge swallowed exceptions into `(code, message)` tuples.
- Incremental partitioning used cursor timestamps as JDBC bounds computed from the driver's clock.
- `README` documented Python 3.11 only and `pip install -r requirements.txt`.

## 3. Your 12 items: what changed

| # | Issue | Fix | Where |
|---|---|---|---|
| 1 | Naive driver `datetime.now()` checkpoints | Watermark = max cursor value of the loaded batch (source `now()` for an empty full load), stored as timezone-aware UTC. Reads use `>=` and the MERGE is idempotent. Session zone and JDBC `SET TIME ZONE 'UTC'` are forced. | `connector/postgresql/postgresql.py`, `checkpoint/checkpoint.py` |
| 2 | S3 errors swallowed, silent FULL REFRESH | "Not found" returns `None`. Every other failure raises `CheckpointError`. `set_checkpoint` raises. A read error always fails the job; a missing checkpoint is a separate, explicit path. | `checkpoint/checkpoint.py`, `errors.py` |
| 3 | `get_bucket_location` assumption, auto-create | `head_bucket`. Missing bucket raises unless `create_bucket=True` (`create_checkpoint_bucket` in the spec). | `checkpoint/checkpoint.py` |
| 4 | Whole-bucket listing per read | Direct `get_object` on `checkpoints/<db>/<schema>/<table>.json`. Legacy `.txt` key is read as a fallback, also by direct get. A unit test fails if listing is ever called. | `checkpoint/checkpoint.py` |
| 5 | `pop(0)` drops first transformer | Chain built from a fresh list of instances. Input list is never mutated. | `connector/base.py` |
| 6 | Empty table / `numPartitions < 1` | One stats query (`COUNT`, `MIN`, `MAX`); partitions = `clamp(ceil(count/size), 1, max)`; partition options only applied when there are bounds and more than one partition. | `connector/postgresql/postgresql.py` |
| 7 | `'"Id"'` vs `Id` inconsistency | One module quotes everything: `pg_ident` (`"x"`) and `spark_ident` (backtick). Callers pass plain names. Legacy pre-quoted names are normalised. | `util/sql.py` |
| 8 | SQL by interpolation | Identifiers are always quoted/escaped. Timestamps come from `datetime` objects as `TIMESTAMPTZ '...+00'` literals. | `util/sql.py` |
| 9 | MERGE with duplicate keys, schema evolution | `on_duplicate_keys: fail` (default, `DuplicateKeyError` with sample keys) or `latest` (window by cursor, nulls last). Schema evolution is opt-in. | `sink/dedupe.py`, `sink/delta/delta.py` |
| 10 | Retries, connection validation | `retry_call` with exponential backoff and jitter on JDBC reads and checkpoint I/O (transient errors only; auth and syntax errors are not retried). `validate_connection()` on the connector (table, columns, cursor types, partition column type) and on `Checkpoint`. | `util/retry.py`, connector, checkpoint |
| 11 | `python_requires` | `>=3.10,<3.12` | `setup.py` |
| 12 | pyspark/delta-spark as hard deps | Moved to `pipeforge[spark]`; `dev` extra added; every dependency pinned exactly. | `setup.py` |

## 4. New functionality

### 4.1 Job CLI
```
pipeforge validate --spec spec.json   # schema + cross-field checks
pipeforge check    --spec spec.json   # + source and checkpoint connectivity
pipeforge run      --spec spec.json   # run the job
```
- Spec source: `--spec FILE`, `PIPEFORGE_SPEC_PATH` (file) or `PIPEFORGE_SPEC` (inline JSON).
- Entry point `pipeforge` (console script).
- Heavy imports (pyspark) only happen for `run`/`check`, so `validate` works anywhere.
- Logs go to stderr; stdout carries only the result line.
- Final stdout line is always `PIPEFORGE_RESULT {json}` with: `status`, `mode`, `initial_load`, `rows_read`, `rows_written`, `old_watermark`, `new_watermark`, `duration_seconds`, `run_id`, `spec_version`, `error`.
- Exit codes: `0` success, `1` job failure, `2` invalid spec. Failures still print a result line with `status: failed`.

### 4.2 Versioned job spec
- JSON Schema (draft 2020-12): `schemas/job-spec.v1.schema.json`, also packaged at `pipeforge/spec/job-spec.v1.schema.json`. A test fails if the two differ.
- Covers: source connection, table/columns, cursor fields, unique key, read-partition column, mode (`full`/`incremental`), `on_missing_checkpoint`, look-back, target database/table/partitioning/duplicate policy/schema evolution, S3 (endpoint, path-style, region), warehouse, checkpoint bucket, metastore URI, Spark master and conf, retry settings, custom transformers (`module:Class`).
- **Secrets are references only**: `username`, `password`, `access_key`, `secret_key` must be `{"env": NAME}` or `{"file": PATH}`. Inline values fail validation. Unknown keys are rejected.
- Cross-field rules in code: columns must include cursor fields and the unique key; `on_duplicate_keys` needs a unique key; incremental needs cursor fields.
- Example: `examples/spec.example.json` (validated by a test).

### 4.3 Job image
`docker/Dockerfile` builds `pipeforge-job` FROM a base given by build args `SPARK_BASE_IMAGE` and `SPARK_BASE_TAG`, installs PipeForge at the pinned version **without** the `spark` extra, asserts pyspark was not pip-installed, sets `TZ=UTC`, runs as the `spark` user, and uses `pipeforge` as the entrypoint. It never touches SparkLake source.

### 4.4 Spark session wrapper
- No Maven resolution unless `packages=` is passed explicitly (development only; logs a warning).
- `s3` argument builds `spark.hadoop.fs.s3a.*` settings, including path-style, region, and SSL derived from the endpoint scheme.
- Hive support only when a metastore URI is given; Delta extensions when `enable_delta=True`.
- Session time zone fixed to UTC; `SparkWrapper.reset()` for tests.

## 5. Load semantics (as implemented)

1. **full**: replaces the target. Watermark = max cursor in the loaded data, or the source `now()` captured in the same run if the table is empty.
2. **incremental**: select rows where *any* cursor field `>=` (stored watermark minus look-back), MERGE on the unique key, then write the new watermark. If no rows qualify, nothing is written and the watermark is unchanged.
3. **incremental with no checkpoint**: initial full load (`initial_load: true`), or fail when `on_missing_checkpoint: fail`.
4. **Checkpoint read error**: the job fails. It never falls back to a full refresh.
5. The checkpoint is written *after* the load. If the write fails, data is loaded but the job fails; a rerun is safe because the load is idempotent.
6. The boundary row (cursor equal to the watermark) is deliberately re-read each run.

## 6. Tests

**Final result: 60 passed** on Python 3.10 inside the Spark 3.5.2 image, against Postgres 16.4 and RustFS 1.0.6, with the driver in `TZ=Asia/Tehran` on purpose.

Command:
```
docker compose -f docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from tests
```

### Unit tests (47, no services)
- `test_sql_and_retry.py`: quoting (plain, legacy-quoted, hostile names), timestamp literals for Tehran/UTC/naive inputs, invalid identifiers, retry/backoff behaviour.
- `test_checkpoint.py`: UTC round-trip including a +03:30 input, naive input, legacy `.txt`, corrupt content, S3 via moto, missing bucket with and without creation, AccessDenied/500/NoSuchBucket raise instead of looking like "not found", `set_checkpoint` raises, direct-key-only reads.
- `test_transformer_chain.py`: order, repeated runs keep the first transformer, input list not mutated.
- `test_spec_and_cli.py`: schema equality, valid spec, inline secrets rejected, schema violations, secret resolution from env and file, spec loading, CLI exit codes 0/1/2 and the final result line.

### Integration tests (13)
- Full load sets a UTC watermark from the batch (mixed-case identifiers, +03:30 source times).
- **Rerunning an incremental load produces no duplicates** (including a wide look-back that re-reads everything).
- No-new-rows is a no-op that keeps the watermark.
- Empty-table full refresh works and falls back to the source clock.
- Partition size clamping.
- A checkpoint read error does not overwrite the target.
- `on_missing_checkpoint: fail`.
- Duplicate keys fail clearly and the checkpoint does not move; `latest` collapses them.
- Schema evolution is opt-in.
- Connection validation and cursor-type validation.
- Spec runner end to end with secrets from env vars, and the CLI result line for success and failure.

### How the run got to green (honest history)
1. First compose run: **MinIO could not be pulled** (Docker Hub repo denied, quay.io 401). Switched the test S3 to RustFS 1.0.6.
2. Second run: 4 failures, all `ModuleNotFoundError: delta`. The image has the Delta jars, not the `delta` Python package. Changed MERGE to Spark SQL (ADR 0005).
3. Third run: 3 failures, all wrong expectations in **my tests** (boundary-row re-read counts, and Delta ignoring extra source columns instead of raising). I corrected the tests and documented the behaviour. The code behaved as designed.
4. Fourth run: 60 passed.

## 7. CI and docs
- `.github/workflows/ci.yml`: lint (ruff), tests in the Spark image via compose, image build. **It has not run on GitHub yet.**
- `README.md` rewritten (stack table, CLI, spec, result line, load semantics, image, library use, tests).
- `CHANGELOG.md` 2.0.0 entry (breaking, fixed, added).
- `docs/adr/`: 12 ADRs plus index and template (stack/Delta choice, UTC watermark, checkpoint layout, SQL MERGE, spec, CLI contract, job image, quoting, duplicate keys, test infrastructure, breaking changes).

## 8. Decisions you should review

1. **Delta 3.2.0, not 3.3.x.** It is the newest 3.x whose published metadata allows pyspark 3.5.2 (3.2.1 and 3.3.x need `pyspark>=3.5.3` and are built against 3.5.3-3.5.6). 3.3.x might run on 3.5.2 but is undeclared and untested.
2. **Breaking 2.0.0 API**: sinks raise `SinkError` and return row counts instead of tuples; checkpoint key/format changed; plain names preferred.
3. **MERGE through Spark SQL** so the job image needs only jars.
4. **Removed `scripts/` and `requirements.txt`** (they relied on `--packages` and the old API); replaced by `examples/` and `setup.py`.
5. **Without `schema_evolution`, new source columns are silently ignored by MERGE** (Delta behaviour). Safe for the schema, but a column can go unnoticed.
6. **RustFS 1.0.6** is the test S3 (current stable release). MinIO images are no longer pullable.

## 9. Findings that affect other tasks

- **`spark:3.5.2-python3` ships Python 3.8.10.** SparkLake's `spark-base` must start from a Python 3.10+ image (I used `spark:3.5.2-scala2.12-java17-python3-ubuntu`), otherwise `pipeforge-job` cannot install.
- `spark-base` must include the Postgres JDBC jar and hadoop-aws/AWS SDK bundle at 3.3.4/1.12.262, and must set no timezone assumptions (PipeForge forces UTC itself).
- The data-platform repo should render specs against `schemas/job-spec.v1.schema.json`. The spec's `source.read_partition_column` is the "partition column" the pipeline form exposes; `target.partition_by` is the Delta partitioning.

## 10. Open risks and what I did NOT do

**Not verified**
- The `pipeforge-job` image build (needs SparkLake's `spark-base` from Task 2).
- GitHub Actions CI on the pushed branch.
- Behaviour against a real Hive Metastore. Integration tests use Spark's in-memory catalog, so a table created in one process is not visible to a later process. The real check comes in the end-to-end smoke test.
- The CLI `check` command end to end (only `validate` and `run` are exercised).

**Test gaps I left**
- `PostgresSink` has no tests (it is unchanged in behaviour apart from errors and connection handling).
- The old pandas-transformer and `SparkWrapper` config tests were deleted as print-based/non-discoverable, and **I did not write direct replacements**. `SparkWrapper` is exercised indirectly by the integration tests.
- Retry behaviour on JDBC reads is unit-tested through `retry_call`, not by killing Postgres mid-run.

**Known limits**
- Rows with NULL cursor values are never selected in incremental mode.
- Cursor fields must be timestamp/timestamptz/date; the read-partition column must be numeric.
- A late-committing transaction with an older timestamp can be missed; `lookback_seconds` mitigates it.
- `KeyVault` (hvac) was left untouched and untested.
- Test-image jars are downloaded without checksums, and image tags are pinned but not digest-pinned.
- The sink API change is breaking for any external callers.

## 11. Next
Task 2 (SparkLake `spark-base` and `spark-thriftserver` images, build script, CI) on a new SparkLake branch, then Task 3 on the same repo. Once `spark-base` exists, build `pipeforge-job` from it to close the open item above.
