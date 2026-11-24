# PipeForge

**PipeForge** replicates data from sources (PostgreSQL) into Delta tables on S3-compatible storage using PySpark. It can be used as a library or as a config-driven job (`pipeforge run --spec spec.json`) packaged as the `pipeforge-job` image.

## Standard stack

| Component | Version | Notes |
|---|---|---|
| Spark / Scala | 3.5.2 / 2.12 | |
| Delta Lake | 3.2.0 | Newest 3.x whose published `delta-spark` metadata allows pyspark 3.5.2 (3.2.1 and 3.3.x require `pyspark>=3.5.3`) |
| hadoop-aws / AWS SDK bundle | 3.3.4 / 1.12.262 | the bundle hadoop-aws 3.3.4 was built against |
| Python | 3.10 or 3.11 | the official Spark 3.5.2 image ships 3.10 |

Jars are baked into images (SparkLake `spark-base`); nothing is downloaded at runtime.

## Install

```bash
pip install pipeforge            # on an image that already contains Spark (spark-base)
pip install "pipeforge[spark]"   # bare Python: also installs pyspark==3.5.2 and delta-spark==3.2.0
```

## Job CLI

```bash
pipeforge validate --spec spec.json   # schema + cross-field checks only
pipeforge check    --spec spec.json   # + source and checkpoint-store connectivity
pipeforge run      --spec spec.json   # run the job
```

The spec can also come from `PIPEFORGE_SPEC_PATH` (file) or `PIPEFORGE_SPEC` (inline JSON).
Schema: [`schemas/job-spec.v1.schema.json`](schemas/job-spec.v1.schema.json) (versioned by `spec_version`); example: [`examples/spec.example.json`](examples/spec.example.json).

Secrets are **never inline**: `username`, `password`, `access_key`, `secret_key` are `{"env": "NAME"}` or `{"file": "/path"}` references, resolved in memory at start-up.

**Result line.** The last stdout line is always `PIPEFORGE_RESULT {json}` so a control plane can parse it from the logs:

```json
{"status":"succeeded","mode":"incremental","initial_load":false,"rows_read":2,"rows_written":2,
 "old_watermark":"2024-06-01T10:05:00+00:00","new_watermark":"2024-06-01T11:01:00+00:00",
 "duration_seconds":12.4,"run_id":"abc","spec_version":"1","error":null}
```

Exit codes: `0` success, `1` job failure, `2` invalid spec. On failure the result line has `"status":"failed"` and `error`.

### Load semantics

- **full**: replaces the target table. Watermark = max cursor value in the loaded data (source `now()` if the table is empty).
- **incremental**: selects rows where *any* cursor field `>=` the stored UTC watermark minus `lookback_seconds`, then MERGEs on `unique_key`. Reruns are idempotent, so overlap never creates duplicates. Rows whose cursor values are NULL are never selected. Use `lookback_seconds` if long transactions can commit rows with older timestamps.
- Incremental with **no checkpoint** does an initial full load (`initial_load: true`), or fails with `on_missing_checkpoint: "fail"`. A checkpoint **read error always fails the job**; it never falls back to a full refresh.
- Cursor fields must be `timestamp`/`timestamptz`/`date` columns. `read_partition_column` must be numeric.
- Duplicate unique keys in a batch fail with `DuplicateKeyError`, or keep the latest per key (`on_duplicate_keys: "latest"`). Schema evolution is opt-in (`schema_evolution: true`): by default new source columns are **not** added to the target (Delta ignores them in MERGE) and full refreshes replace the schema.
- Checkpoints: `s3://<checkpoint_bucket>/checkpoints/<source_db>/<source_schema>/<source_table>.json`. The bucket is not auto-created unless `create_checkpoint_bucket: true`.
- All timestamps are UTC end to end (Spark session time zone, JDBC `SET TIME ZONE 'UTC'`, UTC-aware checkpoints), so driver clock skew or zone (e.g. Tehran, UTC+03:30) cannot skip or duplicate rows.

## Image

`docker/Dockerfile` builds `pipeforge-job` **FROM SparkLake's published `spark-base`** (build args `SPARK_BASE_IMAGE`, `SPARK_BASE_TAG`), installs PipeForge without the `spark` extra, and uses the CLI as the entrypoint:

```bash
docker build -f docker/Dockerfile --build-arg SPARK_BASE_IMAGE=<registry>/spark-base \
  --build-arg SPARK_BASE_TAG=<tag> -t pipeforge-job:2.0.0 .
docker run --rm -e SOURCE_DB_USER -e S3_ACCESS_KEY -e S3_SECRET_KEY -e PIPEFORGE_SPEC="$(cat spec.json)" pipeforge-job:2.0.0 run
```

## Library use

```python
from pipeforge.engine.spark import SparkWrapper
from pipeforge.connector.postgresql import PostgresConnector
from pipeforge.sink.delta import DeltaSink
from pipeforge.checkpoint import Checkpoint

engine = SparkWrapper(enable_delta=True, warehouse_location="s3a://warehouse/",
                      metastore_uri="thrift://hive-metastore:9083",
                      s3={"endpoint": "https://s3.internal", "access_key": "...", "secret_key": "..."})
Checkpoint.configure_s3("checkpoints", "https://s3.internal", "...", "...")
connector = PostgresConnector(engine=engine, sink=DeltaSink(table="users", db_name="bronze", unique_field="id"),
                              cursor_fields=["updated_at"], host="...", port=5432, db="app",
                              username="...", password="...", table="Users")
result = connector.migrate()          # MigrationResult
```

Connection validation: `connector.validate_connection()` and `Checkpoint.validate_connection()`.

## Tests

Unit tests need no services; integration tests need Postgres and S3 and run in the Spark image:

```bash
docker compose -f docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from tests
```

The test runner deliberately uses `TZ=Asia/Tehran` to prove results do not depend on the driver's zone.

## Custom transformers

Subclass `pipeforge.transformer.Transformer`, implement `transform(df)`, and list `package.module:ClassName` under `transformers` in the spec (the module must be importable in the image).

Design decisions are recorded in [docs/adr](docs/adr/README.md). See [CHANGELOG.md](CHANGELOG.md) for breaking changes in 2.0.0.
