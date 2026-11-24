"""Integration fixtures. Skipped unless PG_HOST and S3_ENDPOINT are set (see docker-compose.test.yml)."""
import os
import time
import uuid

import boto3
import psycopg2
import pytest

pytestmark = pytest.mark.integration

REQUIRED = ("PG_HOST", "S3_ENDPOINT")
if not all(os.getenv(k) for k in REQUIRED):
    pytest.skip("integration services not configured", allow_module_level=True)

WAREHOUSE_BUCKET = "warehouse"
CHECKPOINT_BUCKET = "checkpoints"


def _pg():
    return psycopg2.connect(host=os.environ["PG_HOST"], port=os.getenv("PG_PORT", "5432"),
                            dbname=os.environ["PG_DB"], user=os.environ["PG_USER"],
                            password=os.environ["PG_PASSWORD"])


@pytest.fixture(scope="session")
def s3():
    client = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"],
                          aws_access_key_id=os.environ["S3_ACCESS_KEY"],
                          aws_secret_access_key=os.environ["S3_SECRET_KEY"], region_name="us-east-1")
    deadline = time.time() + 120
    while True:                      # the S3 server has no healthcheck: wait until it answers
        try:
            client.list_buckets()
            break
        except Exception:  # noqa: BLE001
            if time.time() > deadline:
                raise
            time.sleep(2)
    for b in (WAREHOUSE_BUCKET, CHECKPOINT_BUCKET):
        try:
            client.create_bucket(Bucket=b)
        except client.exceptions.BucketAlreadyOwnedByYou:
            pass
    return client


@pytest.fixture(scope="session")
def spark_engine(s3):
    from pipeforge.engine.spark import SparkWrapper
    engine = SparkWrapper(
        master="local[2]", app_name="pipeforge-it", enable_delta=True,
        warehouse_location=f"s3a://{WAREHOUSE_BUCKET}/",
        spark_conf={"spark.ui.enabled": "false", "spark.sql.shuffle.partitions": "2",
                    "spark.driver.extraJavaOptions": "-Duser.timezone=Asia/Tehran"},
        s3={"endpoint": os.environ["S3_ENDPOINT"], "access_key": os.environ["S3_ACCESS_KEY"],
            "secret_key": os.environ["S3_SECRET_KEY"], "path_style": True})
    yield engine
    SparkWrapper.reset()


@pytest.fixture
def pg():
    conn = _pg()
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture
def names():
    suffix = uuid.uuid4().hex[:8]
    return {"src": f"Users_{suffix}", "tgt": f"users_{suffix}", "db": "bronze_it"}


@pytest.fixture
def checkpoints(s3):
    from pipeforge.checkpoint import Checkpoint
    Checkpoint.reset()
    Checkpoint.retry_attempts = 1
    Checkpoint.configure_s3(CHECKPOINT_BUCKET, os.environ["S3_ENDPOINT"], os.environ["S3_ACCESS_KEY"],
                            os.environ["S3_SECRET_KEY"], path_style=True)
    yield Checkpoint
    Checkpoint.reset()


@pytest.fixture
def make_connector(spark_engine, names):
    from pipeforge.connector.postgresql import PostgresConnector
    from pipeforge.sink.delta import DeltaSink

    def _make(on_duplicate_keys="fail", transformers=None, **kw):
        sink = DeltaSink(table=names["tgt"], db_name=names["db"], unique_field="Id",
                         on_duplicate_keys=on_duplicate_keys, order_by=["UpdatedAt"])
        return PostgresConnector(
            engine=spark_engine, sink=sink, cursor_fields=["UpdatedAt"],
            host=os.environ["PG_HOST"], port=os.getenv("PG_PORT", "5432"), db=os.environ["PG_DB"],
            username=os.environ["PG_USER"], password=os.environ["PG_PASSWORD"],
            table=names["src"], columns=["Id", "Name", "UpdatedAt"], read_partition_column="Id",
            partition_size=2, retry_attempts=1, transformers=transformers, **kw)
    return _make


@pytest.fixture
def source_table(pg, names):
    """Mixed-case identifiers on purpose: the quoting bug class."""
    with pg.cursor() as cur:
        cur.execute(f'CREATE TABLE public."{names["src"]}" ("Id" bigint PRIMARY KEY, "Name" text, '
                    '"UpdatedAt" timestamptz NOT NULL)')
    yield names["src"]
    with pg.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS public."{names["src"]}"')
