from datetime import datetime, timedelta, timezone

import pytest

from pipeforge.errors import CheckpointError, DuplicateKeyError

pytestmark = pytest.mark.integration
UTC = timezone.utc
TEHRAN = timezone(timedelta(hours=3, minutes=30))


def insert(pg, table, rows):
    with pg.cursor() as cur:
        for rid, name, ts in rows:
            cur.execute(f'INSERT INTO public."{table}" ("Id","Name","UpdatedAt") VALUES (%s,%s,%s) '
                        'ON CONFLICT ("Id") DO UPDATE SET "Name"=excluded."Name","UpdatedAt"=excluded."UpdatedAt"',
                        (rid, name, ts))


def target_rows(spark, names):
    return {r["Id"]: r["Name"] for r in spark.table(f"{names['db']}.{names['tgt']}").collect()}


def test_full_load_sets_utc_watermark_from_batch(spark_engine, make_connector, source_table, names, pg, checkpoints):
    # 03:30 +03:30 on 2024-05-01 is 00:00 UTC; the watermark must be UTC regardless of driver TZ (Tehran).
    insert(pg, source_table, [(1, "a", datetime(2024, 5, 1, 3, 30, tzinfo=TEHRAN)),
                              (2, "b", datetime(2024, 5, 1, 4, 30, tzinfo=TEHRAN))])
    res = make_connector().migrate(full_refresh=True)
    assert (res.mode, res.rows_read, res.rows_written) == ("full", 2, 2)
    assert res.new_watermark == datetime(2024, 5, 1, 1, 0, tzinfo=UTC)
    stored = checkpoints.get_checkpoint("source", source_table, "public")
    assert stored == res.new_watermark and stored.utcoffset() == timedelta(0)
    assert target_rows(spark_engine.spark, names) == {1: "a", 2: "b"}


def test_rerunning_incremental_produces_no_duplicates(spark_engine, make_connector, source_table, names, pg,
                                                      checkpoints):
    t0 = datetime(2024, 6, 1, 10, 0, tzinfo=UTC)
    insert(pg, source_table, [(i, f"n{i}", t0 + timedelta(minutes=i)) for i in range(1, 6)])
    first = make_connector().migrate()                       # no checkpoint -> initial full load
    assert first.mode == "full" and first.initial_load and first.rows_written == 5

    insert(pg, source_table, [(3, "n3-updated", t0 + timedelta(hours=1)), (6, "n6", t0 + timedelta(hours=1, minutes=1))])
    second = make_connector().migrate()
    # '>=' re-reads the boundary row (id 5 at the old watermark) on purpose, plus the 2 new/changed rows
    assert second.mode == "incremental" and second.rows_read == 3 and second.old_watermark == first.new_watermark
    expected = {1: "n1", 2: "n2", 3: "n3-updated", 4: "n4", 5: "n5", 6: "n6"}
    assert target_rows(spark_engine.spark, names) == expected

    # rerun with nothing changed, then rerun with a wide look-back that re-reads everything
    third = make_connector().migrate()
    assert third.rows_read <= 1                              # '>=' re-reads at most the boundary row
    fourth = make_connector().migrate(incremental_offset=timedelta(days=30))
    assert fourth.rows_read == 6
    df = spark_engine.spark.table(f"{names['db']}.{names['tgt']}")
    assert df.count() == 6 and df.select("Id").distinct().count() == 6
    assert target_rows(spark_engine.spark, names) == expected


def test_no_new_rows_is_a_noop_that_keeps_watermark(make_connector, source_table, pg, checkpoints):
    insert(pg, source_table, [(1, "a", datetime(2024, 1, 1, tzinfo=UTC))])
    first = make_connector().migrate(full_refresh=True)
    pg_cur = pg.cursor()
    pg_cur.execute(f'UPDATE public."{source_table}" SET "UpdatedAt" = "UpdatedAt" - interval \'1 day\'')
    res = make_connector().migrate()
    assert res.rows_read == 0 and res.new_watermark == first.new_watermark


def test_full_refresh_of_empty_table_does_not_crash(spark_engine, make_connector, source_table, names, checkpoints):
    res = make_connector().migrate(full_refresh=True)
    assert res.rows_read == 0 and res.rows_written == 0
    assert res.new_watermark is not None and res.new_watermark.tzinfo is not None   # falls back to source now()
    assert target_rows(spark_engine.spark, names) == {}


def test_partition_size_below_one_is_clamped(make_connector, source_table, pg, checkpoints):
    insert(pg, source_table, [(i, "x", datetime(2024, 1, 1, tzinfo=UTC)) for i in range(1, 4)])
    conn = make_connector()
    conn._partition_size = 1
    assert conn.migrate(full_refresh=True).rows_read == 3


def test_checkpoint_read_error_never_degrades_into_full_refresh(spark_engine, make_connector, source_table, names,
                                                               pg, checkpoints):
    insert(pg, source_table, [(1, "keep", datetime(2024, 1, 1, tzinfo=UTC))])
    make_connector().migrate(full_refresh=True)
    insert(pg, source_table, [(1, "SHOULD-NOT-OVERWRITE", datetime(2024, 1, 2, tzinfo=UTC))])

    class Broken:
        def get_object(self, **kw):
            from botocore.exceptions import ClientError
            raise ClientError({"Error": {"Code": "AccessDenied", "Message": "x"}}, "GetObject")

    checkpoints.s3_client = Broken()
    with pytest.raises(CheckpointError):
        make_connector().migrate()
    assert target_rows(spark_engine.spark, names) == {1: "keep"}


def test_on_missing_checkpoint_fail(make_connector, source_table, checkpoints):
    with pytest.raises(CheckpointError):
        make_connector().migrate(on_missing_checkpoint="fail")


def test_duplicate_keys_in_batch_fail_clearly_and_checkpoint_does_not_move(spark_engine, make_connector,
                                                                          source_table, names, pg, checkpoints):
    insert(pg, source_table, [(1, "a", datetime(2024, 1, 1, tzinfo=UTC))])
    first = make_connector().migrate(full_refresh=True)

    from pipeforge.transformer import Transformer

    class DupT(Transformer):
        def transform(self, df):
            return df.unionByName(df)

    insert(pg, source_table, [(2, "b", datetime(2024, 1, 2, tzinfo=UTC))])
    with pytest.raises(DuplicateKeyError):
        make_connector(transformers=[DupT]).migrate()
    assert checkpoints.get_checkpoint("source", source_table, "public") == first.new_watermark

    res = make_connector(on_duplicate_keys="latest", transformers=[DupT]).migrate()
    assert res.rows_written == 2          # boundary row id 1 is re-read, plus id 2; duplicates collapsed per key
    assert target_rows(spark_engine.spark, names) == {1: "a", 2: "b"}


def test_schema_evolution_is_opt_in(spark_engine, source_table, names, pg, checkpoints):
    from pipeforge.connector.postgresql import PostgresConnector
    from pipeforge.sink.delta import DeltaSink
    import os

    def conn(evolve, cols):
        return PostgresConnector(
            engine=spark_engine, sink=DeltaSink(table=names["tgt"], db_name=names["db"], unique_field="Id",
                                                schema_evolution=evolve),
            cursor_fields=["UpdatedAt"], host=os.environ["PG_HOST"], port=os.getenv("PG_PORT", "5432"),
            db=os.environ["PG_DB"], username=os.environ["PG_USER"], password=os.environ["PG_PASSWORD"],
            table=names["src"], columns=cols, retry_attempts=1)

    insert(pg, source_table, [(1, "a", datetime(2024, 1, 1, tzinfo=UTC))])
    conn(False, ["Id", "UpdatedAt"]).migrate(full_refresh=True)
    insert(pg, source_table, [(2, "b", datetime(2024, 1, 2, tzinfo=UTC))])
    # without opt-in the target schema must not change (Delta ignores extra source columns in MERGE *)
    conn(False, ["Id", "Name", "UpdatedAt"]).migrate()
    assert "Name" not in spark_engine.spark.table(f"{names['db']}.{names['tgt']}").columns
    conn(True, ["Id", "Name", "UpdatedAt"]).migrate(incremental_offset=timedelta(days=1))
    assert "Name" in spark_engine.spark.table(f"{names['db']}.{names['tgt']}").columns


def test_validate_connection(make_connector, source_table, names):
    make_connector().validate_connection()
    bad = make_connector()
    bad._table = "does_not_exist"
    from pipeforge.errors import SourceError
    with pytest.raises(SourceError):
        bad.validate_connection()


def test_cursor_type_is_validated(make_connector, source_table):
    conn = make_connector()
    conn._cursor_fields = ["Name"]
    from pipeforge.errors import SourceError
    with pytest.raises(SourceError, match="timestamp/date required"):
        conn.validate_connection()
