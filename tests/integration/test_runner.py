"""End to end through the spec runner: secrets come from env vars, result is parseable."""
import json
import os
from datetime import datetime, timezone

import pytest

from pipeforge import cli, runner

pytestmark = pytest.mark.integration


def spec_for(names, mode):
    return {
        "spec_version": "1", "mode": mode, "run_id": "it-1",
        "source": {"type": "postgres",
                   "connection": {"host": os.environ["PG_HOST"], "port": int(os.getenv("PG_PORT", "5432")),
                                  "database": os.environ["PG_DB"],
                                  "username": {"env": "PG_USER"}, "password": {"env": "PG_PASSWORD"}},
                   "table": names["src"], "columns": ["Id", "Name", "UpdatedAt"],
                   "cursor_fields": ["UpdatedAt"], "unique_key": ["Id"], "read_partition_column": "Id"},
        "target": {"database": names["db"], "table": names["tgt"], "on_duplicate_keys": "latest"},
        "storage": {"s3": {"endpoint": os.environ["S3_ENDPOINT"],
                           "access_key": {"env": "S3_ACCESS_KEY"}, "secret_key": {"env": "S3_SECRET_KEY"}},
                    "warehouse": "s3a://warehouse/", "checkpoint_bucket": "checkpoints"},
        "retry": {"attempts": 1},
    }


def test_run_spec_full_then_incremental(spark_engine, source_table, names, pg, checkpoints):
    with pg.cursor() as cur:
        cur.execute(f'INSERT INTO public."{source_table}" VALUES (1,\'a\',%s),(2,\'b\',%s)',
                    (datetime(2024, 1, 1, tzinfo=timezone.utc), datetime(2024, 1, 2, tzinfo=timezone.utc)))
    first = runner.run_spec(spec_for(names, "full"))
    assert first["status"] == "succeeded" and first["rows_written"] == 2
    assert first["new_watermark"] == "2024-01-02T00:00:00+00:00" and first["run_id"] == "it-1"
    assert first["duration_seconds"] >= 0

    with pg.cursor() as cur:
        cur.execute(f'INSERT INTO public."{source_table}" VALUES (3,\'c\',%s)', (datetime(2024, 1, 3, tzinfo=timezone.utc),))
    second = runner.run_spec(spec_for(names, "incremental"))
    assert second["mode"] == "incremental" and second["old_watermark"] == first["new_watermark"]
    assert spark_engine.spark.table(f"{names['db']}.{names['tgt']}").count() == 3
    json.dumps(second)  # serialisable for the final result line


def test_cli_prints_final_result_line(spark_engine, source_table, names, pg, checkpoints, tmp_path, capsys):
    with pg.cursor() as cur:
        cur.execute(f'INSERT INTO public."{source_table}" VALUES (1,\'a\',%s)', (datetime(2024, 1, 1, tzinfo=timezone.utc),))
    p = tmp_path / "spec.json"
    p.write_text(json.dumps(spec_for(names, "full")))
    assert cli.main(["run", "--spec", str(p)]) == 0
    last = capsys.readouterr().out.strip().splitlines()[-1]
    result = json.loads(last[len(cli.RESULT_PREFIX):])
    assert result["status"] == "succeeded" and result["rows_read"] == 1
    # job failure -> non-zero exit and a failed result line
    broken = spec_for(names, "full")
    broken["source"]["table"] = "nope_" + names["src"]
    p.write_text(json.dumps(broken))
    assert cli.main(["run", "--spec", str(p)]) == 1
    last = capsys.readouterr().out.strip().splitlines()[-1]
    assert json.loads(last[len(cli.RESULT_PREFIX):])["status"] == "failed"
