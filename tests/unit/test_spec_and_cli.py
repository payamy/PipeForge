import copy
import io
import json
from pathlib import Path

import pytest

from pipeforge import cli
from pipeforge.errors import SpecError
from pipeforge.spec import load_schema, read_spec, resolve_secrets, validate_spec

REPO = Path(__file__).resolve().parents[2]


def good_spec(**over):
    spec = {
        "spec_version": "1",
        "mode": "incremental",
        "source": {
            "type": "postgres",
            "connection": {"host": "pg", "database": "src",
                           "username": {"env": "PG_USER"}, "password": {"file": "/run/secrets/pg"}},
            "table": "users", "cursor_fields": ["updated_at"], "unique_key": ["id"],
        },
        "target": {"database": "bronze", "table": "users"},
        "storage": {"s3": {"endpoint": "http://s3:9000", "access_key": {"env": "A"}, "secret_key": {"env": "S"}},
                    "warehouse": "s3a://warehouse/", "checkpoint_bucket": "checkpoints"},
    }
    spec.update(over)
    return spec


def test_published_schema_matches_packaged_schema():
    published = json.loads((REPO / "schemas" / "job-spec.v1.schema.json").read_text())
    assert published == load_schema()


def test_good_spec_validates():
    validate_spec(good_spec())


def test_inline_secret_values_are_rejected():
    spec = good_spec()
    spec["source"]["connection"]["password"] = "hunter2"
    with pytest.raises(SpecError):
        validate_spec(spec)
    spec = good_spec()
    spec["storage"]["s3"]["secret_key"] = {"value": "x"}
    with pytest.raises(SpecError):
        validate_spec(spec)


def test_incremental_requires_cursor_fields():
    spec = good_spec()
    del spec["source"]["cursor_fields"]
    with pytest.raises(SpecError):
        validate_spec(spec)
    spec["mode"] = "full"
    validate_spec(spec)


@pytest.mark.parametrize("mutate", [
    lambda s: s.update(spec_version="2"),
    lambda s: s.update(mode="sometimes"),
    lambda s: s.update(unknown_key=1),
    lambda s: s["target"].update(on_duplicate_keys="ignore"),
    lambda s: s["storage"].update(warehouse="/local/path"),
])
def test_schema_violations(mutate):
    spec = copy.deepcopy(good_spec())
    mutate(spec)
    with pytest.raises(SpecError):
        validate_spec(spec)


def test_columns_must_include_cursor_and_key():
    spec = good_spec()
    spec["source"]["columns"] = ["id"]
    with pytest.raises(SpecError, match="updated_at"):
        validate_spec(spec)


def test_resolve_secrets_from_env_and_file(tmp_path):
    f = tmp_path / "pw"
    f.write_text("s3cret\n")
    spec = good_spec()
    spec["source"]["connection"]["password"] = {"file": str(f)}
    out = resolve_secrets(spec, {"PG_USER": "u", "A": "ak", "S": "sk"})
    assert out["source"]["connection"]["username"] == "u"
    assert out["source"]["connection"]["password"] == "s3cret"
    assert out["storage"]["s3"]["access_key"] == "ak"
    assert spec["source"]["connection"]["username"] == {"env": "PG_USER"}  # original untouched


def test_missing_secret_raises_without_leaking_values():
    with pytest.raises(SpecError, match="PG_USER"):
        resolve_secrets(good_spec(), {})


def test_read_spec_from_path_inline_env_and_nothing(tmp_path):
    p = tmp_path / "s.json"
    p.write_text(json.dumps(good_spec()))
    assert read_spec(str(p))["mode"] == "incremental"
    assert read_spec(None, {"PIPEFORGE_SPEC": json.dumps(good_spec())})["mode"] == "incremental"
    assert read_spec(None, {"PIPEFORGE_SPEC_PATH": str(p)})["mode"] == "incremental"
    with pytest.raises(SpecError):
        read_spec(None, {})
    p.write_text("{broken")
    with pytest.raises(SpecError):
        read_spec(str(p))


def last_line(text):
    return text.strip().splitlines()[-1]


def test_cli_validate_ok(tmp_path, capsys):
    p = tmp_path / "s.json"
    p.write_text(json.dumps(good_spec()))
    assert cli.main(["validate", "--spec", str(p)]) == 0


def test_cli_run_with_invalid_spec_exits_2_and_prints_failed_result(tmp_path, capsys):
    p = tmp_path / "s.json"
    p.write_text(json.dumps({"spec_version": "1"}))
    assert cli.main(["run", "--spec", str(p)]) == 2
    line = last_line(capsys.readouterr().out)
    assert line.startswith(cli.RESULT_PREFIX)
    result = json.loads(line[len(cli.RESULT_PREFIX):])
    assert result["status"] == "failed" and result["rows_read"] == 0 and "invalid job spec" in result["error"]


def test_cli_run_job_failure_exits_1_with_result_line(tmp_path, capsys, monkeypatch):
    p = tmp_path / "s.json"
    p.write_text(json.dumps(good_spec(run_id="r1")))
    from pipeforge import runner

    def boom(spec, environ=None):
        raise RuntimeError("source exploded")

    monkeypatch.setattr(runner, "run_spec", boom)
    assert cli.main(["run", "--spec", str(p)]) == 1
    result = json.loads(last_line(capsys.readouterr().out)[len(cli.RESULT_PREFIX):])
    assert result["status"] == "failed" and result["run_id"] == "r1"
    assert result["error"] == "RuntimeError: source exploded"


def test_cli_success_prints_final_result_line(tmp_path, capsys, monkeypatch):
    p = tmp_path / "s.json"
    p.write_text(json.dumps(good_spec()))
    from pipeforge import runner
    monkeypatch.setattr(runner, "run_spec", lambda spec, environ=None: {
        "status": "succeeded", "rows_read": 3, "rows_written": 3, "old_watermark": None,
        "new_watermark": "2024-01-01T00:00:00+00:00", "duration_seconds": 1.0})
    assert cli.main(["run", "--spec", str(p)]) == 0
    result = json.loads(last_line(capsys.readouterr().out)[len(cli.RESULT_PREFIX):])
    assert result["status"] == "succeeded" and result["rows_written"] == 3


def test_emit_result_is_single_line():
    buf = io.StringIO()
    cli.emit_result({"status": "succeeded", "x": "a\nb"}, buf)
    assert buf.getvalue().count("\n") == 1


def test_example_spec_is_valid():
    validate_spec(json.loads((REPO / "examples" / "spec.example.json").read_text()))
