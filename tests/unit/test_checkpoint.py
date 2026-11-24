from datetime import datetime, timedelta, timezone

import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from pipeforge.checkpoint import Checkpoint
from pipeforge.errors import CheckpointError

TEHRAN = timezone(timedelta(hours=3, minutes=30))


@pytest.fixture(autouse=True)
def _fast_and_clean(monkeypatch):
    Checkpoint.reset()
    monkeypatch.setattr(Checkpoint, "retry_attempts", 1)
    yield
    Checkpoint.reset()


class _Failing:
    """S3 client stub that fails every call with a given error code."""

    def __init__(self, code, status=400):
        self.err = ClientError({"Error": {"Code": code, "Message": "boom"},
                                "ResponseMetadata": {"HTTPStatusCode": status}}, "op")

    def get_object(self, **kw):
        raise self.err

    def put_object(self, **kw):
        raise self.err

    def head_bucket(self, **kw):
        raise self.err


def test_local_roundtrip_is_timezone_aware_utc(tmp_path):
    Checkpoint.configure_local(tmp_path)
    assert Checkpoint.get_checkpoint("db", "t", "public") is None
    Checkpoint.set_checkpoint(datetime(2024, 5, 1, 3, 30, tzinfo=TEHRAN), "db", "t", "public")
    got = Checkpoint.get_checkpoint("db", "t", "public")
    assert got == datetime(2024, 5, 1, 0, 0, tzinfo=timezone.utc)
    assert got.utcoffset() == timedelta(0)
    assert (tmp_path / "checkpoints" / "db" / "public" / "t.json").exists()


def test_naive_input_is_treated_as_utc(tmp_path):
    Checkpoint.configure_local(tmp_path)
    Checkpoint.set_checkpoint(datetime(2024, 1, 1, 12, 0), "db", "t")
    assert Checkpoint.get_checkpoint("db", "t") == datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)


def test_legacy_naive_txt_checkpoint_is_read_as_utc(tmp_path):
    Checkpoint.configure_local(tmp_path)
    (tmp_path / "db-public-t.txt").write_text("2024-03-01 10:00:00")
    assert Checkpoint.get_checkpoint("db", "t", "public") == datetime(2024, 3, 1, 10, tzinfo=timezone.utc)


def test_corrupt_checkpoint_raises(tmp_path):
    Checkpoint.configure_local(tmp_path)
    p = tmp_path / "checkpoints" / "db" / "public" / "t.json"
    p.parent.mkdir(parents=True)
    p.write_text("not a checkpoint")
    with pytest.raises(CheckpointError):
        Checkpoint.get_checkpoint("db", "t", "public")


@mock_aws
def test_s3_not_found_is_none_but_roundtrip_works():
    Checkpoint.configure_s3("cpbucket", endpoint_url=None, access_key="k", secret_key="s", create_bucket=True)
    assert Checkpoint.get_checkpoint("db", "t", "public") is None
    Checkpoint.set_checkpoint(datetime(2024, 1, 2, tzinfo=timezone.utc), "db", "t", "public")
    assert Checkpoint.get_checkpoint("db", "t", "public") == datetime(2024, 1, 2, tzinfo=timezone.utc)
    keys = [o["Key"] for o in Checkpoint.s3_client.list_objects_v2(Bucket="cpbucket")["Contents"]]
    assert keys == ["checkpoints/db/public/t.json"]


@mock_aws
def test_missing_bucket_raises_unless_creation_is_enabled():
    with pytest.raises(CheckpointError, match="does not exist"):
        Checkpoint.configure_s3("nope", endpoint_url=None, access_key="k", secret_key="s")
    Checkpoint.configure_s3("nope", endpoint_url=None, access_key="k", secret_key="s", create_bucket=True)
    assert Checkpoint.use_s3


@pytest.mark.parametrize("code,status", [("AccessDenied", 403), ("InternalError", 500), ("NoSuchBucket", 404)])
def test_s3_errors_raise_instead_of_looking_like_not_found(code, status):
    Checkpoint.use_s3, Checkpoint.s3_bucket_name = True, "cp"
    Checkpoint.s3_client = _Failing(code, status)
    with pytest.raises(CheckpointError):
        Checkpoint.get_checkpoint("db", "t")


def test_set_checkpoint_raises_on_s3_failure():
    Checkpoint.use_s3, Checkpoint.s3_bucket_name = True, "cp"
    Checkpoint.s3_client = _Failing("AccessDenied", 403)
    with pytest.raises(CheckpointError):
        Checkpoint.set_checkpoint(datetime.now(timezone.utc), "db", "t")


def test_unconfigured_s3_raises():
    Checkpoint.use_s3 = True
    with pytest.raises(CheckpointError):
        Checkpoint.get_checkpoint("db", "t")


def test_get_uses_direct_key_and_never_lists(tmp_path):
    calls = []

    class Spy(_Failing):
        def __init__(self):
            super().__init__("NoSuchKey", 404)

        def get_object(self, **kw):
            calls.append(kw["Key"])
            raise self.err

        def list_objects_v2(self, **kw):  # pragma: no cover
            raise AssertionError("must not list the bucket")

        def get_paginator(self, *a):  # pragma: no cover
            raise AssertionError("must not list the bucket")

    Checkpoint.use_s3, Checkpoint.s3_bucket_name, Checkpoint.s3_client = True, "cp", Spy()
    assert Checkpoint.get_checkpoint("db", "t", "public") is None
    assert calls == ["checkpoints/db/public/t.json", "db-public-t.txt"]
