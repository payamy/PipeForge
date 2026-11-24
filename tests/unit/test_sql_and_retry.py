from datetime import datetime, timedelta, timezone

import pytest

from pipeforge.util.retry import is_transient, retry_call
from pipeforge.util.sql import (normalize_identifier, pg_column_list, pg_ident, pg_qualified,
                                pg_timestamptz_literal, spark_ident, spark_qualified, to_utc)


def test_identifier_quoting_is_consistent_for_plain_and_legacy_quoted_names():
    assert pg_ident("Id") == '"Id"'
    assert pg_ident('"Id"') == '"Id"'          # legacy style normalised, not double quoted
    assert spark_ident("Id") == "`Id`"
    assert spark_ident('"Id"') == "`Id`"
    assert normalize_identifier('"Id"') == "Id"


def test_identifiers_with_quotes_are_escaped_not_injected():
    assert pg_ident('a"; DROP TABLE x; --') == '"a""; DROP TABLE x; --"'
    assert spark_ident("a`b") == "`a``b`"
    assert pg_qualified("public", "Users") == '"public"."Users"'
    assert spark_qualified(None, "t") == "`t`"
    assert spark_qualified("db", "t") == "`db`.`t`"


@pytest.mark.parametrize("bad", ["", None, '""', "a\x00b"])
def test_invalid_identifiers_rejected(bad):
    with pytest.raises(ValueError):
        pg_ident(bad)


def test_column_list():
    assert pg_column_list(None) == "*"
    assert pg_column_list(["id", "Name"]) == '"id", "Name"'


def test_timestamp_literal_is_utc_for_any_input_zone():
    tehran = timezone(timedelta(hours=3, minutes=30))
    dt = datetime(2024, 5, 1, 3, 30, 0, 123456, tzinfo=tehran)
    assert pg_timestamptz_literal(dt) == "TIMESTAMPTZ '2024-05-01 00:00:00.123456+00'"
    assert pg_timestamptz_literal(datetime(2024, 5, 1)) == "TIMESTAMPTZ '2024-05-01 00:00:00.000000+00'"
    with pytest.raises(TypeError):
        pg_timestamptz_literal("2024-01-01'; DROP")


def test_to_utc_marks_naive_as_utc():
    assert to_utc(datetime(2024, 1, 1)).tzinfo == timezone.utc


def test_retry_retries_transient_then_succeeds():
    calls = []

    def fn():
        calls.append(1)
        if len(calls) < 3:
            raise RuntimeError("Connection refused")
        return "ok"

    sleeps = []
    assert retry_call(fn, attempts=3, sleep=sleeps.append) == "ok"
    assert len(calls) == 3 and len(sleeps) == 2


def test_retry_does_not_retry_permanent_errors():
    calls = []

    def fn():
        calls.append(1)
        raise RuntimeError("password authentication failed")

    with pytest.raises(RuntimeError):
        retry_call(fn, attempts=5, sleep=lambda s: None)
    assert len(calls) == 1


def test_retry_gives_up_after_attempts():
    calls = []

    def fn():
        calls.append(1)
        raise RuntimeError("timeout")

    with pytest.raises(RuntimeError):
        retry_call(fn, attempts=3, sleep=lambda s: None)
    assert len(calls) == 3


def test_is_transient():
    assert is_transient(RuntimeError("connection reset by peer"))
    assert not is_transient(RuntimeError("relation does not exist"))
