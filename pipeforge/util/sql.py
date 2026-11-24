"""The single place where identifiers and literals are quoted.

PostgreSQL quotes identifiers with double quotes, Spark SQL with backticks.
Callers pass *plain* names (``Id``, ``Users``). Names that arrive already
wrapped in double quotes (the legacy ``'"Id"'`` style) are normalised.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, List


def normalize_identifier(name: str) -> str:
    """Return the plain (unquoted, unescaped) form of an identifier."""
    if not isinstance(name, str) or not name:
        raise ValueError(f"invalid identifier: {name!r}")
    if len(name) >= 2 and name[0] == '"' and name[-1] == '"':
        name = name[1:-1].replace('""', '"')
    if not name or "\x00" in name:
        raise ValueError(f"invalid identifier: {name!r}")
    return name


def pg_ident(name: str) -> str:
    return '"' + normalize_identifier(name).replace('"', '""') + '"'


def pg_qualified(schema: str, table: str) -> str:
    return f"{pg_ident(schema)}.{pg_ident(table)}"


def pg_column_list(columns: Iterable[str] | None) -> str:
    cols: List[str] = list(columns or [])
    return ", ".join(pg_ident(c) for c in cols) if cols else "*"


def spark_ident(name: str) -> str:
    return "`" + normalize_identifier(name).replace("`", "``") + "`"


def spark_qualified(database: str | None, table: str) -> str:
    if database:
        return f"{spark_ident(database)}.{spark_ident(table)}"
    return spark_ident(table)


def to_utc(dt: datetime) -> datetime:
    """Return an aware UTC datetime. Naive values are *assumed* to be UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def pg_timestamptz_literal(dt: datetime) -> str:
    """Safe literal built from a datetime object (never from free text)."""
    if not isinstance(dt, datetime):
        raise TypeError("expected datetime")
    return "TIMESTAMPTZ '" + to_utc(dt).strftime("%Y-%m-%d %H:%M:%S.%f") + "+00'"
