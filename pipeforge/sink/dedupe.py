"""Duplicate-key handling for MERGE batches."""
from __future__ import annotations

from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window

from pipeforge.errors import DuplicateKeyError, SinkError
from pipeforge.util.sql import normalize_identifier


def find_duplicate_keys(df: DataFrame, keys: List[str], limit: int = 5) -> List[dict]:
    cols = [F.col(f"`{normalize_identifier(k)}`") for k in keys]
    dups = df.groupBy(*cols).count().where(F.col("count") > 1).limit(limit).collect()
    return [r.asDict() for r in dups]


def dedupe(df: DataFrame, keys: List[str], policy: str, order_by: List[str] | None = None) -> DataFrame:
    """Apply ``policy`` to duplicate keys.

    ``fail``: raise DuplicateKeyError if any key appears more than once.
    ``latest``: keep one row per key, the one with the greatest ``order_by`` value.
    """
    keys = [normalize_identifier(k) for k in keys]
    if policy == "fail":
        dups = find_duplicate_keys(df, keys)
        if dups:
            raise DuplicateKeyError(
                f"batch has duplicate values for unique key {keys}, e.g. {dups}. "
                "Fix the source or set on_duplicate_keys='latest'.")
        return df
    if policy == "latest":
        if not order_by:
            raise SinkError("on_duplicate_keys='latest' requires order_by (the cursor fields)")
        order = [F.col(f"`{normalize_identifier(c)}`").desc_nulls_last() for c in order_by]
        part = [F.col(f"`{k}`") for k in keys]
        w = Window.partitionBy(*part).orderBy(*order)
        return (df.withColumn("__pf_rn", F.row_number().over(w))
                .where(F.col("__pf_rn") == 1).drop("__pf_rn"))
    raise SinkError(f"unknown duplicate-key policy: {policy!r}")
