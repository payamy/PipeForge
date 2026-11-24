from __future__ import annotations

import logging
import math
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Callable, List, Optional, TypeVar

import psycopg2
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DateType, TimestampNTZType, TimestampType

from pipeforge.checkpoint import Checkpoint
from pipeforge.connector.base import BaseConnector
from pipeforge.engine.spark import SparkWrapper
from pipeforge.errors import CheckpointError, PipeForgeError, SourceError
from pipeforge.sink import Sink
from pipeforge.util.retry import retry_call
from pipeforge.util.sql import (normalize_identifier, pg_column_list, pg_ident,
                                pg_qualified, pg_timestamptz_literal, to_utc)

log = logging.getLogger("pipeforge")
T = TypeVar("T")

_NUMERIC_PG_TYPES = {"smallint", "integer", "bigint", "numeric", "decimal", "real", "double precision"}
_TIME_PG_TYPES = {"timestamp without time zone", "timestamp with time zone", "date"}
_FRACTION_FMT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"


@dataclass
class MigrationResult:
    mode: str                      # "full" | "incremental"
    initial_load: bool             # incremental requested but no checkpoint -> full load
    rows_read: int
    rows_written: int
    old_watermark: Optional[datetime]
    new_watermark: Optional[datetime]

    def to_dict(self) -> dict:
        d = asdict(self)
        for k in ("old_watermark", "new_watermark"):
            d[k] = d[k].isoformat() if d[k] else None
        return d


class PostgresConnector(BaseConnector):
    """Fetches data from a Postgres table and loads it into the provided sink."""

    def __init__(self, engine: SparkWrapper, sink: Sink, cursor_fields: str | List[str],
                 host: str, port: str | int, db: str, username: str, password: str, table: str,
                 columns: List[str] | None = None, schema: str = "public",
                 transformers: List[type] | None = None,
                 read_partition_column: str | None = None, partition_size: int = 10000,
                 max_partitions: int = 64, fetch_size: int = 10000, sslmode: str | None = None,
                 retry_attempts: int = 3, retry_base_delay: float = 1.0, **kwargs):
        super().__init__(engine=engine, sink=sink, cursor_fields=cursor_fields,
                         transformers=transformers, **kwargs)
        self._host, self._port, self._db = host, port, db
        self._username, self._password = username, password
        self._table = normalize_identifier(table)
        self._schema = normalize_identifier(schema)
        self._columns = [normalize_identifier(c) for c in columns] if columns else None
        if isinstance(cursor_fields, str):
            cursor_fields = [cursor_fields]
        self._cursor_fields = [normalize_identifier(c) for c in (cursor_fields or [])]
        self._partition_col = normalize_identifier(read_partition_column) if read_partition_column else None
        self._partition_size = max(1, int(partition_size))
        self._max_partitions = max(1, int(max_partitions))
        self._fetch_size = fetch_size
        self._sslmode = sslmode
        self._retry_attempts = retry_attempts
        self._retry_base_delay = retry_base_delay

    # ----------------------------------------------------------------- helpers
    @property
    def _jdbc_url(self) -> str:
        url = f"jdbc:postgresql://{self._host}:{self._port}/{self._db}"
        return url + (f"?sslmode={self._sslmode}" if self._sslmode else "")

    def _retry(self, fn: Callable[[], T], what: str) -> T:
        return retry_call(fn, attempts=self._retry_attempts, base_delay=self._retry_base_delay, what=what)

    def _reader(self, query: str):
        return (self.engine.spark.read.format("jdbc")
                .option("url", self._jdbc_url)
                .option("dbtable", f"({query}) AS pf_query")
                .option("user", self._username).option("password", self._password)
                .option("driver", "org.postgresql.Driver")
                .option("fetchsize", self._fetch_size)
                .option("sessionInitStatement", "SET TIME ZONE 'UTC'"))

    def _scalar_row(self, query: str, what: str):
        return self._retry(lambda: self._reader(query).load().collect()[0], what)

    @property
    def _qualified(self) -> str:
        return pg_qualified(self._schema, self._table)

    def _connect(self):
        return psycopg2.connect(host=self._host, port=self._port, dbname=self._db,
                                user=self._username, password=self._password,
                                connect_timeout=10, sslmode=self._sslmode or "prefer")

    # ------------------------------------------------------------- validation
    def validate_connection(self) -> None:
        """Check connectivity, table/column existence and cursor/partition column types."""
        def _check():
            conn = self._connect()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT column_name, data_type FROM information_schema.columns "
                                "WHERE table_schema = %s AND table_name = %s",
                                (self._schema, self._table))
                    return dict(cur.fetchall())
            finally:
                conn.close()
        try:
            cols = self._retry(_check, "validate source connection")
        except Exception as exc:  # noqa: BLE001
            raise SourceError(f"cannot connect to source {self._host}:{self._port}/{self._db}: {exc}") from exc
        if not cols:
            raise SourceError(f"table {self._schema}.{self._table} not found (or no privileges)")
        missing = [c for c in (self._columns or []) + self._cursor_fields if c not in cols]
        if missing:
            raise SourceError(f"columns not found in {self._schema}.{self._table}: {sorted(set(missing))}")
        for c in self._cursor_fields:
            if cols[c] not in _TIME_PG_TYPES:
                raise SourceError(f"cursor field '{c}' has type {cols[c]}; timestamp/date required")
        if self._partition_col:
            if self._partition_col not in cols:
                raise SourceError(f"read partition column '{self._partition_col}' not found")
            if cols[self._partition_col] not in _NUMERIC_PG_TYPES:
                raise SourceError(f"read partition column '{self._partition_col}' must be numeric")
        self._check_selected_columns()

    def _check_selected_columns(self) -> None:
        if self._columns:
            absent = [c for c in self._cursor_fields if c not in self._columns]
            if absent:
                raise SourceError(f"cursor fields {absent} must be included in 'columns'")

    # ---------------------------------------------------------------- migrate
    def migrate(self, full_refresh: bool = False, full_refresh_partition_field: str | None = None,
                incremental_offset: timedelta = timedelta(0),
                on_missing_checkpoint: str = "full_refresh") -> MigrationResult:
        """Replicate the table.

        ``full_refresh=True`` always replaces the target. Otherwise the stored
        checkpoint selects an incremental load. A *missing* checkpoint triggers an
        initial full load (or an error when ``on_missing_checkpoint='fail'``); a
        checkpoint *read error* always raises and never degrades to a full refresh.
        """
        if full_refresh_partition_field:
            self._partition_col = normalize_identifier(full_refresh_partition_field)
        self._check_selected_columns()
        if not full_refresh and not self._cursor_fields:
            raise SourceError("incremental mode requires cursor_fields")

        old_wm: datetime | None = None
        initial = False
        if full_refresh:
            mode = "full"
        else:
            old_wm = Checkpoint.get_checkpoint(self._db, self._table, self._schema)  # raises on error
            if old_wm is None:
                if on_missing_checkpoint == "fail":
                    raise CheckpointError("no checkpoint found and on_missing_checkpoint='fail'")
                mode, initial = "full", True
                log.info("no checkpoint for %s.%s: performing initial full load", self._schema, self._table)
            else:
                mode = "incremental"

        where, since = None, None
        if mode == "incremental":
            since = to_utc(old_wm) - incremental_offset
            where = " OR ".join(f"{pg_ident(c)} >= {pg_timestamptz_literal(since)}"
                                for c in self._cursor_fields)

        source_now = self._source_now_utc() if mode == "full" else None
        count, lo, hi = self._read_stats(where)
        if mode == "incremental" and count == 0:
            log.info("no new rows since %s", since)
            return MigrationResult(mode, initial, 0, 0, old_wm, old_wm)

        raw = self._load(where, count, lo, hi)
        raw.cache()
        try:
            rows_read = self._retry(raw.count, "read source rows")
            new_wm = self._max_cursor(raw) or (source_now if mode == "full" else old_wm)

            try:
                df = self._handle_transforms(raw)
            except Exception as exc:  # noqa: BLE001
                raise PipeForgeError(f"transformer chain failed: {exc}") from exc

            if mode == "full":
                rows_written = self.sink.full_load(df, self.engine.spark)
            else:
                rows_written = self.sink.incremental_load(df, self.engine.spark)
        finally:
            raw.unpersist()

        if new_wm is not None:
            Checkpoint.set_checkpoint(new_wm, self._db, self._table, self._schema)  # raises on failure
        return MigrationResult(mode, initial, rows_read, rows_written, old_wm, new_wm)

    # --------------------------------------------------------- source queries
    def _source_now_utc(self) -> datetime:
        row = self._scalar_row(
            "SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US') AS now_utc",
            "read source clock")
        return to_utc(datetime.fromisoformat(row[0]))

    def _read_stats(self, where: str | None):
        pc = pg_ident(self._partition_col) if self._partition_col else None
        lo_hi = f", MIN({pc})::text AS lo, MAX({pc})::text AS hi" if pc else ", NULL::text AS lo, NULL::text AS hi"
        query = f"SELECT COUNT(*) AS cnt{lo_hi} FROM {self._qualified}" + (f" WHERE {where}" if where else "")
        try:
            row = self._scalar_row(query, "read source statistics")
        except Exception as exc:  # noqa: BLE001
            raise SourceError(f"error getting source statistics: {exc}") from exc
        return int(row[0]), row[1], row[2]

    def _load(self, where: str | None, count: int, lo, hi) -> DataFrame:
        query = f"SELECT {pg_column_list(self._columns)} FROM {self._qualified}" + \
                (f" WHERE {where}" if where else "")
        reader = self._reader(query)
        partitions = min(self._max_partitions, max(1, math.ceil(count / self._partition_size)))
        if self._partition_col and partitions > 1 and lo is not None and hi is not None and lo != hi:
            reader = (reader.option("partitionColumn", self._partition_col)
                      .option("lowerBound", lo).option("upperBound", hi)
                      .option("numPartitions", partitions))
        try:
            return reader.load()
        except Exception as exc:  # noqa: BLE001
            raise SourceError(f"error fetching data from postgres: {exc}") from exc

    def _max_cursor(self, df: DataFrame) -> datetime | None:
        """Max of all cursor fields in this batch, as aware UTC (session tz is UTC)."""
        exprs = []
        for c in self._cursor_fields:
            dtype = df.schema[c].dataType
            if not isinstance(dtype, (TimestampType, TimestampNTZType, DateType)):
                raise SourceError(f"cursor field '{c}' has Spark type {dtype.simpleString()}; "
                                  "timestamp/date required")
            exprs.append(F.date_format(F.max(F.col(f"`{c}`")), _FRACTION_FMT).alias(c))
        if not exprs:
            return None
        row = df.agg(*exprs).collect()[0]
        values = [datetime.fromisoformat(v).replace(tzinfo=timezone.utc) for v in row if v]
        return max(values) if values else None
