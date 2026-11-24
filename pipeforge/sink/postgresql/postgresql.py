from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras as extras
from pyspark.sql import DataFrame, SparkSession

from pipeforge.errors import SinkError
from pipeforge.sink.base import BaseSink
from pipeforge.util.sql import normalize_identifier, pg_ident, pg_qualified


class PostgresSink(BaseSink):
    """Sink for loading data into a PostgreSQL table."""

    def __init__(self, host: str, port: str | int, db: str, username: str, password: str,
                 table: str, schema: str = "public",
                 unique_field: str | List[str] | None = "Id", **kwargs):
        super().__init__(**kwargs)
        self._host, self._port, self._db = host, port, db
        self._username, self._password = username, password
        self._table = normalize_identifier(table)
        self._schema = normalize_identifier(schema)
        if unique_field is None:
            self._unique_fields: List[str] = []
        elif isinstance(unique_field, str):
            self._unique_fields = [normalize_identifier(unique_field)]
        else:
            self._unique_fields = [normalize_identifier(c) for c in unique_field]

    def full_load(self, df: DataFrame, spark: SparkSession, **kwargs) -> int:
        try:
            rows = df.count()
            (df.write.format("jdbc").mode("overwrite")
             .option("url", f"jdbc:postgresql://{self._host}:{self._port}/{self._db}")
             .option("dbtable", pg_qualified(self._schema, self._table))
             .option("truncate", True)
             .option("user", self._username).option("password", self._password)
             .option("driver", "org.postgresql.Driver").save())
            return rows
        except Exception as exc:  # noqa: BLE001
            raise SinkError(f"full load into Postgres failed: {exc}") from exc

    def incremental_load(self, df: DataFrame, spark: SparkSession, **kwargs) -> int:
        try:
            cols = [pg_ident(c) for c in df.columns]
            non_key = [c for c in df.columns if c not in self._unique_fields]
            if self._unique_fields:
                keys = ", ".join(pg_ident(k) for k in self._unique_fields)
                sets = ", ".join(f"{pg_ident(c)} = excluded.{pg_ident(c)}" for c in non_key)
                conflict = f"ON CONFLICT ({keys}) " + (f"DO UPDATE SET {sets}" if sets else "DO NOTHING")
            else:
                conflict = ""
            query = (f"INSERT INTO {pg_qualified(self._schema, self._table)} ({', '.join(cols)}) "
                     f"VALUES %s {conflict}")
            p_df: pd.DataFrame = df.toPandas().astype(object).replace({np.nan: None, pd.NaT: None})
            tuples = [tuple(x) for x in p_df.to_numpy()]
            conn = psycopg2.connect(host=self._host, port=self._port, dbname=self._db,
                                    user=self._username, password=self._password)
            try:
                with conn, conn.cursor() as cur:
                    if tuples:
                        extras.execute_values(cur, query, tuples)
            finally:
                conn.close()
            return len(tuples)
        except Exception as exc:  # noqa: BLE001
            raise SinkError(f"incremental load into Postgres failed: {exc}") from exc
