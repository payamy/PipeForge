from __future__ import annotations

import logging
import uuid
from typing import List

from pyspark.sql import DataFrame, SparkSession

from pipeforge.errors import PipeForgeError, SinkError
from pipeforge.sink.base import BaseSink
from pipeforge.sink.dedupe import dedupe
from pipeforge.util.sql import normalize_identifier, spark_ident, spark_qualified

log = logging.getLogger("pipeforge")


def _as_list(value) -> List[str]:
    if value is None:
        return []
    return [value] if isinstance(value, str) else list(value)


class DeltaSink(BaseSink):

    def __init__(self, table: str, partition_col: str | List[str] | None = None,
                 unique_field: str | List[str] | None = "Id", hive_style: bool = False,
                 db_name: str | None = None, on_duplicate_keys: str = "fail",
                 order_by: str | List[str] | None = None, schema_evolution: bool = False,
                 **kwargs):
        """
        :param table: destination table name (plain, unquoted)
        :param partition_col: column(s) to partition the Delta table by
        :param unique_field: merge key column(s)
        :param on_duplicate_keys: 'fail' (default) or 'latest' (keep greatest ``order_by``)
        :param order_by: columns that decide which duplicate is 'latest' (usually cursor fields)
        :param schema_evolution: opt in to Delta automatic schema evolution on MERGE and on overwrite
        """
        super().__init__(**kwargs)
        self._table = normalize_identifier(table)
        self._partition_cols = [normalize_identifier(c) for c in _as_list(partition_col)]
        self._hive_style = hive_style
        self.db_name = normalize_identifier(db_name) if db_name else None
        self._unique_fields = [normalize_identifier(c) for c in _as_list(unique_field)]
        self._on_duplicate_keys = on_duplicate_keys
        self._order_by = _as_list(order_by)
        self._schema_evolution = schema_evolution

    @property
    def qualified_name(self) -> str:
        return spark_qualified(self.db_name, self._table)

    def _ensure_database(self, spark: SparkSession) -> None:
        if self.db_name:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {spark_ident(self.db_name)}")

    def full_load(self, df: DataFrame, spark: SparkSession, **kwargs) -> int:
        try:
            self._ensure_database(spark)
            if self._partition_cols:
                df = df.repartition(*self._partition_cols)
            writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            if self._hive_style and self._partition_cols:
                writer = writer.partitionBy(*self._partition_cols)
            writer.saveAsTable(self.qualified_name)
            return df.count()
        except PipeForgeError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise SinkError(f"full load into {self.qualified_name} failed: {exc}") from exc

    def incremental_load(self, df: DataFrame, spark: SparkSession, **kwargs) -> int:
        """MERGE via Spark SQL, so only the Delta *jars* are needed (no ``delta`` Python package)."""
        if not self._unique_fields:
            raise SinkError("incremental load into Delta needs a unique key")
        view = f"__pipeforge_batch_{uuid.uuid4().hex[:12]}"
        try:
            self._ensure_database(spark)
            df = dedupe(df, self._unique_fields, self._on_duplicate_keys, self._order_by)
            df.createOrReplaceTempView(view)
            cond = " AND ".join(f"old.{spark_ident(k)} = new.{spark_ident(k)}"
                                for k in self._unique_fields)
            if self._schema_evolution:
                spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
            result = spark.sql(
                f"MERGE INTO {self.qualified_name} AS old USING {view} AS new ON {cond} "
                "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *").collect()
            return self._rows_affected(result, df)
        except PipeForgeError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise SinkError(f"merge into {self.qualified_name} failed: {exc}") from exc
        finally:
            spark.catalog.dropTempView(view)
            if self._schema_evolution:
                spark.conf.unset("spark.databricks.delta.schema.autoMerge.enabled")

    @staticmethod
    def _rows_affected(result, df: DataFrame) -> int:
        try:
            row = result[0].asDict()
            return int(row["num_inserted_rows"]) + int(row["num_updated_rows"])
        except Exception:  # noqa: BLE001
            return df.count()
