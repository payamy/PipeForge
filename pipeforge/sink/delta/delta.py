from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from delta import DeltaTable

from pipeforge.sink.base import BaseSink


class DeltaSink(BaseSink):

    def __init__(self, table: str, partition_col: str, unique_field: str | None = 'Id',
                 hive_style: bool = False, db_name: str = None, **kwargs):
        """
        Constructor method for DeltaSink.
        :param bucket: Bucket name of Delta table
        :param table: Name of the destination table
        """
        super().__init__(**kwargs)
        self._table = table
        self._partition_col = partition_col
        self._hive_style = hive_style
        self.db_name = db_name

        self._unique_field = unique_field

    def full_load(self, df: DataFrame, spark: SparkSession, **kwargs) -> Tuple[int, str]:
        try:
            if self._hive_style:
                (df
                 .repartition(self._partition_col)
                 .write.format("delta")
                 .mode("overwrite")
                 .option("overwriteSchema", "true")
                 .partitionBy(self._partition_col)
                 .saveAsTable(self.db_name + '.' + self._table if self.db_name else self._table))
            else:
                (df
                 .repartition(self._partition_col)
                 .write.format("delta")
                 .mode("overwrite")
                 .option("overwriteSchema", "true")
                 .saveAsTable(self.db_name + '.' + self._table if self.db_name else self._table))
            return 0, ""
        except Exception as e:
            # Return 1 and the error message if there is any kind of failure in the operation
            return 1, str(e)

    def incremental_load(self, df: DataFrame, spark: SparkSession, **kwargs) -> Tuple[int, str]:
        try:
            delta_table = DeltaTable.forName(spark, self.db_name + '.' + self._table if self.db_name else self._table)

            delta_table.alias('old') \
                .merge(
                df.alias('new'),
                f'old.{self._unique_field} = new.{self._unique_field}'
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            return 0, ""
        except Exception as e:
            # Return 1 and the error message if there is any kind of failure in the operation
            return 1, str(e)
