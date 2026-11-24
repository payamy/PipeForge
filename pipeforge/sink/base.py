from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession


class BaseSink(ABC):
    """Base class for all sinks.

    Two ways to load data:
    1. ``full_load``: replace the destination with the input
    2. ``incremental_load``: merge (or append) the input into the destination

    Both return the number of rows written and raise
    :class:`pipeforge.errors.SinkError` on failure (they never return error codes).
    """

    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def full_load(self, df: DataFrame, spark: SparkSession, **kwargs) -> int:
        pass

    @abstractmethod
    def incremental_load(self, df: DataFrame, spark: SparkSession, **kwargs) -> int:
        pass
