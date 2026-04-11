from abc import ABC, abstractmethod
from typing import Tuple
from pyspark.sql import DataFrame, SparkSession


class BaseSink(ABC):
    """
    Base class for all Sinks. \n
    There are two approach to load data to the destination: \n
    1. fully refreshing the destination table \n
    2. Appending the input data to the destination table (or merging with) \n
    """

    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def full_load(self, df: DataFrame, spark: SparkSession, **kwargs) -> Tuple[int, str]:
        pass

    @abstractmethod
    def incremental_load(self, df: DataFrame, spark: SparkSession, **kwargs) -> Tuple[int, str]:
        pass
