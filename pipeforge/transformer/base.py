from __future__ import annotations

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from functools import wraps

from pipeforge.engine.spark import SparkWrapper


def pandas_transformer(func):
    """Decorator to convert Spark DataFrame arguments to Pandas DataFrames."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        new_args = []
        new_kwargs = {}

        spark = SparkWrapper().spark

        # Convert positional arguments
        for arg in args:
            if isinstance(arg, DataFrame):
                arg = arg.toPandas()
            new_args.append(arg)

        # Convert keyword arguments
        for key, value in kwargs.items():
            if isinstance(value, DataFrame):
                value = value.toPandas()
            new_kwargs[key] = value

        return spark.createDataFrame(func(*new_args, **new_kwargs))

    return wrapper


class BaseTransformer(ABC):
    """
    Base Transformer class. \n
    Each Transformer class should inherit from this class.\n
    """

    _next_handler: BaseTransformer = None

    def set_next(self, handler: BaseTransformer) -> BaseTransformer:
        self._next_handler = handler
        return handler

    def handle(self, df: DataFrame) -> DataFrame:
        """
        This method applies transformation on input DataFrame by calling the transform method. \n
        Then, it will pass the PySpark DataFrame to the next Transformer class in the chain \n
        :param df: PySpark DataFrame
        :return: Transformed PySpark DataFrame
        """
        df = self.transform(df)
        if self._next_handler:
            return self._next_handler.handle(df)

        return df

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass
