from abc import ABC, abstractmethod
from typing import List
from pyspark.sql import DataFrame

from pipeforge.engine.spark import SparkWrapper
from pipeforge.transformer import Transformer
from pipeforge.sink import Sink


class BaseConnector(ABC):
    """
    Base class for connectors.
    """

    def __init__(self, engine: SparkWrapper, sink: Sink, cursor_fields: str | List[str],
                 transformers: List[type] = None, **kwargs):
        self._engine: SparkWrapper = engine
        self._sink: Sink = sink
        self._cursor_fields = cursor_fields
        self._transformers = transformers


    @property
    def engine(self) -> SparkWrapper:
        return self._engine

    @property
    def sink(self):
        return self._sink

    @abstractmethod
    def migrate(self, full_refresh: bool = False, executors: int = 4):
        pass

    def _handle_transforms(self, df: DataFrame) -> DataFrame:
        """
        Protected method (only for the use of child classes) to apply DataFrame transformations on data \n
        This method benefits from Chain of Responsibilities design pattern. Each Transformer class in chain \
        apply its own transformation on data and pass it to the next Transformer class in chain \
        through its handle method
        :param df: PySpark DataFrame which contains data from a specific data source
        :return: Transformed PySpark DataFrame
        """
        if not self._transformers:
            return df
        _class: type = self._transformers.pop(0)
        first_transformer_obj: Transformer = _class()
        transformer_obj = first_transformer_obj
        for _class in self._transformers:
            _class: type
            next_transformer_obj: Transformer = _class()
            transformer_obj = transformer_obj.set_next(next_transformer_obj)

        return first_transformer_obj.handle(df)
