from abc import ABC, abstractmethod
from typing import List

from pyspark.sql import DataFrame

from pipeforge.engine.spark import SparkWrapper
from pipeforge.sink import Sink
from pipeforge.transformer import Transformer


class BaseConnector(ABC):
    """Base class for connectors."""

    def __init__(self, engine: SparkWrapper, sink: Sink, cursor_fields: str | List[str],
                 transformers: List[type] | None = None, **kwargs):
        self._engine: SparkWrapper = engine
        self._sink: Sink = sink
        self._cursor_fields = cursor_fields
        self._transformers = list(transformers) if transformers else []

    @property
    def engine(self) -> SparkWrapper:
        return self._engine

    @property
    def sink(self):
        return self._sink

    @abstractmethod
    def migrate(self, full_refresh: bool = False, **kwargs):
        pass

    @abstractmethod
    def validate_connection(self) -> None:
        """Raise if the source cannot be reached or is misconfigured."""

    def _handle_transforms(self, df: DataFrame) -> DataFrame:
        """Apply the transformer chain. Does not mutate ``self._transformers``.

        Each transformer class is instantiated freshly, so repeated calls give
        identical results.
        """
        if not self._transformers:
            return df
        objs: List[Transformer] = [cls() for cls in self._transformers]
        for current, nxt in zip(objs, objs[1:]):
            current.set_next(nxt)
        return objs[0].handle(df)
