import unittest

import pandas as pd
from pyspark.sql import SparkSession, DataFrame

from pipeforge.transformer import Transformer, pandas_transformer


class CustomTransformer(Transformer):

    @pandas_transformer
    def transform(self, p_df: pd.DataFrame) -> pd.DataFrame:
        p_df["AgeAddedByTwo"] = p_df["Age"] + 2
        return p_df


class PandasTransformerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_pandas_transformer_decorator(self):
        df = self.spark.createDataFrame([('Alice', 1)], ['Name', 'Age'])

        transformer = CustomTransformer()
        processed_df = transformer.transform(df)
        self.assertEqual(type(processed_df), DataFrame)
        processed_df: DataFrame
        self.assertEqual(processed_df.collect()[0]['AgeAddedByTwo'], 3)
