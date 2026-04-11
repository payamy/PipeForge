from pyspark.sql import DataFrame
import pyspark.sql.functions as f

from pipeforge.transformer import Transformer


class SampleTransformer(Transformer):
    def transform(self, df: DataFrame) -> DataFrame:
        df = df.withColumn("Email", f.split(f.col("Email"), "@").getItem(0))
        return df
