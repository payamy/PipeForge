import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras as extras

from typing import Tuple, Iterator
from pyspark.sql import DataFrame, Row, SparkSession

from pipeforge.sink.base import BaseSink


class PostgresSink(BaseSink):
    """
    Sink class for loading data to Postgres Database
    """

    def __init__(self, host: str, port: str | int, db: str,
                 username: str, password: str, table: str,
                 schema: str = 'public', unique_field: str | None = '"Id"', **kwargs):
        """
        Constructor method for PostgresConnector.
        :param host: Host of the destination database
        :param port: Port of the destination database
        :param db: DB name
        :param username: Username of the destination database with at least write grant
        :param password: Password of the destination database
        :param table: Name of the destination table
        :param schema: Schema of the destination table
        :param unique_field: Name of the unique field. \
                            When calling incremental_load method, if the unique_field is provided, \
                            the input data will be merged with the destination table. \
                            If not, the input data will be appended to the destination table.
        """
        super().__init__(**kwargs)
        self._host = host
        self._port = port
        self._db = db
        self._username = username
        self._password = password
        self._table = table
        self._schema = schema

        self._unique_field = unique_field

    def full_load(self, df: DataFrame, spark: SparkSession, **kwargs) -> Tuple[int, str]:
        try:
            # Overwriting the destination table with the input data
            df.write \
                .format("jdbc") \
                .mode("overwrite") \
                .option("url", f"jdbc:postgresql://{self._host}:{self._port}/{self._db}") \
                .option("dbtable", f"{self._schema}.{self._table}") \
                .option("truncate", True) \
                .option("user", self._username) \
                .option("password", self._password) \
                .option("driver", "org.postgresql.Driver") \
                .save()
            # Return 0 if the operation is successful
            return 0, ""
        except Exception as e:
            # Return 1 and the error message if there is any kind of failure in the operation
            return 1, str(e)

    def incremental_load(self, df: DataFrame, spark: SparkSession, **kwargs) -> Tuple[int, str]:
        try:
            # Calculating query string for loading each partition to the destination
            columns = [f'"{col}"' for col in df.columns]
            columns_string = f"({','.join(columns)})"
            update_clause = f'''ON CONFLICT ({self._unique_field})
                                DO UPDATE SET {','.join([f'"{col}"=excluded."{col}"' for col in df.columns 
                                                         if col != self._unique_field])}''' \
                                    if self._unique_field else ''
            query_str = f'''INSERT INTO {self._schema}.{self._table} {columns_string}
                            VALUES %s 
                            {update_clause}'''

            # Defining psycopg2 connection to Postgres server
            conn = psycopg2.connect(host=self._host, port=self._port, dbname=self._db,
                                    user=self._username, password=self._password)
            cursor = conn.cursor()

            # Converting PySpark DataFrame to Pandas DataFrame and replacing NaN/NaT values with None
            p_df: pd.DataFrame = df.toPandas().replace({np.nan: None})
            tuples = [tuple(x) for x in p_df.to_numpy()]

            # Upsert data to destination
            extras.execute_values(cursor, query_str, tuples)
            # Committing changes
            conn.commit()

            cursor.close()
            conn.close()
            return 0, ""
        except Exception as e:
            # Return 1 and the error message if there is any kind of failure in the operation
            return 1, str(e)

    @staticmethod
    def __upsert_chunk(itr: Iterator[Row], host, port, db, user, password, query_str):

        conn = psycopg2.connect(host=str(host.value), port=str(port.value), dbname=str(db.value),
                                user=str(user.value), password=str(password.value))
        cursor = conn.cursor()

        for row in itr:
            # converting PySpark Row to Numpy array
            row_arr = np.array(row)
            extras.execute_values(cursor, str(query_str.value), [row_arr])

        conn.commit()
        cursor.close()
        conn.close()
