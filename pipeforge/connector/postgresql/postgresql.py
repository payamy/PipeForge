from datetime import timedelta, datetime
from typing import List

from pipeforge.connector.base import BaseConnector
from pipeforge.checkpoint import Checkpoint
from pipeforge.engine.spark import SparkWrapper
from pipeforge.sink import Sink


class PostgresConnector(BaseConnector):
    """
    Connector class for fetching data from Postgres tables and loading them into provided Sinks
    """

    def __init__(self, engine: SparkWrapper, sink: Sink, cursor_fields: str | List[str],
                 host: str, port: str | int, db: str,
                 username: str, password: str, table: str,
                 columns: List[str] = None, schema: str = 'public',
                 transformers: List[type] = None, **kwargs):
        """
        Constructor method for PostgresConnector.
        :param engine: SparkWrapper instance
        :param sink: Sink instance for loading data to the destination
        :param cursor_fields: Column names
        :param host: Host of the Postgres instance
        :param port: Port of the Postgres instance
        :param db: Database name
        :param username: Username of the Postgres instance with read grant at least
        :param password: Password of the Postgres instance
        :param table: Name of the Postgres table to import its data
        :param columns: List of columns to import the data. If not provided, all columns will be imported
        :param schema: Name of the schema which stores the table
        :param transformers: List of transformers to apply to the data
        """
        super().__init__(engine=engine, sink=sink, cursor_fields=cursor_fields,
                         transformers=transformers, **kwargs)
        self._host = host
        self._port = port
        self._db = db
        self._username = username
        self._password = password
        self._table = table
        self._schema = schema

        self._columns = columns

    def migrate(self, full_refresh: bool = False, full_refresh_partition_field: str = '"Id"',
                incremental_offset: timedelta = timedelta(days=0, hours=0, minutes=0, seconds=0), ):
        """
        This method is provided for replicating data between source and destination tables.
        :param full_refresh: The flag which indicates how the data should be fetched from the database \
                                fully or incrementally (based on checkpoints)
        :param full_refresh_partition_field: This field will be used for partitioning data when data is going to \
                                fully fetched. An integer field has to be selected field for that matter
        :param incremental_offset: A date time offset could be set when data is going to fetch incrementally
        :return:
        """
        if full_refresh or not Checkpoint.get_checkpoint(self._db, self._table, self._schema):
            self.__full_refresh(full_refresh_partition_field=full_refresh_partition_field)
        else:
            self.__incremental(incremental_offset=incremental_offset)

    def __full_refresh(self, full_refresh_partition_field: str = '"Id"'):
        try:
            # Getting minimum and maximum of partition fields in order to calculate number of partitions
            lower_bound, upper_bound = self.engine.spark.read.format("jdbc") \
                .option("url", f"jdbc:postgresql://{self._host}:{self._port}/{self._db}") \
                .option("dbtable", f'''(SELECT 
                                                       MIN({full_refresh_partition_field}), 
                                                       MAX({full_refresh_partition_field}) 
                                                   FROM {self._schema}.{self._table} ) AS query_table''') \
                .option("user", self._username) \
                .option("password", self._password)  \
                .option("driver", "org.postgresql.Driver") \
                .load() \
                .collect()[0]
        except Exception as e:
            raise RuntimeError(f"Error getting lower bound and upper bound: {e}")

        # Calculating number of partitions based on a fixed partition size of 10000
        partition_size = 10000
        num_partitions = (upper_bound - lower_bound) // partition_size

        # Calculating checkpoint datetime
        checkpoint_datetime = datetime.now()

        try:
            # Fetching data from source and storing it in a PySpark DataFrame
            df = self.engine.spark.read.format("jdbc") \
                .option("url", f"jdbc:postgresql://{self._host}:{self._port}/{self._db}") \
                .option("dbtable", f'''(SELECT {",".join(self._columns) if self._columns else "*"}
                                                   FROM {self._schema}.{self._table}) AS query_table''') \
                .option("user", self._username) \
                .option("password", self._password) \
                .option("driver", "org.postgresql.Driver") \
                .option("partitionColumn", full_refresh_partition_field) \
                .option("lowerBound", lower_bound) \
                .option("upperBound", upper_bound) \
                .option("numPartitions", num_partitions) \
                .load()
        except Exception as e:
            raise RuntimeError(f"Error fetching data from postgres data source: {e}")

        try:
            # Applying transformations on data
            df = self._handle_transforms(df)
        except Exception as e:
            raise RuntimeError(f"Error processing data with transformer classes: {e}")

        # Load the data on destination table (and refreshing it in case it has data)
        code, message = self.sink.full_load(df, self.engine.spark)
        if code == 0:
            # Write the datetime checkpoint if loading data is successful
            Checkpoint.set_checkpoint(checkpoint_datetime, self._db, self._table, self._schema)
        else:
            raise RuntimeError(message)

    def __incremental(self, incremental_offset: timedelta = timedelta(days=0, hours=0, minutes=0, seconds=0)):

        # Fetching previous datetime checkpoint
        checkpoint_datetime = Checkpoint.get_checkpoint(self._db, self._table, self._schema) - incremental_offset

        # Appending condition clauses with OR statement
        # Assume cursor fields are "CreatedAt" and "UpdatedAt" and previous checkpoint is 2024-01-01 00:00:00
        # incremental_condition => `"CreatedAt" >= '2024-01-01 00:00:00' OR "UpdatedAt" >= '2024-01-01 00:00:00'
        cursor_fields = self._cursor_fields if isinstance(self._cursor_fields, list) else [self._cursor_fields]
        incremental_condition = " OR ".join([f'''{col} >= '{checkpoint_datetime}' ''' for col in cursor_fields])

        try:
            # Calculating number of records which is going to be fetched
            num_records = self.engine.spark.read.format("jdbc") \
                .option("url", f"jdbc:postgresql://{self._host}:{self._port}/{self._db}") \
                .option("dbtable", f'''(SELECT 
                                                       COUNT(*)
                                                   FROM {self._schema}.{self._table}
                                                   WHERE {incremental_condition}) AS query_table''') \
                .option("user", self._username) \
                .option("password", self._password) \
                .option("driver", "org.postgresql.Driver") \
                .load() \
                .collect()[0][0]
        except Exception as e:
            raise RuntimeError(f"Error getting records count: {e}")

        # Calculating number of partitions based on a fixed partition size of 10000
        partition_size = 10000
        num_partitions = num_records // partition_size

        # Calculating checkpoint datetime
        new_checkpoint_datetime = datetime.now()

        try:
            # Fetching data from source and storing it in a PySpark DataFrame
            df = self.engine.spark.read.format("jdbc") \
                .option("url", f"jdbc:postgresql://{self._host}:{self._port}/{self._db}") \
                .option("dbtable", f'''(SELECT {",".join(self._columns) if self._columns else "*"}
                                                   FROM {self._schema}.{self._table}
                                                   WHERE {incremental_condition}) AS query_table''') \
                .option("user", self._username) \
                .option("password", self._password) \
                .option("driver", "org.postgresql.Driver") \
                .option("partitionColumn", cursor_fields[0]) \
                .option("lowerBound", checkpoint_datetime.strftime("%Y-%m-%d %H:%M:%S")) \
                .option("upperBound", new_checkpoint_datetime.strftime("%Y-%m-%d %H:%M:%S")) \
                .option("numPartitions", num_partitions) \
                .load()
        except Exception as e:
            raise RuntimeError(f"Error fetching data from postgres data source: {e}")

        try:
            # Applying transformations on data
            df = self._handle_transforms(df)
        except Exception as e:
            raise RuntimeError(f"Error processing data with transformer classes: {e}")

        # Merge data with the destination table
        code, message = self.sink.incremental_load(df, self.engine.spark)
        if code == 0:
            # Write the datetime checkpoint if loading data is successful
            Checkpoint.set_checkpoint(new_checkpoint_datetime, self._db, self._table, self._schema)
        else:
            raise RuntimeError(message)
