import os

from pipeforge.engine.spark import SparkWrapper
from pipeforge.sink.postgresql import PostgresSink
from pipeforge.connector.postgresql import PostgresConnector


def main():

    engine: SparkWrapper = SparkWrapper(
        dependencies=["org.postgresql:postgresql:42.7.3"],
        master="local[*]",
        app_name="replicate_pg_to_pg",
    )

    sink = PostgresSink(
        host="localhost",
        port=5432,
        db="source",
        username="postgres",
        password="postgres_pass",
        table='"Users"',
    )

    connector = PostgresConnector(
        engine=engine,
        sink=sink,
        host="localhost",
        port="5432",
        db="sink",
        username="postgres",
        password="postgres_pass",
        table='"Users"',
        cursor_fields='updated_at',
        columns=[
            'id',
            'email',
            'phone',
            'registration_date',
            'updated_at',
        ]
    )

    connector.migrate(full_refresh=False)


if __name__ == '__main__':
    main()
