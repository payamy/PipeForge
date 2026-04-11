import os

from datetime import timedelta

from pyspark.sql import DataFrame
import pyspark.sql.functions as f

from pipeforge.checkpoint import Checkpoint
from pipeforge.engine.spark import SparkWrapper
from pipeforge.sink.delta import DeltaSink
from pipeforge.connector.postgresql import PostgresConnector
from pipeforge.secret_manager.key_vault import KeyVault
from pipeforge.transformer import Transformer

vault_url = os.getenv('VAULT_URL')
vault_token = os.getenv('VAULT_TOKEN')
username = os.getenv('VAULT_USERNAME')
password = os.getenv('VAULT_PASSWORD')

minio_access_key = os.getenv('MINIO_ACCESS_KEY')
minio_secret_key = os.getenv('MINIO_SECRET_KEY')
minio_endpoint_url = os.getenv('MINIO_ENDPOINT_URL')
minio_checkpoints_bucket_name = "elt-checkpoints"


class RemoveInvalidTimestampsTransformer(Transformer):

    def transform(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "registration_date",
            f.when(
                (f.col("registration_date") < "1900-01-01 00:00:00") | (f.col("registration_date") < "1582-10-15"),
                None
            ).otherwise(f.col("registration_date"))
        )


def main():

    key_vault = KeyVault(vault_url, vault_token, username, password)
    if key_vault.error is not None:
        raise Exception("Failed to create KeyVault")
    secret, err = key_vault.get_kv_secret('user')
    if err is None:
        print(secret)
    else:
        print(err)

    engine: SparkWrapper = SparkWrapper(
        dependencies=[
            "org.postgresql:postgresql:42.7.3",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.761",
        ],
        master="local[3, 100]",
        app_name="replicate_appusers",
        enable_delta=True,
        metastore_uri="thrift://localhost:9083",
        warehouse_location="s3a://my/warehouse"
    )

    engine.set_s3_configurations(
        endpoint=minio_endpoint_url,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
    )

    Checkpoint.configure_s3(
        bucket_name=minio_checkpoints_bucket_name,
        endpoint_url=minio_endpoint_url,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
    )

    sink = DeltaSink(
        db_name="bronze",
        table="AppUsers",
        partition_col="registration_date",
    )

    connector = PostgresConnector(
        engine=engine,
        sink=sink,
        host=secret["user_POSTGRESQL_HOST"],
        port=secret["user_POSTGRESQL_PORT"],
        db=secret["user_POSTGRESQL_DB_NAME"],
        username=secret["user_POSTGRESQL_USERNAME"],
        password=secret["user_POSTGRESQL_PASSWORD"],
        table='"Users"',
        cursor_fields='updated_at',
        columns=[
            'id',
            'email',
            'phone',
            'registration_date',
            'updated_at',
        ],
        transformers=[RemoveInvalidTimestampsTransformer],
    )

    connector.migrate(full_refresh=False, incremental_offset=timedelta(days=1))


if __name__ == '__main__':
    main()
