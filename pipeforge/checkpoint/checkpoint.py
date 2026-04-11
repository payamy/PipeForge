import os
from pathlib import Path
from datetime import datetime
import boto3
import botocore

class Checkpoint(object):
    """
    Static class for setting datetime checkpoints.
    """

    checkpoint_dir = Path(__file__).parent.parent.parent.joinpath("pipeforge_checkpoint")
    use_s3 = False  # Default is False, use local filesystem

    # S3 configuration variables (initially None)
    s3_bucket_name = None
    s3_endpoint_url = None
    s3_access_key = None
    s3_secret_key = None
    s3_client = None

    @classmethod
    def configure_s3(cls, bucket_name: str, endpoint_url: str, access_key: str, secret_key: str):
        """
        Configure S3 settings for the checkpoint.
        :param bucket_name: S3 bucket name
        :param endpoint_url: S3 endpoint URL
        :param access_key: S3 access key
        :param secret_key: S3 secret key
        :return: None
        """
        cls.use_s3 = True
        cls.s3_bucket_name = bucket_name
        cls.s3_endpoint_url = endpoint_url
        cls.s3_access_key = access_key
        cls.s3_secret_key = secret_key

        cls.s3_client = boto3.client(
            's3',
            endpoint_url=cls.s3_endpoint_url,
            aws_access_key_id=cls.s3_access_key,
            aws_secret_access_key=cls.s3_secret_key,
            config=botocore.client.Config(signature_version='s3v4')
        )

        bucket_loc = cls.s3_client.get_bucket_location(Bucket=bucket_name)
        if bucket_loc is None:
            cls.s3_client.create_bucket(Bucket=bucket_name)

    @classmethod
    def get_checkpoint(cls, db: str, table: str, schema: str = 'main') -> datetime | None:
        """
        Reading checkpoints from disk or S3.
        :param db: Database name
        :param table: Table name
        :param schema: Schema title
        :return: Returns datetime object or None if not found
        """
        filename = f"{db}-{schema}-{table}.txt"

        if cls.use_s3:
            if cls.s3_client is None:
                raise ValueError("S3 is not configured. Call 'configure_s3' first.")
            try:
                response = cls.s3_client.get_object(Bucket=cls.s3_bucket_name,
                                                    Key=cls.find_object_key_by_filename(s3_client=cls.s3_client,
                                                                                        bucket_name=cls.s3_bucket_name,
                                                                                        file_name=filename))
                content = response['Body'].read().decode('utf-8')
                return datetime.strptime(content, "%Y-%m-%d %H:%M:%S")
            except cls.s3_client.exceptions.NoSuchKey:
                return None
            except Exception as e:
                print(f"Error reading from S3: {e}")
                return None
        else:
            try:
                with open(cls.checkpoint_dir.joinpath(filename), 'r') as f:
                    return datetime.strptime(f.read(), "%Y-%m-%d %H:%M:%S")
            except FileNotFoundError:
                return None

    @staticmethod
    def find_object_key_by_filename(s3_client, bucket_name, file_name):
        # List all objects in the bucket
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name):
            if "Contents" in page:
                for obj in page["Contents"]:
                    if obj["Key"] == file_name:  # Check if the file name matches
                        return obj["Key"]
        return None

    @classmethod
    def set_checkpoint(cls, checkpoint_datetime: datetime, db: str, table: str, schema: str = 'main'):
        """
        Writing checkpoints to disk or S3.
        :param checkpoint_datetime: DateTime value
        :param db: Database name
        :param table: Table name
        :param schema: Schema title
        :return: None
        """
        filename = f"{db}-{schema}-{table}.txt"
        content = checkpoint_datetime.strftime("%Y-%m-%d %H:%M:%S")

        if cls.use_s3:
            if cls.s3_client is None:
                raise ValueError("S3 is not configured. Call 'configure_s3' first.")
            try:
                cls.s3_client.put_object(Bucket=cls.s3_bucket_name, Key=filename, Body=content)
            except Exception as e:
                print(f"Error writing to S3: {e}")
        else:
            if not os.path.exists(cls.checkpoint_dir):
                os.makedirs(cls.checkpoint_dir)
            with open(cls.checkpoint_dir.joinpath(filename), 'w') as f:
                f.write(content)