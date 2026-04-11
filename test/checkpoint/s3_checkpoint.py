import unittest
import boto3
import botocore
import os

from botocore.client import ClientError
from datetime import datetime

from pipeforge.checkpoint import Checkpoint


class S3CheckpointTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Please note that you have to start an instance of MinIO with the following parameters: \n
        endpoint_url: http://localhost:9000 \n
        access_key: minioadmin \n
        secret_key: minioadmin \n
        """
        cls.s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            config=botocore.client.Config(signature_version='s3v4')
        )

        cls.bucket_name = 'elt-checkpoints'

        cls.vault_url = os.getenv("VAULT_URL")
        cls.vault_token = os.getenv("VAULT_TOKEN")
        cls.vault_username = os.getenv("VAULT_USERNAME")
        cls.vault_password = os.getenv("VAULT_PASSWORD")

        try:
            cls.s3_client.delete_bucket(Bucket=cls.bucket_name)
        except ClientError:
            print("The bucket does not exist. skipping deletion.")

        Checkpoint.configure_s3(bucket_name=cls.bucket_name, endpoint_url='http://localhost:9000',
                                access_key='minioadmin', secret_key='minioadmin')

    @classmethod
    def tearDownClass(cls):
        cls.vault_url = ""
        cls.vault_token = ""
        cls.vault_username = ""
        cls.vault_password = ""

    def test_s3_bucket_creation(self):
        checkpoint_datetime = datetime.now()
        try:
            Checkpoint.set_checkpoint(checkpoint_datetime, db='mydb', table='mytable')
        except Exception as e:
            raise Exception(f"error writing checkpoint: {e}")

        response = self.s3_client.get_object(Bucket=self.bucket_name, Key=f"mydb-main-mytable.txt")
        content = response['Body'].read().decode('utf-8')
        self.assertEqual(content, checkpoint_datetime.strftime("%Y-%m-%d %H:%M:%S"))
