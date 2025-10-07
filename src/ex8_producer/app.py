import base64
import csv
import logging
import time
from io import BytesIO, StringIO
from typing import Any, Dict, Iterator, List, Tuple

import boto3
from botocore.exceptions import ClientError
from avro import schema
from avro.io import DatumWriter, BinaryEncoder
from kafka import KafkaProducer

from ex8_producer.settings import *
from ex8_producer.utils.functions import *

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class Producer:
    """
    A class to produce messages from S3 CSV files to a Kafka topic.
    """

    def __init__(self, bucket_name: str, kafka_bootstrap_servers: str, kafka_topic: str, s3_path_prefix: str):
        """
        Initializes the Producer.
        """
        self.bucket_name = bucket_name
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.s3_path_prefix = s3_path_prefix
        self.s3_client = self._get_aws_clients()
        self.kafka_producer = self._get_kafka_producer()
        self.parsed_schema = self._get_parsed_schema()


    @staticmethod
    def _get_aws_clients() -> boto3.client:
        """Initializes and returns Boto3 S3 client."""
        if not all([AWS_DEFAULT_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY]):
            raise ValueError(
                "AWS credentials and region must be set in .env file or environment."
            )

        client_kwargs = {
            "region_name": AWS_DEFAULT_REGION,
            "aws_access_key_id": AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
            "endpoint_url": AWS_ENDPOINT_URL
        }

        s3_client = boto3.client("s3", **client_kwargs)
        return s3_client

    def _get_kafka_producer(self) -> KafkaProducer:
        """Initializes and returns a Kafka producer."""
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda v: v
            )
            return producer
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise


    @staticmethod
    def _get_parsed_schema() -> Dict[str, Any]:
        """Builds and parses the Avro schema."""
        try:
            with open(SCHEMA_PATH, "r") as f:
                parsed_schema = schema.parse(f.read()) 
            return parsed_schema
        except FileNotFoundError:
            logger.error(f"Schema file not found at: {SCHEMA_PATH}")
            raise
        except Exception as e:
            logger.error(f"Failed to parse schema: {e}")
            raise


    def list_csv_files(self) -> List[str]:
        """Lists all CSV files in the S3 bucket under the given prefix."""
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(
                Bucket=self.bucket_name, Prefix=self.s3_path_prefix
            )
            csv_files = [
                obj["Key"]
                for page in pages
                for obj in page.get("Contents", [])
                if obj["Key"].endswith(".csv")
            ]
            if not csv_files:
                logger.info(
                    f"No CSV files found for prefix '{self.s3_path_prefix}' in bucket '{self.bucket_name}'"
                )
            return csv_files
        except ClientError as e:
            logger.error(
                f"Failed to list files in bucket {self.bucket_name} with prefix {self.s3_path_prefix}: {e}"
            )
            return []


    def _process_csv_rows(self, object_key: str) -> Iterator[Dict[str, Any]]:
        """
        Downloads a CSV from S3 and yields processed rows as dictionaries.
        """
        logger.info(f"Attempting to download s3://{self.bucket_name}/{object_key}")
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name, Key=object_key
            )
            content = response["Body"].read().decode("iso-8859-1")
            csv_file = StringIO(content)

            reader = csv.DictReader(csv_file, delimiter=";")

            for row in reader:
                row_data = {}
                for field, value in row.items():
                    sanitized_field = sinitize_text(field).lower()
                    if sanitized_field in SCHEMA:
                        row_data[sanitized_field] = value if value else None
                yield row_data

        except self.s3_client.exceptions.NoSuchKey:
            logger.error(f"File not found: s3://{self.bucket_name}/{object_key}")
        except Exception as e:
            logger.error(
                f"An unexpected error occurred while processing {object_key}: {e}"
            )


    def _send_chunk_to_kafka(self, chunk: List[Dict[str, Any]]):
        """Sends a chunk of rows as a single message to Kafka."""
        try:
            writer = DatumWriter(self.parsed_schema)

            for record in chunk:
                buffer = BytesIO()
                encoder = BinaryEncoder(buffer)
                writer.write(record, encoder)

                avro_binary_data = buffer.getvalue()
                self.kafka_producer.send(self.kafka_topic, avro_binary_data)

            self.kafka_producer.flush()
            logger.info(f"Sent {len(chunk)} messages to Kafka topic '{self.kafka_topic}'.")
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {e}")


    def process_file(self, object_key: str):
        """
        Processes a single CSV file and sends its data to Kafka in chunks.
        """
        chunk = []
        for row_data in self._process_csv_rows(object_key):
            chunk.append(row_data)
            if len(chunk) >= CHUNK_SIZE:
                self._send_chunk_to_kafka(chunk)
                chunk = []
                time.sleep(RETRY_DELAY)
        if chunk:
            self._send_chunk_to_kafka(chunk)


    def run(self):
        """Main loop to run the producer."""
        while True:
            logger.info(
                f"Checking for CSV files in s3://{self.bucket_name}/{self.s3_path_prefix}"
            )
            csv_files = self.list_csv_files()
            if not csv_files:
                logger.warning(
                    f"No CSV files found in s3://{self.bucket_name}/{self.s3_path_prefix}. Waiting..."
                )
                time.sleep(PROCESS_INTERVAL)
                continue

            for object_key in csv_files:
                self.process_file(object_key)

            logger.info(
                f"Finished processing all CSV files. Waiting for {PROCESS_INTERVAL} seconds before checking again."
            )
            time.sleep(PROCESS_INTERVAL)


def main():
    """Main function to run the producer."""
    try:
        if not all([S3_BUCKET_NAME, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC]):
            raise ValueError("S3_BUCKET_NAME, KAFKA_BOOTSTRAP_SERVERS and KAFKA_TOPIC must be set.")

        producer = Producer(S3_BUCKET_NAME, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, S3_PATH_PREFIX)
        producer.run()

    except ValueError as e:
        logger.error(f"Configuration error: {e}")
    except Exception as e:
        logger.error(f"A critical error occurred in main: {e}")


if __name__ == "__main__":
    main()