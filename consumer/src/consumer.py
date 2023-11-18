# sytem import
import os

# library imports
from kafka import KafkaConsumer
import avro.schema
from dotenv import load_dotenv

# dependency import
from utility.functions import (
    read_message,
)

load_dotenv()


# Define the consumer to read from the Kafka topic
consumer = KafkaConsumer(
    os.getenv('KAFKA_TOPIC_NAME'),
    bootstrap_servers=f"{os.getenv('KAFKA_HOST')}:{os.getenv('KAFKA_PORT')}",
    # https://stackoverflow.com/questions/57076780/how-to-determine-api-version-of-kafka
    api_version=(0, 10, 1)
)


# Define the schema that corresponds to the encoded data
schema = avro.schema.parse(open('schema.avsc').read())

for message in consumer:
    read_message(message, schema)
