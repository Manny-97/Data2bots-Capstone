# system import
import io
# library import
import avro.io
import avro.schema
from kafka import KafkaConsumer


def load_consumer(topic_name, kafka_server):
    return KafkaConsumer(
        topic_name,
        bootstrap_servers=kafka_server,
        api_version=(0, 10, 1)
    )

def read_message(message, schema):
    bytes_reader = io.BytesIO(message.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    data = reader.read(decoder)
    print(data)
