from dotenv import load_dotenv
import os
import websocket
import json
import io
import avro.schema
import avro.io
from kafka import KafkaProducer

load_dotenv()

token = os.getenv("FINNHUB_TOKEN")

# https://finnhub.io/docs/api/websocket-trades
class FinnhubProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=f"{os.getenv('KAFKA_HOST')}:{os.getenv('KAFKA_PORT')}",
            # https://stackoverflow.com/questions/57076780/how-to-determine-api-version-of-kafka
            api_version=(0, 10, 1),
        )

        self.avro_schema = avro.schema.parse(open("schema.avsc").read())
        print("Schema Loaded")

        self.ws = websocket.WebSocketApp(
            f"wss://ws.finnhub.io?token={token}",
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        self.ws.on_open = self.on_open
        self.ws.run_forever()

    def avro_encode(self, data, schema):
        writer = avro.io.DatumWriter(schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(data, encoder)
        return bytes_writer.getvalue()

    def on_message(self, ws, message):
        message = json.loads(message)
        print(message)
        avro_message = self.avro_encode(
            {"data": message["data"], "type": message["type"]}, self.avro_schema
        )
        self.producer.send(os.getenv('KAFKA_TOPIC_NAME'), avro_message)

    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws, close_status_code, close_msg):
        print(f"Websocket closed with status code {close_status_code}: {close_msg}")

    def on_open(self, ws):
        print("sending subscribe message")
        try:
            message = {"type":"subscribe","symbol":os.getenv('FINNHUB_STOCKS_TICKERS')}
            self.ws.send(json.dumps(message))
            print(f"sending message for {os.getenv('FINNHUB_STOCKS_TICKERS')}")
        except Exception as e:
            print(f"Failed to send data via Websocket: {e}")

if __name__ == "__main__":
    FinnhubProducer()
