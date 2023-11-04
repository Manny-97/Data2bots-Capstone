from dotenv import load_dotenv
import os
#https://pypi.org/project/websocket_client/
import websocket
import json
import time

# Define the rate limit (requests per second)
rate_limit = os.getenv("RATE_LIMIT")

load_dotenv()
token = os.getenv("FINNHUB_TOKEN")

class Producer:
    def __init__(self):
        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(
            f"wss://ws.finnhub.io?token={token}",
            on_message = self.on_message,
            on_error = self.on_error,
            on_close = self.on_close
        )
        self.ws.on_open = self.on_open
        try:
            self.ws.run_forever()
        except KeyboardInterrupt:
            self.ws.close()

    def on_message(self, ws, message):
        print(message)

    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws, close_status_code, close_msg):
        print(f"Websocket closed with status code {close_status_code}: {close_msg}")

    def on_open(self, ws):
        while True:
            self.send_messages(ws)
            time.sleep(1 / rate_limit)

    def send_messages(self, ws):
        try:
            ws.send('{"type":"subscribe","symbol":"AAPL"}')
            ws.send('{"type":"subscribe","symbol":"AMZN"}')
            ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')
        except Exception as e:
            print(f"Failed to send data via Websocket: {e}")

if __name__ == "__main__":
    Producer()
