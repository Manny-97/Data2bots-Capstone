from dotenv import load_dotenv
import os
#https://pypi.org/project/websocket_client/
import websocket

load_dotenv()
token = os.getenv("FINNHIB_TOKEN")

def on_message(ws, message):
    print(message)

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    ws.send('{"type":"subscribe-pr","symbol":"AAPL"}')
    ws.send('{"type":"subscribe-pr","symbol":"AMZN"}')
    ws.send('{"type":"subscribe-pr","symbol":"MSFT"}')
    ws.send('{"type":"subscribe-pr","symbol":"BYND"}')

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={token}",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()