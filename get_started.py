import os
from dotenv import load_dotenv
import finnhub

load_dotenv()
token = os.getenv("FINNHIB_TOKEN")

# Setup client
finnhub_client = finnhub.Client(api_key=token)

# Stock candles
res = finnhub_client.stock_candles('AAPL', 'D', 1590988249, 1591852249)
print(res)

#Convert to Pandas Dataframe
import pandas as pd
print(pd.DataFrame(res))
