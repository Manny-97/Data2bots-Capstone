import json
import finnhub
import io
import avro.schema
import avro.io
from kafka import KafkaProducer

# https://github.com/Finnhub-Stock-API/finnhub-python

# Setup client
def load_client(token):
    return finnhub.Client(api_key=token)

# Symbol lookup
def lookup_symbol(finnhub_client,ticker):
    return finnhub_client.symbol_lookup(ticker)

# Stock candles
def stock_candles(finnhub_client, symbol, start_date, end_date):
    res = finnhub_client.stock_candles(
        symbol, 'D', start_date, end_date
    )
    return res

# Check if Symbol exists
def symbol_checker(finnhub_client,ticker):
    for stock in lookup_symbol(finnhub_client,ticker)['result']:
        if stock['symbol']==ticker:
            return True
    return False

# Company News
# Need to use _from instead of from to avoid conflict
def company_news(finnhub_client, start_date, end_date):
    return finnhub_client.company_news(
        'AAPL',_from=start_date, to=end_date
    )

# Company Profile
def company_profile(finnhub_client, symbol):
    return finnhub_client.company_profile(symbol=symbol)

# Revenue Estimates
def revenue_estimates(finnhub_client, symbol, frequency):
    return finnhub_client.company_revenue_estimates(
        symbol, freq=frequency
    )

#setting up a Kafka connection
def load_producer(kafka_server):
    return KafkaProducer(
        bootstrap_servers=kafka_server,
        api_version=(0, 10, 1)
    )

#parse Avro schema
def load_avro_schema(schema_content):
    return avro.schema.parse(schema_content)
    
#encode message into avro format
def avro_encode(data, schema):
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(data, encoder)
    return bytes_writer.getvalue()
