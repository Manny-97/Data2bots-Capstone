-- Create a database if it does not exist and switch to it
CREATE DATABASE IF NOT EXISTS markets;

-- Switch to the market database
USE DATABASE markets;

-- Create a table 'trades'
CREATE TABLE IF NOT EXISTS trades (
    uuid STRING,
    trade_conditions STRING,
    price DOUBLE,
    symbol STRING,
    trade_timestamp TIMESTAMP,
    ingest_timestamp TIMESTAMP,
    volume DOUBLE,
    type STRING,
    PRIMARY KEY (symbol, trade_timestamp)
);
