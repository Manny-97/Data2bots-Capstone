import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import (
    StringType, 
    LongType, 
    DoubleType, 
    ArrayType, 
)
from pyspark.sql.streaming import *
from pyspark.sql.avro.functions import from_avro
from uuid import uuid1
from dotenv import load_dotenv

load_dotenv()

# Create a Spark session
spark = SparkSession \
    .builder \
    .master("spark://spark-master:7077") \
    .appName("StreamProcessor") \
    .getOrCreate()

# Set log level to DEBUG
spark.sparkContext.setLogLevel("DEBUG")

# Define the Snowflake connection properties
snowflake_options = {
    "sfURL" : os.getenv('SNOWFLAKE_URL'),
    "sfAccount": os.getenv('SNOWFLAKE_ACCOUNT'),
    "sfSchema" : os.getenv('SNOWFLAKE_SCHEMA'),
    "sfWarehouse" : os.getenv('SNOWFLAKE_WAREHOUSE'),
    "sfRole" : os.getenv('SNOWFLAKE_ROLE'),
    "sfDatabase" : os.getenv('SNOWFLAKE_DATABASE'),
    "sfUser" : os.getenv('SNOWFLAKE_USER'),
    "sfPassword" : os.getenv('SNOWFLAKE_PASSWORD'),
}

@udf(returnType=StringType())
def makeUUID():
    return str(uuid1())

# define the stream to read from the Kafka topic market
inputDF = spark \
    .readStream \
    .format("kafka") \
    .options(**snowflake_options) \
    .option("kafka.bootstrap.servers", f"{os.getenv('KAFKA_HOST')}:{os.getenv('KAFKA_PORT')}") \
    .option("subscribe", os.getenv('KAFKA_TOPIC_NAME')) \
    .option("minPartitions", "1") \
    .option("maxOffsetsPerTrigger", "1000") \
    .load()

# define the Avro schema that corresponds to the encoded data
tradesSchema = open('src/schemas.avsc', 'r').read()

# explode the data column and select the columns we need
expandedDF = inputDF \
    .withColumn("avroData", from_avro(col("value"), tradesSchema)) \
    .select(col("avroData.*")) \
    .select(explode(col("data")), col("type")) \
    .select(col("col.*"), col("type"))

# Cast each column individually
expandedDF = expandedDF \
    .withColumn("c", col("c").cast(ArrayType(StringType()))) \
    .withColumn("p", col("p").cast(DoubleType())) \
    .withColumn("s", col("s").cast(StringType())) \
    .withColumn("t", col("t").cast(LongType())) \
    .withColumn("v", col("v").cast(DoubleType())) \
    .withColumn("type", col("type").cast(StringType()))

# create the final dataframe with the columns we need plus the ingest timestamp
finalDF = expandedDF \
    .withColumn("uuid", makeUUID()) \
    .withColumnRenamed("c", "trade_conditions") \
    .withColumnRenamed("p", "price") \
    .withColumnRenamed("s", "symbol") \
    .withColumnRenamed("t", "trade_timestamp") \
    .withColumnRenamed("v", "volume") \
    .withColumn("trade_timestamp", (col("trade_timestamp") / 1000).cast("timestamp")) \
    .withColumn("ingest_timestamp", current_timestamp().alias("ingest_timestamp")) \
    .select("uuid", "trade_conditions", "price", "symbol", "trade_timestamp", "ingest_timestamp", "volume", "type")

# write the final dataframe to Snowflake
# spark handles the streaming and batching for us
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
query = finalDF \
    .writeStream \
    .trigger(processingTime="5 seconds") \
    .foreachBatch(lambda batchDF, batchId: \
        batchDF.write \
            .format(SNOWFLAKE_SOURCE_NAME) \
            .options(**snowflake_options) \
            .option("dbtable", os.getenv('SNOWFLAKE_TABLE')) \
            .mode("append") \
            .save()) \
    .outputMode("update") \
    .start()

# Wait for the stream to terminate (i.e., wait forever)
query.awaitTermination()
