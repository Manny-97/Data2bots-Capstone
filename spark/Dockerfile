# this image automatically installs the requirements from requirements.txt assuming it is in the root of the project
FROM bde2020/spark-python-template:3.2.1-hadoop3.2

# set the working directory
WORKDIR /app

# copy the source files
COPY . /app

RUN rm -rf /tmp/*

# https://community.snowflake.com/s/article/faq-2023-client-driver-deprecation-for-GCP-customers
CMD ["spark-submit", "--master", "spark://spark-master:7077", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.spark:spark-avro_2.12:3.2.1,net.snowflake:snowflake-jdbc:3.13.25,net.snowflake:spark-snowflake_2.12:2.11.3-spark_3.2", "src/snowflake.py"]
