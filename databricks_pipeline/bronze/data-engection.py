import json
import pyspark.sql.functions as F
from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType,
    DoubleType, StringType, BooleanType
)


@dp.table(
    name="fraud_dectection.data_ingection.bronze",
    comment="Raw Kafka events ingested from Confluent Cloud"
)


def bronze_ingest() -> DataFrame:
    kafka_connection_json = dbutils.secrets.get(
        scope="TXC_scope",
        key="kafka_connection_details"
    )
    kafka_config = json.loads(kafka_connection_json)

    jaas_config = (
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{kafka_config["api_key"]}" password="{kafka_config["api_secret"]}";'
    )

    streaming_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
        .option("subscribe", kafka_config["topic"])
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("startingOffsets", "earliest")
        .load()
    )

    return streaming_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("bronze_ingestion_timestamp")
    )