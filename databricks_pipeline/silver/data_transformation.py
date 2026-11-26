from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import json
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import *

@dp.table(
    name="silver",
    comment="Parsed, cleaned, and feature-enriched transactions"
)
@dp.expect_or_drop("valid_id", "id IS NOT NULL")
def transactions_silver() -> DataFrame:
    bronze_df = spark.readStream.table("bronze")

    inner_schema = StructType([
        StructField("id", LongType(), True),
        StructField("amount_usd", DoubleType(), True),
        StructField("hours_since_last_txn", DoubleType(), True),
        StructField("txn_count_last_24h", IntegerType(), True),
        StructField("distance_from_home_km", DoubleType(), True),
        StructField("card_age_months", IntegerType(), True),
        StructField("customer_age", IntegerType(), True),
        StructField("account_balance_usd", DoubleType(), True),
        StructField("cvv_retry_count", IntegerType(), True),
        StructField("velocity_score", DoubleType(), True),
        StructField("time_of_day_hour", IntegerType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("merchant_risk_score", DoubleType(), True),
        StructField("prior_disputes", IntegerType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("card_type", StringType(), True),
        StructField("auth_method", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("is_foreign_transaction", BooleanType(), True),
        StructField("is_new_merchant", BooleanType(), True),
        StructField("used_vpn", BooleanType(), True),
        StructField("ip_country_mismatch", BooleanType(), True),
        StructField("billing_shipping_mismatch", BooleanType(), True),
        StructField("is_ai_generated_scam_attempt", BooleanType(), True),
        StructField("event_timestamp", DoubleType(), True)
    ])

   
    wrapper_schema = StructType([
        StructField("id", StringType(), True),
        StructField("data", inner_schema, True)
    ])

    return (
        bronze_df
        .withColumn("parsed", F.from_json(F.col("raw_value"), wrapper_schema))
        .select(
            F.col("parsed.id").alias("event_uuid"),
            F.col("parsed.data.*"),
            F.col("kafka_topic"),
            F.col("kafka_partition"),
            F.col("kafka_offset"),
            F.col("kafka_timestamp"),
            F.col("bronze_ingestion_timestamp"),
            F.current_timestamp().alias("silver_ingestion_timestamp")
        )
        .withColumn("event_ts", F.to_timestamp(F.from_unixtime(F.col("event_timestamp"))))
    )