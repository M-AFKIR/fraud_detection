import mlflow
import pyspark.sql.functions as F
import json
from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame

mlflow.set_registry_uri("databricks-uc")



kafka_connection_json = dbutils.secrets.get(
        scope="TXC_scope",
        key="kafka_connection_details"
    )
kafka_config = json.loads(kafka_connection_json)

FEATURE_COLS = [
    "amount_usd", "hours_since_last_txn", "txn_count_last_24h", "distance_from_home_km",
    "card_age_months", "customer_age", "account_balance_usd", "cvv_retry_count",
    "velocity_score", "time_of_day_hour", "day_of_week", "merchant_risk_score",
    "prior_disputes", "merchant_category", "card_type", "auth_method", "channel",
    "device_type", "is_foreign_transaction", "is_new_merchant", "used_vpn",
    "ip_country_mismatch", "billing_shipping_mismatch", "is_ai_generated_scam_attempt"
]

predict_fraud_udf = mlflow.pyfunc.spark_udf(
    spark,
    model_uri="models:/fraud_detection.data_ingection.fraud_ensemble_model/1",
    result_type="double",
    env_manager="virtualenv"
)

@dp.table(
    name="gold",
    comment="Real-time fraud probability predictions from ensemble model"
)
def score_transactions() -> DataFrame:
    silver_df = spark.readStream.table("silver")
    return (
        silver_df
        .withColumn("fraud_probability", predict_fraud_udf(F.struct(*FEATURE_COLS)))
        .withColumn("is_fraud_alert", F.col("fraud_probability") >= F.lit(0.70))
        .withColumn("scored_timestamp", F.current_timestamp())
    )


dp.create_sink(
    name="kafka_scored_sink",
    format="kafka",
    options={
        "kafka.bootstrap.servers": kafka_config["bootstrap_servers"],
        "topic": "TXC_SCORE",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": (
            f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{kafka_config["api_key"]}" password="{kafka_config["api_secret"]}";'
        ),
    }
)


@dp.append_flow(target="kafka_scored_sink", name="stream_gold_to_kafka")
def publish_scored_transactions():
    return (
        spark.readStream.table("gold")
        .select(
            F.col("id").cast("string").alias("key"),
            F.to_json(F.struct(
                "id", "merchant_category", "amount_usd",
                "fraud_probability", "is_fraud_alert", "scored_timestamp"
            )).alias("value")
        )
    )