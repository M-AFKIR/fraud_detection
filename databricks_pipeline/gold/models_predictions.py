import mlflow
import pyspark.sql.functions as F
from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame

mlflow.set_registry_uri("databricks-uc")

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