from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

if os.path.exists('/Workspace'):
    sys.path.insert(0, '/Workspace/Repos/databricks/libs')
else:
    libs_path = Path(__file__).parent.parent.parent / "libs"
    sys.path.insert(0, str(libs_path))

from silver_check import add_quality_flags_transactions

spark: SparkSession

load_dotenv()
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")

bronze_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/bronze/transactions"
silver_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/silver/transactions"

bronze_df = spark.read.format("delta").load(bronze_path)

silver_transactions = (bronze_df
    .filter(col("is_valid") == True)
    .filter(col("transaction_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("merchant_id").isNotNull())
    .filter(col("amount") > 0)
    .filter(col("status").isin(['pending', 'completed', 'failed', 'cancelled']))
    .filter(col("currency").isin(['PEN', 'USD']))
    .filter(col("channel").isin(['online', 'mobile', 'atm', 'branch', 'pos', 'agent']))
    .withColumn("transaction_date", to_timestamp(col("transaction_date")))
    .withColumn("processed_timestamp", current_timestamp())
    .select(
        "transaction_id", "customer_id", "merchant_id", "transaction_date",
        "amount", "currency", "transaction_type", "channel", "device_id",
        "ip_address", "location_lat", "location_lon", "card_type", "card_last_4",
        "status", "is_fraud", "fraud_reason", "processing_fee",
        "created_at", "processed_timestamp"
    ))

silver_transactions_with_flags = add_quality_flags_transactions(silver_transactions)

(silver_transactions_with_flags.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(silver_path))
