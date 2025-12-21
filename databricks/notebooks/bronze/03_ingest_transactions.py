from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, BooleanType
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

if os.path.exists('/Workspace'):
    sys.path.insert(0, '/Workspace/Repos/databricks/libs')
else:
    libs_path = Path(__file__).parent.parent.parent / "libs"
    sys.path.insert(0, str(libs_path))

from data_quality import run_bronze_quality_checks

spark: SparkSession

load_dotenv()
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")

transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("location_lat", DoubleType(), True),
    StructField("location_lon", DoubleType(), True),
    StructField("card_type", StringType(), True),
    StructField("card_last_4", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("is_fraud", BooleanType(), True),
    StructField("fraud_reason", StringType(), True),
    StructField("processing_fee", DoubleType(), True),
    StructField("created_at", DateType(), True),
    StructField("updated_at", DateType(), True)
])

source_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/raw/transactions/*.csv"
target_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/bronze/transactions"

transactions_df = (spark.read
    .format("csv")
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(transaction_schema)
    .load(source_path))

bronze_transactions = (transactions_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
    .withColumn("data_source", lit("azure_csv")))

bronze_transactions.show(5, truncate=False)

expected_schema = {field.name: field.dataType.simpleString() for field in transaction_schema.fields}
quality_report = run_bronze_quality_checks(bronze_transactions, ['transaction_id'], expected_schema)
print(quality_report.get_summary())

(bronze_transactions.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(target_path))
