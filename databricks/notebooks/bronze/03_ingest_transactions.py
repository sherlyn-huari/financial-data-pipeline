from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import os
from dotenv import load_dotenv
import sys
sys.path.append('/Workspace/Repos/databricks/libs')
from data_quality import run_transaction_quality_checks

spark: SparkSession

load_dotenv()
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")

transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("payment_method", StringType(), True)
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

quality_report = run_transaction_quality_checks(bronze_transactions)
print(quality_report.get_summary())

(bronze_transactions.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(target_path))
