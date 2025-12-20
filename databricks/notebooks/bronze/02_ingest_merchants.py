from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os
from dotenv import load_dotenv

spark: SparkSession

load_dotenv()
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")

merchant_schema = StructType([
    StructField("merchant_id", StringType(), False),
    StructField("merchant_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("registration_date", StringType(), True)
])

source_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/raw/merchants/merchants.csv"
target_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/bronze/merchants"

merchants_df = (spark.read
    .format("csv")
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(merchant_schema)
    .load(source_path))

bronze_merchants = (merchants_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
    .withColumn("data_source", lit("azure_csv")))

bronze_merchants.show(5, truncate=False)

(bronze_merchants.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(target_path))

print(f"Ingested merchants to {target_path}")