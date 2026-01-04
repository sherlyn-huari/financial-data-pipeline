from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
import os
from pathlib import Path
from dotenv import load_dotenv

spark: SparkSession

load_dotenv()
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")

merchant_schema = StructType([
    StructField("merchant_id", StringType(), False),
    StructField("merchant_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("mcc_code", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("is_verified", BooleanType(), True),
    StructField("registration_date", DateType(), True)
])

source_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/raw/merchants/merchants.json"
target_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/bronze/merchants"

merchants_df = (spark.read
    .format("json")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(merchant_schema)
    .load(source_path))

bronze_merchants = (merchants_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
    .withColumn("data_source", lit("azure_json")))

(bronze_merchants.write
    .format('delta')
    .mode('overwrite')
    .option('mergeSchema', 'true')
    .save(target_path))
