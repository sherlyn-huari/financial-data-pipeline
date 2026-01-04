from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, BooleanType
import os
from pathlib import Path
from dotenv import load_dotenv

if not os.path.exists('/Workspace'):
    libs_path = Path(__file__).parent.parent.parent / "libs"
    import sys
    sys.path.insert(0, str(libs_path))

from bronze_check import add_quality_flags_customers, generate_quality_summary, print_quality_summary

spark: SparkSession

load_dotenv()
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")

customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("dni", StringType(), False),
    StructField("date_of_birth", DateType(), True),
    StructField("registration_date", DateType(), True),
    StructField("city", StringType(), True),
    StructField("district", StringType(), True),
    StructField("country", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("risk_score", DoubleType(), True),
    StructField("account_balance", DoubleType(), True),
    StructField("credit_limit", DoubleType(), True),
    StructField("is_active", BooleanType(), False),
    StructField("created_at", DateType(), True)
])

source_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/raw/customers/customers.json"
target_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/bronze/customers"

customers_df = (spark.read
    .format("json")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(customer_schema)
    .load(source_path))

bronze_customers = (customers_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
    .withColumn("data_source", lit("azure_json")))

bronze_customers_with_quality = add_quality_flags_customers(bronze_customers)

quality_summary = generate_quality_summary(bronze_customers_with_quality)
print_quality_summary(quality_summary)

(bronze_customers_with_quality.write
    .format('delta')
    .mode('overwrite')
    .option('mergeSchema', 'true')
    .save(target_path))
