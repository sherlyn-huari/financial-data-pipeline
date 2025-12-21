from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType,BooleanType
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
    StructField("risk_score", IntegerType(), True),
    StructField("account_balance", DoubleType(), True),
    StructField("credit_limit", DoubleType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("created_at", DateType(), True),
    StructField("updated_at", DateType(), True)
])

source_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/raw/customers/customers.csv"
target_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/bronze/customers"

customers_df = (spark.read
    .format("csv")
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(customer_schema)
    .load(source_path))

bronze_customers = (customers_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
    .withColumn("data_source", lit("azure_csv")))

bronze_customers.show(5, truncate=False)

expected_schema = {field.name: field.dataType.simpleString() for field in customer_schema.fields}
quality_report = run_bronze_quality_checks(bronze_customers, ['customer_id', 'dni'], expected_schema)
print(quality_report.get_summary())

(bronze_customers.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(target_path))

