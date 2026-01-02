from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper, trim, current_timestamp
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

if os.path.exists('/Workspace'):
    sys.path.insert(0, '/Workspace/Repos/databricks/libs')
else:
    libs_path = Path(__file__).parent.parent.parent / "libs"
    sys.path.insert(0, str(libs_path))

from silver_check import add_quality_flags_customers, generate_quality_summary, print_quality_summary

spark: SparkSession

load_dotenv()
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")

bronze_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/bronze/customers"
silver_path = f"abfss://bronze@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/silver/customers"

bronze_df = spark.read.format("delta").load(bronze_path)

silver_customers = (bronze_df
    .filter(col("is_valid") == True)
    .filter(col("customer_id").isNotNull())
    .filter(col("dni").isNotNull())
    .filter(col("is_active") == True)
    .withColumn("email", trim(col("email")))
    .withColumn("city", when(col("city") == "Limasse", "Lima").otherwise(trim(col("city"))))
    .withColumn("customer_segment", trim(col("customer_segment")))
    .withColumn("risk_score", when((col("risk_score") > 1.0) | (col("risk_score") < 0.0), None).otherwise(col("risk_score")))
    .withColumn("processed_timestamp", current_timestamp())
    .select(
        "customer_id", "first_name", "last_name", "email", "phone", "dni",
        "date_of_birth", "registration_date", "city", "district", "country",
        "customer_segment", "risk_score", "account_balance", "credit_limit",
        "is_active", "created_at", "updated_at", "processed_timestamp"
    ))

quality_report = validate_silver_customers(silver_customers)
print(quality_report.summary())

(silver_customers.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .save(silver_path))
