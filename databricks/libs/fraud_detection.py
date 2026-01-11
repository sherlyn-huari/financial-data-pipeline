from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def calculate_fraud_score(df: DataFrame) -> DataFrame:
    window_1h = Window.partitionBy("customer_id").orderBy(F.col("transaction_date").cast("long")).rangeBetween(-3600, 0)
    window_customer = Window.partitionBy("customer_id")

    return (df
            .withColumn("transaction_hour", F.hour(F.col("transaction_date")))
            .withColumn("txn_count_1h", F.count("transaction_id").over(window_1h))
            .withColumn("customer_avg_amount", F.avg("amount").over(window_customer))
            .withColumn("is_high_velocity", F.when(F.col("txn_count_1h") >= 5, 1).otherwise(0))
            .withColumn("is_unusual_hour", F.when(F.col("transaction_hour").between(0, 5), 1).otherwise(0))
            .withColumn("is_high_amount", F.when(F.col("amount") > F.col("customer_avg_amount") * 5, 1).otherwise(0))
            .withColumn("fraud_score", (F.col("is_high_velocity") * 40 + F.col("is_unusual_hour") * 30 + F.col("is_high_amount") * 30))
            .withColumn("risk_level", F.when(F.col("fraud_score") >= 70, "HIGH").when(F.col("fraud_score") >= 40, "MEDIUM").otherwise("LOW"))
            )


def get_high_risk_transactions(df: DataFrame, min_score: int = 70) -> DataFrame:
    return (df
            .filter(F.col("fraud_score") >= min_score)
            .select("transaction_id", "customer_id", "transaction_date", "amount", "fraud_score", "risk_level", "is_fraud")
            .orderBy(F.col("fraud_score").desc())
            )
