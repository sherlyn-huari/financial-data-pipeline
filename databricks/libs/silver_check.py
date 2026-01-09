from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from typing import List, Callable, Any
from dataclasses import dataclass

@dataclass
class ValidationRule:
    column: str
    check_name: str
    validation_expr: Callable[[Any], Any]


CUSTOMER_VALIDATION_RULES = [
    ValidationRule(
        column='customer_id',
        check_name='null_customer_id',
        validation_expr=lambda col: col.isNull()
    ),
    ValidationRule(
        column='first_name',
        check_name='null_first_name',
        validation_expr=lambda col: col.isNull()
    ),
    ValidationRule(
        column='last_name',
        check_name='null_last_name',
        validation_expr=lambda col: col.isNull()
    ),
    ValidationRule(
        column='customer_segment',
        check_name='invalid_customer_segment',
        validation_expr=lambda col: col.isNotNull() & ~col.isin(['VIP', 'Premium', 'Standard', 'Basic'])
    ),
    ValidationRule(
        column='risk_score',
        check_name='invalid_risk_score',
        validation_expr=lambda col: col.isNotNull() & ((col < 0.0) | (col > 1.0))
    ),
]

MERCHANT_VALIDATION_RULES = [
    ValidationRule(
        column='merchant_id',
        check_name='null_merchant_id',
        validation_expr=lambda col: col.isNull()
    ),
    ValidationRule(
        column='category',
        check_name='invalid_category',
        validation_expr=lambda col: col.isNotNull() & ~col.isin([
            'Retail', 'Restaurants', 'Online Services', 'Transportation',
            'Utilities', 'Health & Pharmacies', 'Education', 'Entertainment', 'Financial Services'
        ])
    ),
]

TRANSACTION_VALIDATION_RULES = [
    ValidationRule(
        column='transaction_id',
        check_name='null_transaction_id',
        validation_expr=lambda col: col.isNull()
    ),
    ValidationRule(
        column='customer_id',
        check_name='null_customer_id',
        validation_expr=lambda col: col.isNull()
    ),
    ValidationRule(
        column='merchant_id',
        check_name='null_merchant_id',
        validation_expr=lambda col: col.isNull()
    ),
    ValidationRule(
        column='amount',
        check_name='invalid_amount',
        validation_expr=lambda col: col.isNotNull() & (col <= 0)
    ),
    ValidationRule(
        column='status',
        check_name='invalid_status',
        validation_expr=lambda col: col.isNotNull() & ~col.isin(['pending', 'completed', 'failed', 'cancelled'])
    ),
    ValidationRule(
        column='currency',
        check_name='invalid_currency',
        validation_expr=lambda col: col.isNotNull() & ~col.isin(['PEN', 'USD'])
    ),
    ValidationRule(
        column='channel',
        check_name='invalid_channel',
        validation_expr=lambda col: col.isNotNull() & ~col.isin(['online', 'mobile', 'atm', 'branch', 'pos', 'agent'])
    ),
]


def add_quality_flags(df: DataFrame, validation_rules: List[ValidationRule]) -> DataFrame:
    quality_issues = F.array().cast(ArrayType(StringType()))

    for rule in validation_rules:
        if rule.column in df.columns:
            quality_issues = F.when(
                rule.validation_expr(F.col(rule.column)),
                F.array_union(quality_issues, F.array(F.lit(rule.check_name)))
            ).otherwise(quality_issues)

    return (df
            .withColumn('quality_issues', quality_issues)
            .withColumn('is_valid', F.size(F.col('quality_issues')) == 0)
            .withColumn('quality_check_timestamp', F.current_timestamp())
    )


def add_quality_flags_customers(df: DataFrame) -> DataFrame:
    return add_quality_flags(df, CUSTOMER_VALIDATION_RULES)


def add_quality_flags_merchants(df: DataFrame) -> DataFrame:
    return add_quality_flags(df, MERCHANT_VALIDATION_RULES)


def add_quality_flags_transactions(df: DataFrame) -> DataFrame:
    return add_quality_flags(df, TRANSACTION_VALIDATION_RULES)
