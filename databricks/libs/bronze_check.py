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


EMAIL_PATTERN = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
PHONE_PATTERN = r'^\+51 9\d{8}$'
DNI_PATTERN = r'^\d{8}$'
CARD_LAST_4_PATTERN = r'^\d{4}$'


CUSTOMER_VALIDATION_RULES = [
    ValidationRule(
        column='email',
        check_name='invalid_email_format',
        validation_expr=lambda col: ~col.rlike(EMAIL_PATTERN)
    ),
    ValidationRule(
        column='phone',
        check_name='invalid_phone_format',
        validation_expr=lambda col: ~col.rlike(PHONE_PATTERN)
    ),
    ValidationRule(
        column='dni',
        check_name='invalid_dni_format',
        validation_expr=lambda col: ~col.rlike(DNI_PATTERN)
    ),
    ValidationRule(
        column='date_of_birth',
        check_name='invalid_date_of_birth',
        validation_expr=lambda col: col > F.current_date()
    ),
]

TRANSACTION_VALIDATION_RULES = [
    ValidationRule(
        column='card_last_4',
        check_name='invalid_card_last_4_format',
        validation_expr=lambda col: ~col.cast('string').rlike(CARD_LAST_4_PATTERN)
    ),
]


def add_quality_flags(df: DataFrame, validation_rules: List[ValidationRule]) -> DataFrame:
    quality_issues = F.array().cast(ArrayType(StringType()))

    for rule in validation_rules:
        if rule.column in df.columns:
            quality_issues = F.when(
                F.col(rule.column).isNotNull() & rule.validation_expr(F.col(rule.column)),
                F.array_union(quality_issues, F.array(F.lit(rule.check_name)))
            ).otherwise(quality_issues)

    return (df
            .withColumn('quality_issues', quality_issues)
            .withColumn('is_valid', F.size(F.col('quality_issues')) == 0)
        )


def add_quality_flags_customers(df: DataFrame) -> DataFrame:
    return add_quality_flags(df, CUSTOMER_VALIDATION_RULES)


def add_quality_flags_transactions(df: DataFrame) -> DataFrame:
    return add_quality_flags(df, TRANSACTION_VALIDATION_RULES)
