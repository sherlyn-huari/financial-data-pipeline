from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from typing import Dict, Any

# BRONZE LAYER: Format validation only - flag issues, don't fix them
# Purpose: Identify format errors in raw data

# customers: email, phone, dni, date of birth
# transactions: card_last_4

def check_email_format(df: DataFrame) -> Dict[str, Any]:
    invalid_count = df.filter(
        ~F.col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
    ).count()

    if invalid_count == 0:
        return {"status": "passed"}
    return {"status": "failed", "invalid_count": invalid_count}


def check_phone_format(df: DataFrame) -> Dict[str, Any]:
    invalid_count = df.filter(
        ~F.col("phone").rlike(r"^\+51 9\d{8}$")
    ).count()

    if invalid_count == 0:
        return {"status": "passed"}
    return {"status": "failed", "invalid_count": invalid_count}


def check_dni_format(df: DataFrame) -> Dict[str, Any]:
    invalid_count = df.filter(
        ~F.col("dni").rlike(r"^\d{8}$")
    ).count()

    if invalid_count == 0:
        return {"status": "passed"}
    return {"status": "failed", "invalid_count": invalid_count}


def check_date_of_birth_format(df: DataFrame) -> Dict[str, Any]:
    invalid_count = df.filter(
        F.col("date_of_birth").isNull() |
        (F.col("date_of_birth") > F.current_date())
    ).count()

    if invalid_count == 0:
        return {"status": "passed"}
    return {"status": "failed", "invalid_count": invalid_count}


def check_card_last_4_format(df: DataFrame) -> Dict[str, Any]:
    invalid_count = df.filter(
        F.col("card_last_4").isNotNull() &
        ~F.col("card_last_4").rlike(r"^\d{4}$")
    ).count()

    if invalid_count == 0:
        return {"status": "passed"}
    return {"status": "failed", "invalid_count": invalid_count}


def add_quality_flags_customers(df: DataFrame) -> DataFrame:
    quality_issues = F.array().cast(ArrayType(StringType()))

    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    quality_issues = F.when(
        (F.col('email').isNotNull()) & (~F.col('email').rlike(email_pattern)),
        F.array_union(quality_issues, F.array(F.lit('invalid_email_format')))
    ).otherwise(quality_issues)

    phone_pattern = r'^\+51 9\d{8}$'
    quality_issues = F.when(
        (F.col('phone').isNotNull()) & (~F.col('phone').rlike(phone_pattern)),
        F.array_union(quality_issues, F.array(F.lit('invalid_phone_format')))
    ).otherwise(quality_issues)

    dni_pattern = r'^\d{8}$'
    quality_issues = F.when(
        (F.col('dni').isNotNull()) & (~F.col('dni').rlike(dni_pattern)),
        F.array_union(quality_issues, F.array(F.lit('invalid_dni_format')))
    ).otherwise(quality_issues)

    quality_issues = F.when(
        (F.col('date_of_birth').isNotNull()) & (F.col('date_of_birth') > F.current_date()),
        F.array_union(quality_issues, F.array(F.lit('invalid_date_of_birth')))
    ).otherwise(quality_issues)

    df_with_flags = df.withColumn('quality_issues', quality_issues)
    df_with_flags = df_with_flags.withColumn('is_valid', F.size(F.col('quality_issues')) == 0)
    df_with_flags = df_with_flags.withColumn('quality_check_timestamp', F.current_timestamp())

    return df_with_flags


def add_quality_flags_transactions(df: DataFrame) -> DataFrame:
    quality_issues = F.array().cast(ArrayType(StringType()))

    card_last_4_pattern = r'^\d{4}$'
    quality_issues = F.when(
        (F.col('card_last_4').isNotNull()) & (~F.col('card_last_4').cast('string').rlike(card_last_4_pattern)),
        F.array_union(quality_issues, F.array(F.lit('invalid_card_last_4_format')))
    ).otherwise(quality_issues)

    df_with_flags = df.withColumn('quality_issues', quality_issues)
    df_with_flags = df_with_flags.withColumn('is_valid', F.size(F.col('quality_issues')) == 0)
    df_with_flags = df_with_flags.withColumn('quality_check_timestamp', F.current_timestamp())

    return df_with_flags


def generate_quality_summary(df: DataFrame) -> Dict:
    total_records = df.count()
    valid_records = df.filter(F.col('is_valid') == True).count()
    invalid_records = total_records - valid_records

    issues_exploded = df.filter(F.size(F.col('quality_issues')) > 0).select(
        F.explode('quality_issues').alias('issue_type')
    )

    issue_counts = {}
    if issues_exploded.count() > 0:
        issue_counts = {
            row['issue_type']: row['count']
            for row in issues_exploded.groupBy('issue_type').count().collect()
        }

    return {
        'total_records': total_records,
        'valid_records': valid_records,
        'invalid_records': invalid_records,
        'data_quality_score': (valid_records / total_records * 100) if total_records > 0 else 0.0,
        'issue_breakdown': issue_counts
    }


def print_quality_summary(summary: Dict):
    print('\n' + '='*60)
    print('BRONZE LAYER QUALITY SUMMARY')
    print('='*60)
    print(f"Total Records: {summary['total_records']}")
    print(f"Valid Records: {summary['valid_records']}")
    print(f"Invalid Records: {summary['invalid_records']}")
    print(f"Data Quality Score: {summary['data_quality_score']:.2f}%")

    if summary['issue_breakdown']:
        print('\nIssue Breakdown:')
        for issue, count in sorted(summary['issue_breakdown'].items(), key=lambda x: x[1], reverse=True):
            print(f"  - {issue}: {count}")

    print('='*60 + '\n')
