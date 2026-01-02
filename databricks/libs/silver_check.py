from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from typing import List, Dict, Any

# SILVER LAYER: Data cleaning and validation
# Purpose: Remove duplicates, check nulls, validate ranges, check relationships

#columns zero in each dataframe
# customers: list(customer_id, dni, first_name, last_name, email)
# merchants: list(merchant_id, registration_date, is_verified)
# transactions: list(transaction_id, customer_id, merchant_id, device_id)

def check_nulls(df: DataFrame, columns_zero: List[str]) -> Dict[str, Dict[str, Any]]:
    result = {}

    for column in df.columns:
        null_count = df.filter(F.col(column).isNull()).count()

        if column in columns_zero:
            if null_count == 0:
                result[column] = {"status": "passed"}
            else:
                result[column] = {"status": "not passed", "null_count": null_count}
        else:
            result[column] = {"null_count": null_count}

    return result


# customers: list(account_balance, credit_limit, risk_score, date_of_birth, registration_date)
# merchants: list(registration_date)
# transactions: list(transaction_date, amount, location_lat, location_lon)

def check_value_outrange(df: DataFrame, range_limit: Dict[str, Dict[str, float]]) -> Dict[str, Dict[str, Any]]:
    result = {}

    for column, range_vals in range_limit.items():
        if "min" not in range_vals or "max" not in range_vals:
            raise ValueError(f"Column {column} missing required keys. Expected min and max but got {list(range_vals.keys())}")

        total_count = df.filter((F.col(column) < range_vals["min"]) | (F.col(column) > range_vals["max"])).count()
        if total_count == 0:
            result[column] = {"status": "passed"}
        else:
            result[column] = {"status": "failed", "count": total_count}
    return result


# customers: list(district: 'San Isidro', 'San Borja', 'Jesús María'
#                 customer_segment: 'VIP', 'Premium', 'Standard', 'Basic')
#             list(is_active: 1, 0)

# merchants: list(category: 'Retail','Restaurants','Online Services',
#        'Transportation','Utilities','Health & Pharmacies',
#        'Education','Entertainment','Financial Services')
# list(merchant_name: 'Wong', 'Plaza Vea','Pardos Chicken', 'Bembos','Rappi', 'PedidosYa', 'Mercado Libre',
#  'Uber', 'Luz del Sur', 'Sedapal','Entel','InKafarma', 'Mifarma', 'PUCP', 'UPC','Cineplanet','BCP', 'BBVA', 'Yape', 'Plin')
# list(mcc: 5411, 5812, 5967, 4121, 4900, 5912, 8220, 7832, 6011)
# list(is_verified: 1,0)

# transactions: list(currency: USD, PEN)
# list(card_type: credit, debit)
# list(status: completed, failed)
# list(channel:'online', 'mobile', 'atm', 'branch', 'pos', 'agent')

def check_allowed_values(df: DataFrame, column_values: Dict[str, set[str]]) -> Dict[str, Dict[str, Any]]:
    result = {}

    for column, allowed_values in column_values.items():
        row = df.select(F.collect_set(column)).first()
        actual_values = set(row[0]) if row and row[0] else set()

        extra_values = actual_values - allowed_values
        missing_values = allowed_values - actual_values

        if len(extra_values) == 0 and len(missing_values) == 0:
            result[column] = {"status": "passed"}
        else:
            result[column] = {
                "status": "failed",
                "extra_values": list(extra_values) if extra_values else None,
                "missing_values": list(missing_values) if missing_values else None
            }

    return result


def check_duplicates(df: DataFrame, key_columns: List[str]) -> Dict[str, Any]:
    total = df.count()
    distinct = df.select(key_columns).distinct().count()
    duplicate_count = total - distinct

    if duplicate_count == 0:
        return {"status": "passed", "duplicate_count": 0}
    return {"status": "failed", "duplicate_count": duplicate_count}


def check_referential_integrity(df: DataFrame, ref_df: DataFrame, fk_column: str, pk_column: str) -> Dict[str, Any]:
    orphaned = df.join(ref_df, df[fk_column] == ref_df[pk_column], "left_anti").count()

    if orphaned == 0:
        return {"status": "passed", "orphaned_count": 0}
    return {"status": "failed", "orphaned_count": orphaned}


def add_quality_flags_customers(df: DataFrame) -> DataFrame:
    quality_issues = F.array().cast(ArrayType(StringType()))

    for col in ['customer_id', 'first_name', 'last_name']:
        quality_issues = F.when(
            F.col(col).isNull(),
            F.array_union(quality_issues, F.array(F.lit(f'null_{col}')))
        ).otherwise(quality_issues)

    valid_segments = ['VIP', 'Premium', 'Standard', 'Basic']
    quality_issues = F.when(
        (F.col('customer_segment').isNotNull()) & (~F.col('customer_segment').isin(valid_segments)),
        F.array_union(quality_issues, F.array(F.lit('invalid_customer_segment')))
    ).otherwise(quality_issues)

    quality_issues = F.when(
        (F.col('risk_score').isNotNull()) & ((F.col('risk_score') < 0.0) | (F.col('risk_score') > 1.0)),
        F.array_union(quality_issues, F.array(F.lit('invalid_risk_score')))
    ).otherwise(quality_issues)

    df_with_flags = df.withColumn('quality_issues', quality_issues)
    df_with_flags = df_with_flags.withColumn('is_valid', F.size(F.col('quality_issues')) == 0)
    df_with_flags = df_with_flags.withColumn('quality_check_timestamp', F.current_timestamp())

    return df_with_flags


def add_quality_flags_merchants(df: DataFrame) -> DataFrame:
    quality_issues = F.array().cast(ArrayType(StringType()))

    quality_issues = F.when(
        F.col('merchant_id').isNull(),
        F.array_union(quality_issues, F.array(F.lit('null_merchant_id')))
    ).otherwise(quality_issues)

    valid_categories = ['Retail', 'Restaurants', 'Online Services', 'Transportation',
                       'Utilities', 'Health & Pharmacies', 'Education', 'Entertainment', 'Financial Services']
    quality_issues = F.when(
        (F.col('category').isNotNull()) & (~F.col('category').isin(valid_categories)),
        F.array_union(quality_issues, F.array(F.lit('invalid_category')))
    ).otherwise(quality_issues)

    df_with_flags = df.withColumn('quality_issues', quality_issues)
    df_with_flags = df_with_flags.withColumn('is_valid', F.size(F.col('quality_issues')) == 0)
    df_with_flags = df_with_flags.withColumn('quality_check_timestamp', F.current_timestamp())

    return df_with_flags


def add_quality_flags_transactions(df: DataFrame) -> DataFrame:
    quality_issues = F.array().cast(ArrayType(StringType()))

    for col in ['transaction_id', 'customer_id', 'merchant_id']:
        quality_issues = F.when(
            F.col(col).isNull(),
            F.array_union(quality_issues, F.array(F.lit(f'null_{col}')))
        ).otherwise(quality_issues)

    quality_issues = F.when(
        (F.col('amount').isNotNull()) & (F.col('amount') <= 0),
        F.array_union(quality_issues, F.array(F.lit('invalid_amount')))
    ).otherwise(quality_issues)

    valid_statuses = ['completed', 'failed']
    quality_issues = F.when(
        (F.col('status').isNotNull()) & (~F.col('status').isin(valid_statuses)),
        F.array_union(quality_issues, F.array(F.lit('invalid_status')))
    ).otherwise(quality_issues)

    valid_currencies = ['PEN', 'USD']
    quality_issues = F.when(
        (F.col('currency').isNotNull()) & (~F.col('currency').isin(valid_currencies)),
        F.array_union(quality_issues, F.array(F.lit('invalid_currency')))
    ).otherwise(quality_issues)

    valid_channels = ['online', 'mobile', 'atm', 'branch', 'pos', 'agent']
    quality_issues = F.when(
        (F.col('channel').isNotNull()) & (~F.col('channel').isin(valid_channels)),
        F.array_union(quality_issues, F.array(F.lit('invalid_channel')))
    ).otherwise(quality_issues)

    df_with_flags = df.withColumn('quality_issues', quality_issues)
    df_with_flags = df_with_flags.withColumn('is_valid', F.size(F.col('quality_issues')) == 0)
    df_with_flags = df_with_flags.withColumn('quality_check_timestamp', F.current_timestamp())

    return df_with_flags


def generate_quality_summary(df: DataFrame) -> Dict:
    total_records = df.count()
    valid_records = df.filter(F.col('is_valid')).count()
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
    print('SILVER LAYER QUALITY SUMMARY')
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
