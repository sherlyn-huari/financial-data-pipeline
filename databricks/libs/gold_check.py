from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from typing import Dict, Any

# GOLD LAYER: Business metrics validation
# Purpose: Validate aggregated data and business metrics

def check_aggregation_consistency(df: DataFrame, group_cols: list, agg_col: str, expected_total: float = None) -> Dict[str, Any]:
    total = df.agg(F.sum(agg_col)).collect()[0][0]

    if expected_total is not None:
        diff = abs(total - expected_total)
        if diff < 0.01:
            return {"status": "passed", "total": total}
        return {"status": "failed", "total": total, "expected": expected_total, "difference": diff}

    return {"status": "passed", "total": total}


def check_no_negative_metrics(df: DataFrame, metric_columns: list) -> Dict[str, Dict[str, Any]]:
    result = {}

    for col in metric_columns:
        negative_count = df.filter(F.col(col) < 0).count()
        if negative_count == 0:
            result[col] = {"status": "passed"}
        else:
            result[col] = {"status": "failed", "negative_count": negative_count}

    return result


def check_metric_bounds(df: DataFrame, column: str, min_val: float, max_val: float) -> Dict[str, Any]:
    out_of_bounds = df.filter((F.col(column) < min_val) | (F.col(column) > max_val)).count()

    if out_of_bounds == 0:
        return {"status": "passed"}
    return {"status": "failed", "out_of_bounds_count": out_of_bounds}


def check_completeness(df: DataFrame, required_columns: list) -> Dict[str, Any]:
    total_rows = df.count()
    incomplete_rows = df.filter(
        F.array_contains(
            F.array(*[F.col(c).isNull() for c in required_columns]),
            True
        )
    ).count()

    completeness_pct = ((total_rows - incomplete_rows) / total_rows * 100) if total_rows > 0 else 0

    if completeness_pct == 100:
        return {"status": "passed", "completeness": completeness_pct}
    return {"status": "failed", "completeness": completeness_pct, "incomplete_rows": incomplete_rows}


def add_quality_flags_aggregations(df: DataFrame, metric_columns: list) -> DataFrame:
    quality_issues = F.array().cast(ArrayType(StringType()))

    for col in metric_columns:
        quality_issues = F.when(
            F.col(col).isNull(),
            F.array_union(quality_issues, F.array(F.lit(f'null_{col}')))
        ).otherwise(quality_issues)

        quality_issues = F.when(
            (F.col(col).isNotNull()) & (F.col(col) < 0),
            F.array_union(quality_issues, F.array(F.lit(f'negative_{col}')))
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
    print('GOLD LAYER QUALITY SUMMARY')
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
