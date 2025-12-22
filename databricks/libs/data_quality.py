from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, List, Any
from datetime import datetime

# SECTION 1: BASIC CHECKS
def check_null_counts(df: DataFrame, critical_columns: List[str]) -> Dict[str, int]:
    null_counts = {}

    for col in critical_columns:
        if col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            null_counts[col] = null_count
        else:
            null_counts[col] = -1 

    return null_counts

def check_duplicate_keys(df: DataFrame, key_columns: List[str]) -> int:
    total_count = df.count()
    distinct_count = df.select(key_columns).distinct().count()

    duplicates = total_count - distinct_count
    return duplicates

# SECTION 2: BUSINESS LOGIC CHECKS

def check_value_ranges(df: DataFrame, column: str, min_val: float, max_val: float) -> int:
    out_of_range = df.filter(
        (F.col(column) < min_val) | (F.col(column) > max_val)
    ).count()

    return out_of_range

def check_allowed_values(df: DataFrame, column: str, allowed_values: List) -> Dict:

    actual_values = [row[column] for row in df.select(column).distinct().collect()] 

    invalid_values = [v for v in actual_values if v not in allowed_values and v is not None]

    invalid_counts = {}
    for val in invalid_values:
        count = df.filter(F.col(column) == val).count()
        invalid_counts[val] = count

    return invalid_counts

# SECTION 3: SCHEMA CHECKS 

def check_referential_integrity(df_child: DataFrame, df_parent: DataFrame, child_key: str, parent_key: str) -> int:
    orphaned = df_child.join(
        df_parent,
        df_child[child_key]==df_parent[parent_key],
        "left_anti"
    ).count()

    return orphaned

def detect_schema_drift( current_df: DataFrame, expected_schema: Dict[str, str]) -> Dict[str, List[str]]:
    current_schema = {field.name: field.dataType.simpleString() for field in current_df.schema.fields}
    missing_columns = [col for col in expected_schema if col not in current_schema]
    extra_columns = [col for col in current_schema if col not in expected_schema]

    type_mismatches = []
    for col, expected_type in expected_schema.items():
        if col in current_schema and current_schema[col] != expected_type:
            type_mismatches.append(f"{col}: expected {expected_type}, got {current_schema[col]}")
    return {
        'missing_columns': missing_columns,
        'extra_columns': extra_columns,
        'type_mismatches': type_mismatches
    }

# SECTION 4: ORCHESTRATION 

class DataQualityReport:

    def __init__(self):
        self.checks = {}
        self.passed = True 
        self.timestamp = datetime.now()

    def add_check(self, check_name: str, result: Any, passed: bool):
        self.checks[check_name]= {
            'result': result,
            'passed': passed
        }
        if not passed: 
            self.passed = False

    def get_summary(self) -> str:
        summary = f"Data Quality Report - {self.timestamp}\n"
        summary += f"Overall Status: {'PASSED' if self.passed else 'FAILED'}\n\n"

        for check_name, check_data in self.checks.items():
            status = 'ok' if check_data['passed'] else 'not ok'
            summary += f"{status} {check_name}: {check_data['result']}\n"

        return summary
    
def run_bronze_quality_checks(df: DataFrame, key_columns: List[str], expected_schema: Dict[str, str]) -> DataQualityReport:
    report = DataQualityReport()

    row_count = df.count()
    report.add_check("Row Count", f"{row_count} rows", True)

    null_counts = check_null_counts(df, key_columns)
    total_nulls = sum(null_counts.values())
    report.add_check("Null Tracking", f"{total_nulls} nulls in key columns", True)

    duplicates = check_duplicate_keys(df, key_columns)
    report.add_check("Duplicate Tracking", f"{duplicates} duplicates", True)

    drift = detect_schema_drift(df, expected_schema)
    has_drift = (len(drift['missing_columns']) > 0 or
                 len(drift['extra_columns']) > 0 or
                 len(drift['type_mismatches']) > 0)
    report.add_check("Schema Drift", f"{drift}", not has_drift)

    return report

def run_silver_transaction_quality_checks(df: DataFrame) -> DataQualityReport:
    report = DataQualityReport()

    null_counts = check_null_counts(df,['transaction_id','amount', 'customer_id', 'merchant_id'])
    total_nulls = sum(null_counts.values())
    report.add_check("Critical Nulls Check",
                     f"{total_nulls} nulls found",
                     total_nulls == 0)

    duplicates = check_duplicate_keys(df, ['transaction_id'])
    report.add_check("Duplicate Keys Check",
                     f"{duplicates} duplicates",
                     duplicates == 0)

    if 'amount' in df.columns:
        invalid_amounts = check_value_ranges(df, 'amount', 0.01, 10000000)
        report.add_check("Amount Range Check", f"{invalid_amounts} out of range", invalid_amounts == 0)

    if 'status' in df.columns:
        invalid_statuses = check_allowed_values(
            df,
            'status',
            ['pending', 'completed', 'failed', 'cancelled']
        )
        report.add_check(
            "Status Values Check",
            f"Invalid: {invalid_statuses}" if invalid_statuses else "All valid",
            len(invalid_statuses) == 0
        )

    completeness = calculate_completeness_score(
        df,
        ['transaction_id', 'amount', 'customer_id', 'transaction_date']
    )
    report.add_check(
        "Completeness Score",
        f"{completeness:.2f}%",
        completeness >= 95.0
    )

    return report