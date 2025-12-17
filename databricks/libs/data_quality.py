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


def check_date_freshness(df: DataFrame, date_column: str, max_age_days: int) -> int:
    cutoff_date = datetime.now().date()

    stale_records = df.filter(
        F.datediff(F.lit(cutoff_date), F.col(date_column)) > max_age_days
    ).count()

    return stale_records


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

def calculate_completeness_score(df: DataFrame, important_columns: List[str])-> float:
    total_cells = df.count() * len(important_columns)

    if total_cells == 0:
        return 0.0
    
    non_null_count = 0
    for col in important_columns:
        if col in df.columns:
            non_null_count += df.filter(F.col(col).isNotNull()).count()
    
    completeness = (non_null_count / total_cells) * 100

    return completeness

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
def run_transaction_quality_checks(df: DataFrame) -> DataQualityReport:

    #nulls check
    report = DataQualityReport()
    null_counts = check_null_counts(df,['transaction_id', 'amount', 'customer_id'])
    total_nulls = sum(null_counts.values())
    report.add_check("Critical Nulls Check", 
                     f"{total_nulls} null found in critical columns",
                     total_nulls == 0)
    



    return report 


if __name__ == "__main__":
    

