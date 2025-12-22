import pandas as pd
from typing import Dict, List, Any
import random
from pathlib import Path

DATA_DIR = Path("data")

def add_quality_issues_to_customers(df: pd.DataFrame) -> pd.DataFrame:
    df_dirty= df.copy()
    n_rows = len(df_dirty) 

    n_missing_emails = int(n_rows * 0.003)
    n_invalid_segments = int(n_rows * 0.0006)
    n_invalid_risk = int(n_rows * 0.0009)
    n_city_typos = int(n_rows * 0.006)
    
    index_email = random.sample(range(n_rows), n_missing_emails)
    df_dirty.loc[index_email, 'email'] = ''

    index_customer_segment = random.sample(range(n_rows), n_invalid_segments)
    df_dirty.loc[index_customer_segment, 'customer_segment'] = 'Basicos'

    index_risk_score = random.sample(range(n_rows),n_invalid_risk)
    df_dirty.loc[index_risk_score,'risk_score'] = 1.23

    index_city = random.sample(range(n_rows), n_city_typos)
    df_dirty.loc[index_city,'city'] = 'Limasse'

    return df_dirty

def add_quality_issues_to_merchants(df: pd.DataFrame) -> pd.DataFrame:
    df_dirty = df.copy()
    n_rows = len(df_dirty)

    n_missing_category = int(n_rows * 0.004)
    n_invalid_country = int(n_rows * 0.002)
    n_invalid_mcc = int(n_rows * 0.003)
    n_name_typos = int(n_rows * 0.005)

    index_category = random.sample(range(n_rows), n_missing_category)
    df_dirty.loc[index_category, 'category'] = ''

    index_country = random.sample(range(n_rows), n_invalid_country)
    df_dirty.loc[index_country, 'country'] = 'Perú'

    index_mcc = random.sample(range(n_rows), n_invalid_mcc)
    df_dirty.loc[index_mcc, 'mcc_code'] = 99999

    index_name = random.sample(range(n_rows), n_name_typos)
    df_dirty.loc[index_name, 'merchant_name'] = df_dirty.loc[index_name, 'merchant_name'].str.replace('S.A.', 'SA', regex=False)

    return df_dirty

def add_quality_issues_to_transactions(df: pd.DataFrame) -> pd.DataFrame:
    df_dirty = df.copy()
    n_rows = len(df_dirty)

    n_negative_amounts = int(n_rows * 0.0005)
    n_invalid_status = int(n_rows * 0.0008)
    n_missing_merchant = int(n_rows * 0.0015)
    n_invalid_currency = int(n_rows * 0.001)
    n_invalid_channel = int(n_rows * 0.0012)

    index_amount = random.sample(range(n_rows), n_negative_amounts)
    df_dirty.loc[index_amount, 'amount'] = df_dirty.loc[index_amount, 'amount'] * -1

    index_status = random.sample(range(n_rows), n_invalid_status)
    df_dirty.loc[index_status, 'status'] = 'processing'

    index_merchant = random.sample(range(n_rows), n_missing_merchant)
    df_dirty.loc[index_merchant, 'merchant_id'] = None

    index_currency = random.sample(range(n_rows), n_invalid_currency)
    df_dirty.loc[index_currency, 'currency'] = 'SOL'

    index_channel = random.sample(range(n_rows), n_invalid_channel)
    df_dirty.loc[index_channel, 'channel'] = 'app'

    return df_dirty

def main():
    customers = pd.read_csv(DATA_DIR / 'customers.csv')
    customers_dirty = add_quality_issues_to_customers(customers)
    customers_dirty.to_csv(DATA_DIR / 'customers_dirty.csv', index=False)

    merchants = pd.read_csv(DATA_DIR / 'merchants.csv')
    merchants_dirty = add_quality_issues_to_merchants(merchants)
    merchants_dirty.to_csv(DATA_DIR / 'merchants_dirty.csv', index=False)

    transaction_files = list((DATA_DIR / 'transactions').glob('transactions_*.csv'))
    for txn_file in transaction_files:
        transactions = pd.read_csv(txn_file)
        transactions_dirty = add_quality_issues_to_transactions(transactions)
        dirty_filename = txn_file.stem + '_dirty.csv'
        transactions_dirty.to_csv(DATA_DIR / 'transactions' / dirty_filename, index=False)


if __name__=="__main__":
    main()