import pandas as pd
import numpy as np
from faker import Faker
import random
from pathlib import Path

NUM_CUSTOMERS = 50000
NUM_MERCHANTS = 5000
NUM_TRANSACTIONS = 1000000
OUTPUT_DIR = Path("data")

fake = Faker(['es_PE'])
random.seed(42)
np.random.seed(42)

def generate_customers(n=NUM_CUSTOMERS):
    customers = []
    for i in range(n):
        reg_date = fake.date_between(start_date='-5y', end_date='-1y')
        customers.append({
            'customer_id': f'CUST{i:08d}',
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email':fake.email(),
            'phone':fake.phone_number(),
            'dni': fake.random_number(digits=8,fix_len=True),
            'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=70),
            'registration_date': reg_date,
            'city': 'Lima',
            'district': random.choice([
                'San Isidro', 'Miraflores', 'Surco',
                'La Molina', 'San Borja', 'Jesús María'
            ]),
            'country': 'Peru',
            'customer_segment': random.choices(
                ['VIP', 'Premium', 'Standard', 'Basic'],
                weights=[0.05, 0.15, 0.50, 0.30]
            )[0],
            'risk_score': round(random.uniform(0,1), 4),
            'account_balance': round(random.lognormvariate(8,2), 2),
            'credit_limit': round(random.lognormvariate(9, 1.5), 2),
            'is_active': random.choices([1,0], weights=[0.95, 0.05])[0],
            'created_at': reg_date,
            'updated_at': fake.date_time_between(start_date=reg_date, end_date='now')
            })
    return pd.DataFrame(customers)

def generate_merchants(n=NUM_MERCHANTS):
    categories = [
        'Retail','Restaurants & Fast Food','E-commerce & Online Services',
        'Transportation','Telecom','Utilities','Health & Pharmacies', 
        'Education','Entertainment','Financial Services']
   
    merchants = []

    for i in range(n):
        merchants.append({
            'merchant_id':f'MERCH{i:06d}',
            'merchant_name': fake.company(),
            'category': random.choice(categories),
            'mcc_code': random.randint(1000,9999),
            'city': fake.city(),
            'country': 'Peru',
            'is_verified': random.choices([1, 0], weights=[0.9, 0.1])[0],
            'registration_date': fake.date_between(start_date="-3y", end_date='now')
            })
        
    return pd.DataFrame(merchants)

def generate_transactions(customers_df, merchants_df, n=NUM_TRANSACTIONS):

    customer_ids = customers_df['customer_id'].tolist()
    merchant_ids = merchants_df['merchant_id'].tolist()

    high_risk_customers = customers_df[
        customers_df['risk_score'] > 0.8
    ]['customer_id'].tolist()

    transactions = []
    batch_size = 10000

    for batch_num in range(0, n, batch_size):
        batch_transactions = []

        for i in range(batch_num, min(batch_num + batch_size, n)):

            is_high_risk = random.random() < 0.05
            customer_id = random.choice(
                high_risk_customers if is_high_risk and high_risk_customers else customer_ids
            )

            trans_date = fake.date_time_between(start_date='-12M', end_date='now')

            trans_type = random.choices(
                ['purchase', 'withdrawal', 'transfer', 'payment', 'refund'],
                weights=[0.50, 0.20, 0.15, 0.10, 0.05]
            )[0]
            
            if trans_type == 'withdrawal':
                base_amount = random.choice([100, 200, 300, 500, 1000])
            else:
                base_amount = random.lognormvariate(4, 1.5)
            
            is_fraud = 0
            fraud_reason = None

            if base_amount > 5000 and random.random() < 0.3:
                is_fraud = 1
                fraud_reason = 'high_amount'
                base_amount *= random.uniform(2, 5)
            
            if random.random() < 0.01:
                is_fraud = 1
                fraud_reason = 'high_velocity'

            if is_high_risk and random.random() < 0.05:
                is_fraud = 1
                fraud_reason = 'high_risk_customer'

            channel = random.choices(
                ['online', 'mobile', 'atm', 'branch', 'pos'],
                weights=[0.30, 0.25, 0.15, 0.10, 0.20]
            )[0]

            if is_fraud:
                status = random.choices(
                    ['completed', 'failed', 'cancelled'],
                    weights=[0.30, 0.50, 0.20]
                )[0]
            else:
                status = random.choices(
                    ['completed', 'failed', 'pending'],
                    weights=[0.94, 0.04, 0.02]
                )[0]

            transaction = {
                'transaction_id': f'TXN{i:012d}',
                'customer_id': customer_id,
                'merchant_id': random.choice(merchant_ids),
                'transaction_date': trans_date,
                'amount': round(base_amount, 2),
                'currency': random.choices(['PEN', 'USD'], weights=[0.85, 0.15])[0],
                'transaction_type': trans_type,
                'channel': channel,
                'device_id': fake.uuid4() if channel in ['online', 'mobile'] else None,
                'ip_address': fake.ipv4() if channel in ['online', 'mobile'] else None,
                'location_lat': round(random.uniform(-12.2, -11.9), 6),
                'location_lon': round(random.uniform(-77.2, -76.8), 6),
                'card_type': random.choice(['credit', 'debit', None]),
                'card_last_4': random.randint(1000, 9999) if random.random() < 0.7 else None,
                'status': status,
                'is_fraud': is_fraud,
                'fraud_reason': fraud_reason,
                'processing_fee': round(base_amount * random.uniform(0.01, 0.03), 2),
                'created_at': trans_date,
                'updated_at': trans_date
            }

            batch_transactions.append(transaction)
        
        transactions.extend(batch_transactions)

        if (batch_num + batch_size) % 100000 == 0:
            print(f"Generated {batch_num + batch_size:,} transactions")
    
    return pd.DataFrame(transactions)

def main():
    print("Transaction Data Generator")

    OUTPUT_DIR.mkdir(exist_ok=True)
    (OUTPUT_DIR / "transactions").mkdir(exist_ok=True)

    customers = generate_customers()
    merchants = generate_merchants()
    transactions = generate_transactions(customers, merchants)

    customers.to_csv(OUTPUT_DIR / 'customers.csv', index=False)
    print(f"Saved customers.csv ({len(customers):,} rows)")

    merchants.to_csv(OUTPUT_DIR / 'merchants.csv', index=False)
    print(f"Saved merchants.csv ({len(merchants):,} rows)")

    transactions['year_month'] = pd.to_datetime(
        transactions['transaction_date']
    ).dt.to_period('M')
    
    for period, group in transactions.groupby('year_month'):
        filename = OUTPUT_DIR / 'transactions' / f'transactions_{period}.csv'
        group.drop('year_month', axis=1).to_csv(filename, index=False)
        print(f"Saved {filename.name} ({len(group):,} rows)")

    print("DATA GENERATION SUMMARY")
    print(f"Customers: {len(customers):,}")
    print(f"Merchants: {len(merchants):,}")
    print(f"Transactions: {len(transactions):,}")
    print(f"Fraud Count: {transactions['is_fraud'].sum():,}")
    print(f"Total Amount: S/{transactions['amount'].sum():,.2f}")
    print(f"Date Range: {transactions['transaction_date'].min()} to {transactions['transaction_date'].max()}")
    print("\nTransactions Status:")
    print(transactions['status'].value_counts())
    print("\nTransactions Types:")
    print(transactions['transaction_type'].value_counts())


if __name__ == "__main__":
    main()