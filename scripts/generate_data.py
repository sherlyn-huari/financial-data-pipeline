import pandas as pd
import numpy as np
from faker import Faker
import random
from pathlib import Path

NUM_CUSTOMERS = 50000
NUM_MERCHANTS = 5000
NUM_TRANSACTIONS = 1000000
OUTPUT_DIR = Path("data")

fake = Faker(['es_MX'])
random.seed(42)
np.random.seed(42)


def generate_peruvian_ip():
    prefixes = [
        '181.176', '181.177', '181.178',  
        '190.232', '190.233',             
        '190.236', '190.237',             
        '200.48', '200.49',          
        '181.64', '181.65',        
    ]
    prefix = random.choice(prefixes)
    return f"{prefix}.{random.randint(0, 255)}.{random.randint(1, 254)}"


def generate_dni(birth_date):
    birth_year = birth_date.year
    if birth_year <= 1965:
        dni = random.randint(10000000, 25000000)
    elif birth_year <= 1975:
        dni = random.randint(20000000, 35000000)
    elif birth_year <= 1985:
        dni = random.randint(30000000, 49999999)
    elif birth_year <= 1995:
        dni = random.randint(50000000, 69999999)
    elif birth_year <= 2005:
        dni = random.randint(70000000, 79999999)
    else:
        dni = random.randint(80000000, 82000000)
    return f"{dni:08d}"


def generate_customers(n: int = NUM_CUSTOMERS) -> pd.DataFrame:
    customers = []
    for i in range(n):
        reg_date = fake.date_between(start_date='-2y', end_date='-1y')
        birth_date = fake.date_of_birth(minimum_age=18, maximum_age=61)      
        customers.append({
            'customer_id': f'CUST{i:08d}',
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': f"+51 9{random.randint(10000000, 99999999)}",
            'dni': generate_dni(birth_date),
            'date_of_birth': birth_date,
            'registration_date': reg_date,
            'city': 'Lima',
            'district': random.choice([
                'San Isidro', 'San Borja', 'Miraflores']),
            'country': 'Peru',
            'customer_segment': random.choices(
                ['VIP', 'Premium', 'Standard', 'Basic'],
                weights=[0.05, 0.15, 0.50, 0.30]
            )[0],
            'risk_score': round(random.uniform(0, 1), 4),
            'account_balance': round(random.lognormvariate(8, 2), 2),
            'credit_limit': round(random.lognormvariate(9, 1.5), 2),
            'is_active': random.choices([1, 0], weights=[0.95, 0.05])[0]
        })
    return pd.DataFrame(customers)


def generate_merchants(n: int = NUM_MERCHANTS) -> pd.DataFrame:
    merchant_configs = {
        'Retail': {
            'names': ['Wong', 'Plaza Vea'],
            'mcc': 5411
        },
        'Restaurants': {
            'names': ['Pardos Chicken', 'Bembos'],
            'mcc': 5812
        },
        'Online Services': {
            'names': ['Rappi', 'PedidosYa', 'Mercado Libre'],
            'mcc': 5967
        },
        'Transportation': {
            'names': ['Uber'],
            'mcc': 4121
        },
        'Utilities': {
            'names': ['Luz del Sur', 'Sedapal', 'Entel'],
            'mcc': 4900
        },
        'Health & Pharmacies': {
            'names': ['InKafarma', 'Mifarma'],
            'mcc': 5912
        },
        'Education': {
            'names': ['PUCP', 'UPC'],
            'mcc': 8220
        },
        'Entertainment': {
            'names': ['Cineplanet'],
            'mcc': 7832
        },
        'Financial Services': {
            'names': ['BCP', 'BBVA', 'Yape', 'Plin'],
            'mcc': 6011
        }
    }

    merchants = []
    for i in range(n):
        category = random.choice(list(merchant_configs.keys()))
        config = merchant_configs[category]

        merchant_name = random.choice(config['names'])

        merchants.append({
            'merchant_id': f'MERCH{i:06d}',
            'merchant_name': merchant_name,
            'category': category,
            'mcc_code': config['mcc'],
            'city': 'Lima',
            'country': 'Peru',
            'is_verified': random.choices([1, 0], weights=[0.9, 0.1])[0],
            'registration_date': fake.date_between(start_date="-3y", end_date='now')
        })

    return pd.DataFrame(merchants)


def generate_realistic_hour():
    hour_weights = {
        0: 0.6, 1: 0.4, 2: 0.3, 3: 0.3, 4: 0.4, 5: 0.6,
        6: 1.5, 7: 3.0, 8: 5.0, 9: 6.0, 10: 6.5, 11: 7.0,
        12: 7.5, 13: 6.5, 14: 5.5, 15: 5.5, 16: 6.0, 17: 6.5,
        18: 7.0, 19: 8.0, 20: 7.5, 21: 6.0, 22: 4.0, 23: 2.0
    }

    hours = list(hour_weights.keys())
    weights = list(hour_weights.values())
    return random.choices(hours, weights=weights)[0]


def generate_transactions(customers_df: pd.DataFrame, merchants_df: pd.DataFrame, n: int = NUM_TRANSACTIONS) -> pd.DataFrame:

    customer_ids = customers_df['customer_id'].tolist()
    merchant_ids = merchants_df['merchant_id'].tolist()

    transactions = []
    batch_size = 10000

    for batch_num in range(0, n, batch_size):
        batch_transactions = []

        for i in range(batch_num, min(batch_num + batch_size, n)):

            customer_id = random.choice(customer_ids)

            trans_date = fake.date_between(start_date='-12M', end_date='now')
            trans_hour = generate_realistic_hour()
            trans_minute = random.randint(0, 59)
            trans_second = random.randint(0, 59)
            trans_datetime = pd.Timestamp(year=trans_date.year, month=trans_date.month,
                                          day=trans_date.day, hour=trans_hour,
                                          minute=trans_minute, second=trans_second)

            trans_type = random.choices(
                ['purchase', 'withdrawal', 'transfer', 'payment', 'refund'],
                weights=[0.50, 0.20, 0.15, 0.10, 0.05]
            )[0]

            if trans_type == 'withdrawal':
                base_amount = random.choice([100, 200, 300, 500, 1000])
            else:
                base_amount = random.lognormvariate(4, 1.5)

            is_fraud = 1 if random.random() < 0.02 else 0

            if is_fraud and random.random() < 0.6:
                trans_hour = random.choice([0, 1, 2, 3, 4, 5])
                trans_datetime = trans_datetime.replace(hour=trans_hour)

            channel = random.choices(
                ['online', 'mobile', 'atm', 'branch', 'pos', 'agent'],
                weights=[0.25, 0.30, 0.10, 0.08, 0.20, 0.07]
            )[0]

            status = random.choices(
                ['completed', 'failed'],
                weights=[0.95, 0.05]
            )[0]

            transaction = {
                'transaction_id': f'TXN{i:012d}',
                'customer_id': customer_id,
                'merchant_id': random.choice(merchant_ids),
                'transaction_date': trans_datetime,
                'amount': round(base_amount, 2),
                'currency': random.choices(['PEN', 'USD'], weights=[0.85, 0.15])[0],
                'transaction_type': trans_type,
                'channel': channel,
                'device_id': fake.uuid4() if channel in ['online', 'mobile'] else None,
                'ip_address': generate_peruvian_ip() if channel in ['online', 'mobile'] else None,
                'location_lat': round(random.uniform(-12.2, -11.9), 6),
                'location_lon': round(random.uniform(-77.2, -76.8), 6),
                'card_type': random.choice(['credit', 'debit', None]),
                'card_last_4': random.randint(1000, 9999) if random.random() < 0.7 else None,
                'status': status,
                'is_fraud': is_fraud,
                'processing_fee': round(base_amount * random.uniform(0.01, 0.03), 2)
            }

            batch_transactions.append(transaction)

        transactions.extend(batch_transactions)

    return pd.DataFrame(transactions)


def main():
    OUTPUT_DIR.mkdir(exist_ok=True)
    (OUTPUT_DIR / "transactions").mkdir(exist_ok=True)

    customers = generate_customers()
    merchants = generate_merchants()
    transactions = generate_transactions(customers, merchants)

    customers.to_json(OUTPUT_DIR / 'customers.json', orient='records', lines=True)
    merchants.to_json(OUTPUT_DIR / 'merchants.json', orient='records', lines=True)

    transactions['year_month'] = pd.to_datetime(transactions['transaction_date']).dt.to_period('M')

    for period, group in transactions.groupby('year_month'):
        filename = OUTPUT_DIR / 'transactions' / f'transactions_{period}.csv'
        group_to_save = group.drop('year_month', axis=1).copy()
        group_to_save['transaction_date'] = pd.to_datetime(group_to_save['transaction_date']).dt.strftime('%Y-%m-%d %H:%M:%S')
        group_to_save.to_csv(filename, index=False)


if __name__ == "__main__":
    main()