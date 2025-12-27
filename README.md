# Financial Data Pipeline
A data pipeline that processes financial transactions using PySpark and Delta Lake on Azure.

## What It Does

- Reads transaction data from customers and merchants
- Cleans and validates the data
- Creates analytics tables

## Pipeline Flow

**Bronze Layer** → Raw data ingestion
**Silver Layer** → Clean and validate
**Gold Layer** → Analytics tables

## Project Structure

```
databricks/notebooks/
├── bronze/   # 3 notebooks - ingest customers, merchants, transactions
├── silver/   # 3 notebooks - clean data
└── gold/     # 4 notebooks - analytics

databricks/libs/
└── data_quality.py  # validation functions

scripts/
├── generate_data.py  # creates test data
├── corrupt_data.py   # adds errors for testing
└── upload_to_datalake.py
```

## Tech Stack

- PySpark
- Delta Lake
- Azure Databricks
- Azure Data Lake

## Test Data

- 50,000 customers
- 5,000 merchants
- 1,000,000 transactions

## Setup

```bash
pip install -r requirements.txt
python scripts/generate_data.py
python scripts/corrupt_data.py
python scripts/upload_to_datalake.py
```

Then run notebooks: bronze → silver → gold

## What It Handles

- Invalid emails and missing data
- Negative amounts
- Bad currencies/channels
- Typos in city/country names
