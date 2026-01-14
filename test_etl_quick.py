#!/usr/bin/env python
import sys
sys.path.insert(0, 'scripts')

from Extract import extract_etl_data
from TransformAmazon import transform_etl_data  
from Load import load_etl_data

print("Running ETL pipeline test...")

extracted = extract_etl_data()
transformed, rejected = transform_etl_data(extracted)
result = load_etl_data(transformed, rejected)

print('\n=== FINAL LOAD RESULT ===')
print(f'Status: {result["status"]}')
print(f'Total Loaded: {result.get("total_loaded", 0)}')
print(f'Total Rejected: {result.get("total_rejected", 0)}')
if result.get('failed_datasets'):
    print(f'Failed: {result["failed_datasets"]}')
else:
    print('✓ All datasets loaded successfully!')

# Verify data in database
print("\n=== DATABASE VERIFICATION ===")
from sqlalchemy import create_engine, text
engine = create_engine('postgresql://airflow:airflow@localhost:5432/airflow')
with engine.begin() as conn:
    for table in ['customers_cleaned', 'sales_cleaned', 'products', 'stores', 'exchange_rates']:
        result = conn.execute(text(f'SELECT COUNT(*) FROM etl.output_{table}'))
        count = result.scalar()
        print(f'output_{table}: {count} records')
