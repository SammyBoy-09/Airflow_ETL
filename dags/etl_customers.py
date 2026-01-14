# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - PHASE 5: ETL CUSTOMERS DAG
# Tasks: T0023-T0027 (Sprint 5)
# ═══════════════════════════════════════════════════════════════════════

"""
etl_customers.py - Customers Table ETL Pipeline

TASKS IMPLEMENTED:
- T0023: Build master DAG to trigger all pipelines
- T0024: Event-driven DAG triggering (email callbacks)
- T0025: Multi-DAG dependency management (independent DAG)
- T0026: Backfill & catchup features (catchup=True)
- T0027: DAG failure handling strategy (retries, email alerts)

Pipeline: Extract → Transform → Load (Customers)
Schedule: Midnight IST (18:30 UTC)
Dependencies: None (Independent)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import sys
import os
sys.path.insert(0, '/opt/airflow/scripts')

from dag_base import (
    DEFAULT_ARGS, DAG_CONFIG, SCHEDULE_MIDNIGHT_IST, START_DATE,
    send_success_email, send_failure_email,
    DATA_RAW, DATA_STAGING, DATA_PROCESSED, get_connection_string
)


# ========================================
# Team 1 - T0028: Combined E-T-L Pipeline
# ========================================
# ETL TASK FUNCTIONS

def extract_customers(**context):
    """Extract customers data from CSV"""
    import pandas as pd
    from pathlib import Path
    
    # DATA_RAW already includes /dataset subfolder
    source_file = f'{DATA_RAW}/Customers.csv'
    staging_file = f'{DATA_STAGING}/customers_raw.csv'
    
    # Read source
    df = pd.read_csv(source_file)
    row_count = len(df)
    
    # Save to staging
    df.to_csv(staging_file, index=False)
    
    print(f"✅ Extracted {row_count:,} customers to staging")
    
    # Push to XCom
    context['ti'].xcom_push(key='row_count', value=row_count)
    context['ti'].xcom_push(key='staging_file', value=staging_file)
    
    return {'rows': row_count, 'file': staging_file}


def transform_customers(**context):
    """Clean and transform customers data"""
    import pandas as pd
    import numpy as np
    import re
    
    staging_file = context['ti'].xcom_pull(key='staging_file', task_ids='extract')
    output_file = f'{DATA_PROCESSED}/customers_cleaned.csv'
    
    # Read from staging
    df = pd.read_csv(staging_file)
    initial_rows = len(df)
    
    # ========================================
    # Team 1 - T0010: Duplicate data detection & removal
    # ========================================
    df = df.drop_duplicates(subset=['CustomerKey'], keep='first')
    duplicates_removed = initial_rows - len(df)
    
    # ========================================
    # Team 1 - T0009: Handle incorrect data types (birthday)
    # ========================================
    if 'Birthday' in df.columns:
        df['Birthday'] = pd.to_datetime(df['Birthday'], errors='coerce')
    
    # ========================================
    # Team 1 - T0008: Build reusable cleaning utilities (typecast)
    # Team 1 - T0011: Missing data handling (mean fill)
    # ========================================
    if 'Age' in df.columns:
        df['Age'] = pd.to_numeric(df['Age'], errors='coerce')
        mean_age = df['Age'].mean()
        df['Age'] = df['Age'].fillna(mean_age).astype(int)
    
    # ========================================
    # Team 1 - T0009: Handle incorrect data types (email validation)
    # ========================================
    if 'Email' in df.columns:
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        def validate_email(email):
            if pd.isna(email):
                return 'no-email@unknown.com'
            if re.match(email_pattern, str(email)):
                return email
            return 'invalid-email@unknown.com'
        
        df['Email'] = df['Email'].apply(validate_email)
    
    # Save cleaned data
    df.to_csv(output_file, index=False)
    
    print(f"✅ Transformed customers: {initial_rows:,} → {len(df):,} rows")
    print(f"   Duplicates removed: {duplicates_removed}")
    
    context['ti'].xcom_push(key='output_file', value=output_file)
    context['ti'].xcom_push(key='cleaned_rows', value=len(df))
    
    return {'rows': len(df), 'file': output_file}


def load_customers(**context):
    """Load customers to PostgreSQL"""
    import pandas as pd
    from sqlalchemy import create_engine, text
    
    output_file = context['ti'].xcom_pull(key='output_file', task_ids='transform')
    
    # Read cleaned data
    df = pd.read_csv(output_file)
    
    # Connect to database
    conn_str = get_connection_string()
    engine = create_engine(conn_str)
    
    # Create schema if not exists (handle race condition)
    with engine.begin() as conn:
        try:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS etl_output"))
        except Exception:
            pass  # Schema already exists
        conn.execute(text("DROP TABLE IF EXISTS etl_output.customers"))
    
    # Load to database
    df.to_sql(
        'customers',
        engine,
        schema='etl_output',
        if_exists='replace',
        index=False
    )
    
    print(f"✅ Loaded {len(df):,} customers to etl_output.customers")
    
    context['ti'].xcom_push(key='loaded_rows', value=len(df))
    
    return {'rows': len(df), 'table': 'etl_output.customers'}


# ========================================
# DAG DEFINITION
# ========================================

with DAG(
    dag_id='etl_customers',
    default_args={
        **DEFAULT_ARGS,
        'on_failure_callback': send_failure_email,
    },
    description='ETL Pipeline for Customers table',
    start_date=START_DATE,
    schedule_interval=None,  # Disabled for testing - manual trigger only
    catchup=False,
    max_active_runs=3,
    tags=['team1', 'etl', 'customers'],
) as dag:
    
    # Start marker
    start = EmptyOperator(task_id='start')
    
    # Extract task
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_customers,
        provide_context=True,
    )
    
    # Transform task
    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_customers,
        provide_context=True,
    )
    
    # Load task
    load = PythonOperator(
        task_id='load',
        python_callable=load_customers,
        provide_context=True,
    )
    
    # End marker with success callback
    end = EmptyOperator(
        task_id='end',
        on_success_callback=send_success_email,
    )
    
    # Task dependencies
    start >> extract >> transform >> load >> end
