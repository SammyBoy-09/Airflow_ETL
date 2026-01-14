# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - PHASE 5: ETL SALES DAG
# Tasks: T0023-T0027, especially T0024-T0025 (Sprint 5)
# ═══════════════════════════════════════════════════════════════════════

"""
etl_sales.py - Sales Table ETL Pipeline

TASKS IMPLEMENTED:
- T0023: Build master DAG to trigger all pipelines
- T0024: Event-driven DAG triggering (ExternalTaskSensor)
- T0025: Multi-DAG dependency management (waits for etl_products)
- T0026: Backfill & catchup features (catchup=True)
- T0027: DAG failure handling strategy (retries, email alerts)

Pipeline: Wait for Products → Extract → Transform → Load (Sales)
Schedule: Midnight IST (18:30 UTC)
Dependencies: WAITS FOR etl_products (uses ExternalTaskSensor)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun
from airflow.utils.state import DagRunState

import sys
sys.path.insert(0, '/opt/airflow/scripts')


def get_latest_execution_date(dt, external_dag_id):
    """Get the latest successful execution date for an external DAG."""
    # Find the most recent successful run of the external DAG
    dag_runs = DagRun.find(
        dag_id=external_dag_id,
        state=DagRunState.SUCCESS,
    )
    if dag_runs:
        # Return the most recent successful run's execution date
        latest_run = max(dag_runs, key=lambda x: x.execution_date)
        return latest_run.execution_date
    # If no successful runs, return the current execution date
    return dt

from dag_base import (
    DEFAULT_ARGS, SCHEDULE_MIDNIGHT_IST, START_DATE,
    send_success_email, send_failure_email,
    DATA_RAW, DATA_STAGING, DATA_PROCESSED, get_connection_string
)


# ========================================
# Team 1 - T0024: Event-Driven DAG Triggering
# ========================================

# ========================================
# ETL TASK FUNCTIONS
# ========================================

def extract_sales(**context):
    """Extract sales data from CSV"""
    import pandas as pd
    
    # DATA_RAW already includes /dataset subfolder
    source_file = f'{DATA_RAW}/Sales.csv'
    staging_file = f'{DATA_STAGING}/sales_raw.csv'
    
    df = pd.read_csv(source_file)
    row_count = len(df)
    
    df.to_csv(staging_file, index=False)
    
    print(f"✅ Extracted {row_count:,} sales records to staging")
    
    context['ti'].xcom_push(key='row_count', value=row_count)
    context['ti'].xcom_push(key='staging_file', value=staging_file)
    
    return {'rows': row_count, 'file': staging_file}


def transform_sales(**context):
    """Clean and transform sales data"""
    import pandas as pd
    
    staging_file = context['ti'].xcom_pull(key='staging_file', task_ids='extract')
    output_file = f'{DATA_PROCESSED}/sales_cleaned.csv'
    
    df = pd.read_csv(staging_file)
    initial_rows = len(df)
    
    # Parse date columns
    date_columns = ['Order Date', 'Delivery Date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Remove duplicates based on Order Number
    df = df.drop_duplicates(subset=['Order Number'], keep='first')
    duplicates_removed = initial_rows - len(df)
    
    # Ensure numeric columns are properly typed
    numeric_cols = ['Quantity', 'ProductKey', 'StoreKey', 'CustomerKey']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    
    # Remove invalid records (negative quantities)
    if 'Quantity' in df.columns:
        invalid = len(df[df['Quantity'] <= 0])
        df = df[df['Quantity'] > 0]
        print(f"   Removed {invalid} records with invalid quantity")
    
    df.to_csv(output_file, index=False)
    
    print(f"✅ Transformed sales: {initial_rows:,} → {len(df):,} rows")
    print(f"   Duplicates removed: {duplicates_removed}")
    
    context['ti'].xcom_push(key='output_file', value=output_file)
    context['ti'].xcom_push(key='cleaned_rows', value=len(df))
    
    return {'rows': len(df), 'file': output_file}


def load_sales(**context):
    """Load sales to PostgreSQL"""
    import pandas as pd
    from sqlalchemy import create_engine, text
    
    output_file = context['ti'].xcom_pull(key='output_file', task_ids='transform')
    
    df = pd.read_csv(output_file)
    
    conn_str = get_connection_string()
    engine = create_engine(conn_str)
    
    # Create schema if not exists (handle race condition)
    with engine.begin() as conn:
        try:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS etl_output"))
        except Exception:
            pass  # Schema already exists
        conn.execute(text("DROP TABLE IF EXISTS etl_output.sales"))
    
    df.to_sql(
        'sales',
        engine,
        schema='etl_output',
        if_exists='replace',
        index=False
    )
    
    print(f"✅ Loaded {len(df):,} sales to etl_output.sales")
    
    context['ti'].xcom_push(key='loaded_rows', value=len(df))
    
    return {'rows': len(df), 'table': 'etl_output.sales'}


# ========================================
# DAG DEFINITION
# ========================================

with DAG(
    dag_id='etl_sales',
    default_args={
        **DEFAULT_ARGS,
        'on_failure_callback': send_failure_email,
    },
    description='ETL Pipeline for Sales table (waits for Products)',
    start_date=START_DATE,
    schedule_interval=None,  # Disabled for testing - manual trigger only
    catchup=False,
    max_active_runs=3,
    tags=['team1', 'etl', 'sales'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Wait for Products DAG to complete (Sales references Products)
    # For testing with manual triggers, we look for the latest successful run
    wait_for_products = ExternalTaskSensor(
        task_id='wait_for_products',
        external_dag_id='etl_products',
        external_task_id='end',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        execution_date_fn=lambda dt: get_latest_execution_date(dt, 'etl_products'),
        timeout=7200,  # 2 hours
        poke_interval=30,  # Check every 30 seconds
        mode='reschedule',  # Free worker while waiting
    )
    
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_sales,
        provide_context=True,
    )
    
    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_sales,
        provide_context=True,
    )
    
    load = PythonOperator(
        task_id='load',
        python_callable=load_sales,
        provide_context=True,
    )
    
    end = EmptyOperator(
        task_id='end',
        on_success_callback=send_success_email,
    )
    
    start >> wait_for_products >> extract >> transform >> load >> end
