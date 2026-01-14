# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - PHASE 5: ETL DAG BASE MODULE
# Tasks: T0023, T0026, T0027 (Sprint 5)
# ═══════════════════════════════════════════════════════════════════════

"""
dag_base.py - Shared DAG Configuration and Utilities

TASKS IMPLEMENTED:
- T0023: Build master DAG to trigger all pipelines - Shared configuration
- T0026: Backfill & catchup features - catchup=True, max_active_runs=3
- T0027: DAG failure handling strategy - Email callbacks, retries

Provides:
- Common DAG default arguments (retries, timeouts, email)
- Email notification settings (success/failure callbacks)
- Shared utility functions for all ETL DAGs
- Database connection utilities
- Path constants for data directories
"""

from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.utils.email import send_email
import os

# ========================================
# DAG DEFAULT ARGUMENTS
# ========================================

# Email for notifications
ALERT_EMAIL = os.environ.get('SMTP_USER', 'sidiot6969@gmail.com')

# ========================================
# Team 1 - T0032: Error Recovery Workflow
# ========================================
# Common default args for all DAGs (retries, timeouts)
DEFAULT_ARGS = {
    'owner': 'team1',
    'depends_on_past': False,
    'email': [ALERT_EMAIL],
    'email_on_failure': False,  # Disabled for testing
    'email_on_retry': False,    # Disabled for testing
    'retries': 3,
    'retry_delay': timedelta(minutes=1),  # Reduced for testing
    'execution_timeout': timedelta(hours=2),
}

# Schedule: Midnight IST = 18:30 UTC
# IST is UTC+5:30, so midnight IST = 18:30 previous day UTC
SCHEDULE_MIDNIGHT_IST = '30 18 * * *'

# Start date for catchup
START_DATE = datetime(2025, 12, 25)

# ========================================
# Team 1 - T0026: Backfill & Catchup Features
# ========================================
# Common DAG configuration
DAG_CONFIG = {
    'start_date': START_DATE,
    'schedule_interval': SCHEDULE_MIDNIGHT_IST,
    'catchup': True,
    'max_active_runs': 3,
    'tags': ['team1', 'etl', 'amazon'],
}


# ========================================
# Team 1 - T0027: DAG Failure Handling Strategy
# ========================================
# EMAIL NOTIFICATION FUNCTIONS
# ========================================

def send_success_email(context):
    """Send email on successful DAG completion"""
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    
    subject = f"✅ ETL Success: {dag_id}"
    body = f"""
    <h2>ETL Pipeline Completed Successfully</h2>
    <table border="1" cellpadding="5">
        <tr><td><b>DAG</b></td><td>{dag_id}</td></tr>
        <tr><td><b>Execution Date</b></td><td>{execution_date}</td></tr>
        <tr><td><b>Status</b></td><td style="color:green;">SUCCESS</td></tr>
    </table>
    <p>All tasks completed without errors.</p>
    """
    
    send_email(
        to=ALERT_EMAIL,
        subject=subject,
        html_content=body
    )


def send_failure_email(context):
    """Send email on task failure"""
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']
    exception = context.get('exception', 'Unknown error')
    
    subject = f"❌ ETL Failed: {dag_id} - {task_id}"
    body = f"""
    <h2>ETL Pipeline Failed</h2>
    <table border="1" cellpadding="5">
        <tr><td><b>DAG</b></td><td>{dag_id}</td></tr>
        <tr><td><b>Task</b></td><td>{task_id}</td></tr>
        <tr><td><b>Execution Date</b></td><td>{execution_date}</td></tr>
        <tr><td><b>Status</b></td><td style="color:red;">FAILED</td></tr>
        <tr><td><b>Error</b></td><td>{exception}</td></tr>
    </table>
    <p>Please check the logs for more details.</p>
    """
    
    send_email(
        to=ALERT_EMAIL,
        subject=subject,
        html_content=body
    )


# ========================================
# DATA PATHS
# ========================================

# Base paths (relative to Airflow container)
DATA_RAW = '/opt/airflow/data/raw/dataset'
DATA_STAGING = '/opt/airflow/data/staging'
DATA_PROCESSED = '/opt/airflow/data/processed'
DATA_REPORTS = '/opt/airflow/data/reports'

# ========================================
# Team 1 - T0029: Multi-Source Data Pipelines
# ========================================
# Source files
SOURCE_FILES = {
    'customers': f'{DATA_RAW}/Customers.csv',
    'sales': f'{DATA_RAW}/Sales.csv',
    'products': f'{DATA_RAW}/Products.csv',
    'stores': f'{DATA_RAW}/Stores.csv',
    'exchange_rates': f'{DATA_RAW}/Exchange_Rates.csv',
}

# Processed files
PROCESSED_FILES = {
    'customers': f'{DATA_PROCESSED}/customers_cleaned.csv',
    'sales': f'{DATA_PROCESSED}/sales_cleaned.csv',
    'products': f'{DATA_PROCESSED}/products_cleaned.csv',
    'stores': f'{DATA_PROCESSED}/stores_cleaned.csv',
    'exchange_rates': f'{DATA_PROCESSED}/exchange_rates_cleaned.csv',
}


# ========================================
# DATABASE CONFIGURATION
# ========================================

DB_CONFIG = {
    'host': 'postgres',  # Docker service name
    'port': 5432,
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow',
    'schema': 'etl_output'
}

def get_connection_string():
    """Get PostgreSQL connection string for ETL tables"""
    return (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )


# ========================================
# Team 1 - T0030: Reusable Pipeline Config
# ========================================
# XCOM KEYS FOR INTER-DAG COMMUNICATION

XCOM_KEYS = {
    'extraction_complete': 'extraction_complete',
    'transformation_complete': 'transformation_complete',
    'load_complete': 'load_complete',
    'row_count': 'row_count',
    'output_file': 'output_file',
}
