# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - MASTER ORCHESTRATOR DAG
# Orchestrates all ETL pipelines with proper sequencing and logging
# ═══════════════════════════════════════════════════════════════════════

"""
etl_master_orchestrator.py - Master Pipeline Orchestrator

PURPOSE:
- Single entry point to run entire ETL pipeline
- Triggers child DAGs in correct dependency order
- Waits for each stage to complete before proceeding
- Logs execution status and timing for all DAGs
- Provides centralized monitoring and control

EXECUTION ORDER:
  Stage 1 (Parallel): etl_customers, etl_products, etl_stores, etl_exchange_rates
  Stage 2 (Sequential): etl_sales (after products complete)
  Stage 3 (Final): etl_reports (after ALL tables complete)

FEATURES:
- TriggerDagRunOperator: Triggers child DAGs
- ExternalTaskSensor: Waits for DAG completion
- Execution logging with timing metrics
- Configurable trigger modes (wait_for_completion)
"""

from datetime import datetime, timedelta
import json
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.task_group import TaskGroup

import sys
sys.path.insert(0, '/opt/airflow/scripts')

from dag_base import (
    DEFAULT_ARGS, START_DATE,
    send_success_email, send_failure_email,
    DATA_PROCESSED
)


# ========================================
# CONFIGURATION
# ========================================

# Child DAGs to orchestrate
INDEPENDENT_DAGS = ['etl_customers', 'etl_products', 'etl_stores', 'etl_exchange_rates']
DEPENDENT_DAGS = ['etl_sales']  # Depends on etl_products
FINAL_DAGS = ['etl_reports']    # Depends on ALL table DAGs

# Sensor configuration
SENSOR_TIMEOUT = 3600  # 1 hour timeout per DAG
SENSOR_POKE_INTERVAL = 30  # Check every 30 seconds


# ========================================
# HELPER FUNCTIONS
# ========================================

def get_triggered_run_id(context, dag_id):
    """Get the run_id of the DAG we just triggered."""
    # The triggered DAG run_id follows a pattern based on our trigger
    trigger_run_id = context['run_id']
    return f"triggered__{trigger_run_id}__{dag_id}"


def get_latest_execution_date(dt, external_dag_id):
    """Get the latest execution date for an external DAG (any state)."""
    dag_runs = DagRun.find(dag_id=external_dag_id)
    if dag_runs:
        # Get the most recent run
        latest_run = max(dag_runs, key=lambda x: x.execution_date)
        return latest_run.execution_date
    return dt


def log_stage_start(**context):
    """Log the start of a pipeline stage."""
    stage = context['params'].get('stage', 'Unknown')
    dags = context['params'].get('dags', [])
    
    print("=" * 60)
    print(f"🚀 STAGE: {stage}")
    print(f"📅 Execution Date: {context['execution_date']}")
    print(f"🔧 DAGs to process: {', '.join(dags)}")
    print("=" * 60)
    
    context['ti'].xcom_push(key=f'{stage}_start_time', value=datetime.now().isoformat())
    return {'stage': stage, 'dags': dags, 'status': 'started'}


def log_stage_complete(**context):
    """Log the completion of a pipeline stage."""
    stage = context['params'].get('stage', 'Unknown')
    dags = context['params'].get('dags', [])
    
    start_time_str = context['ti'].xcom_pull(key=f'{stage}_start_time')
    start_time = datetime.fromisoformat(start_time_str) if start_time_str else datetime.now()
    duration = (datetime.now() - start_time).total_seconds()
    
    print("=" * 60)
    print(f"✅ STAGE COMPLETE: {stage}")
    print(f"⏱️  Duration: {duration:.2f} seconds")
    print(f"📊 DAGs completed: {', '.join(dags)}")
    print("=" * 60)
    
    context['ti'].xcom_push(key=f'{stage}_duration', value=duration)
    return {'stage': stage, 'dags': dags, 'status': 'completed', 'duration': duration}


# ========================================
# Team 1 - T0031: Pipeline Execution Summary
# ========================================
def generate_execution_summary(**context):
    """Generate comprehensive execution summary for all DAGs."""
    ti = context['ti']
    run_id = context['run_id']
    execution_date = context['execution_date']
    
    print("\n" + "=" * 70)
    print("📊 MASTER ORCHESTRATOR - EXECUTION SUMMARY")
    print("=" * 70)
    
    # Collect timing for each stage
    stages = ['Stage1_Independent', 'Stage2_Sales', 'Stage3_Reports']
    total_duration = 0
    stage_results = []
    
    for stage in stages:
        duration = ti.xcom_pull(key=f'{stage}_duration') or 0
        total_duration += duration
        stage_results.append({
            'stage': stage,
            'duration_seconds': duration,
            'duration_formatted': f"{duration:.2f}s"
        })
        print(f"  {stage}: {duration:.2f} seconds")
    
    print(f"\n⏱️  TOTAL PIPELINE DURATION: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
    
    # Get status of all child DAGs
    all_dags = INDEPENDENT_DAGS + DEPENDENT_DAGS + FINAL_DAGS
    dag_statuses = []
    
    print("\n📋 CHILD DAG STATUS:")
    print("-" * 50)
    
    for dag_id in all_dags:
        dag_runs = DagRun.find(dag_id=dag_id, state=DagRunState.SUCCESS)
        if dag_runs:
            latest = max(dag_runs, key=lambda x: x.execution_date)
            status = 'SUCCESS'
            exec_date = latest.execution_date.isoformat()
        else:
            # Check for running or failed
            all_runs = DagRun.find(dag_id=dag_id)
            if all_runs:
                latest = max(all_runs, key=lambda x: x.execution_date)
                status = str(latest.state).upper()
                exec_date = latest.execution_date.isoformat()
            else:
                status = 'NO RUNS'
                exec_date = 'N/A'
        
        dag_statuses.append({
            'dag_id': dag_id,
            'status': status,
            'last_execution': exec_date
        })
        status_icon = '✅' if status == 'SUCCESS' else '❌' if status == 'FAILED' else '🔄'
        print(f"  {status_icon} {dag_id}: {status}")
    
    print("-" * 50)
    
    # Save summary to file
    summary = {
        'orchestrator_run_id': run_id,
        'execution_date': str(execution_date),
        'total_duration_seconds': total_duration,
        'stages': stage_results,
        'dag_statuses': dag_statuses,
        'generated_at': datetime.now().isoformat()
    }
    
    # Save to reports directory
    reports_dir = os.path.join(DATA_PROCESSED, 'reports')
    os.makedirs(reports_dir, exist_ok=True)
    
    summary_file = os.path.join(reports_dir, 'orchestrator_execution_summary.json')
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2, default=str)
    
    print(f"\n💾 Summary saved to: {summary_file}")
    print("=" * 70)
    
    # Push summary to XCom
    ti.xcom_push(key='execution_summary', value=summary)
    
    return summary


# ========================================
# DAG DEFINITION
# ========================================

# ========================================
# Team 1 - T0023: Build Master DAG to Trigger All Pipelines
# ========================================
with DAG(
    dag_id='etl_master_orchestrator',
    default_args={
        **DEFAULT_ARGS,
        'on_failure_callback': send_failure_email,
    },
    description='Master Orchestrator - Triggers and monitors all ETL pipelines',
    start_date=START_DATE,
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['team1', 'orchestrator', 'master'],
) as dag:
    
    # ========================================
    # PIPELINE START & END
    # ========================================
    
    start = EmptyOperator(task_id='pipeline_start')
    
    end = EmptyOperator(
        task_id='pipeline_end',
        on_success_callback=send_success_email,
    )
    
    # ========================================
    # INITIALIZATION TASK GROUP
    # ========================================
    
    with TaskGroup(
        group_id='initialization',
        tooltip='Pipeline initialization and logging setup'
    ) as init_group:
        
        log_pipeline_start = PythonOperator(
            task_id='log_start',
            python_callable=log_stage_start,
            params={'stage': 'Pipeline', 'dags': INDEPENDENT_DAGS + DEPENDENT_DAGS + FINAL_DAGS},
        )
    
    # ========================================
    # STAGE 1: DIMENSION TABLES (PARALLEL)
    # ========================================
    
    with TaskGroup(
        group_id='stage1_dimension_tables',
        tooltip='Load dimension tables: Customers, Products, Stores, Exchange Rates'
    ) as stage1_group:
        
        log_stage1_start = PythonOperator(
            task_id='log_start',
            python_callable=log_stage_start,
            params={'stage': 'Stage1_Independent', 'dags': INDEPENDENT_DAGS},
        )
        
        log_stage1_complete = PythonOperator(
            task_id='log_complete',
            python_callable=log_stage_complete,
            params={'stage': 'Stage1_Independent', 'dags': INDEPENDENT_DAGS},
        )
        
        # Customers sub-group
        with TaskGroup(
            group_id='customers',
            tooltip='ETL: Customers dimension table'
        ) as customers_group:
            trigger_customers = TriggerDagRunOperator(
                task_id='trigger',
                trigger_dag_id='etl_customers',
                wait_for_completion=False,
                reset_dag_run=True,
                poke_interval=30,
            )
            wait_customers = ExternalTaskSensor(
                task_id='wait_complete',
                external_dag_id='etl_customers',
                external_task_id='end',
                allowed_states=['success'],
                failed_states=['failed', 'skipped'],
                execution_date_fn=lambda dt: get_latest_execution_date(dt, 'etl_customers'),
                timeout=SENSOR_TIMEOUT,
                poke_interval=SENSOR_POKE_INTERVAL,
                mode='reschedule',
            )
            trigger_customers >> wait_customers
        
        # Products sub-group
        with TaskGroup(
            group_id='products',
            tooltip='ETL: Products dimension table'
        ) as products_group:
            trigger_products = TriggerDagRunOperator(
                task_id='trigger',
                trigger_dag_id='etl_products',
                wait_for_completion=False,
                reset_dag_run=True,
                poke_interval=30,
            )
            wait_products = ExternalTaskSensor(
                task_id='wait_complete',
                external_dag_id='etl_products',
                external_task_id='end',
                allowed_states=['success'],
                failed_states=['failed', 'skipped'],
                execution_date_fn=lambda dt: get_latest_execution_date(dt, 'etl_products'),
                timeout=SENSOR_TIMEOUT,
                poke_interval=SENSOR_POKE_INTERVAL,
                mode='reschedule',
            )
            trigger_products >> wait_products
        
        # Stores sub-group
        with TaskGroup(
            group_id='stores',
            tooltip='ETL: Stores dimension table'
        ) as stores_group:
            trigger_stores = TriggerDagRunOperator(
                task_id='trigger',
                trigger_dag_id='etl_stores',
                wait_for_completion=False,
                reset_dag_run=True,
                poke_interval=30,
            )
            wait_stores = ExternalTaskSensor(
                task_id='wait_complete',
                external_dag_id='etl_stores',
                external_task_id='end',
                allowed_states=['success'],
                failed_states=['failed', 'skipped'],
                execution_date_fn=lambda dt: get_latest_execution_date(dt, 'etl_stores'),
                timeout=SENSOR_TIMEOUT,
                poke_interval=SENSOR_POKE_INTERVAL,
                mode='reschedule',
            )
            trigger_stores >> wait_stores
        
        # Exchange Rates sub-group
        with TaskGroup(
            group_id='exchange_rates',
            tooltip='ETL: Exchange Rates dimension table'
        ) as exchange_rates_group:
            trigger_exchange_rates = TriggerDagRunOperator(
                task_id='trigger',
                trigger_dag_id='etl_exchange_rates',
                wait_for_completion=False,
                reset_dag_run=True,
                poke_interval=30,
            )
            wait_exchange_rates = ExternalTaskSensor(
                task_id='wait_complete',
                external_dag_id='etl_exchange_rates',
                external_task_id='end',
                allowed_states=['success'],
                failed_states=['failed', 'skipped'],
                execution_date_fn=lambda dt: get_latest_execution_date(dt, 'etl_exchange_rates'),
                timeout=SENSOR_TIMEOUT,
                poke_interval=SENSOR_POKE_INTERVAL,
                mode='reschedule',
            )
            trigger_exchange_rates >> wait_exchange_rates
        
        # Stage 1 internal dependencies
        log_stage1_start >> [customers_group, products_group, stores_group, exchange_rates_group] >> log_stage1_complete
    
    # ========================================
    # STAGE 2: FACT TABLE (SALES)
    # ========================================
    
    with TaskGroup(
        group_id='stage2_fact_table',
        tooltip='Load fact table: Sales (depends on Products)'
    ) as stage2_group:
        
        log_stage2_start = PythonOperator(
            task_id='log_start',
            python_callable=log_stage_start,
            params={'stage': 'Stage2_Sales', 'dags': DEPENDENT_DAGS},
        )
        
        with TaskGroup(
            group_id='sales',
            tooltip='ETL: Sales fact table'
        ) as sales_group:
            trigger_sales = TriggerDagRunOperator(
                task_id='trigger',
                trigger_dag_id='etl_sales',
                wait_for_completion=False,
                reset_dag_run=True,
                poke_interval=30,
            )
            wait_sales = ExternalTaskSensor(
                task_id='wait_complete',
                external_dag_id='etl_sales',
                external_task_id='end',
                allowed_states=['success'],
                failed_states=['failed', 'skipped'],
                execution_date_fn=lambda dt: get_latest_execution_date(dt, 'etl_sales'),
                timeout=SENSOR_TIMEOUT,
                poke_interval=SENSOR_POKE_INTERVAL,
                mode='reschedule',
            )
            trigger_sales >> wait_sales
        
        log_stage2_complete = PythonOperator(
            task_id='log_complete',
            python_callable=log_stage_complete,
            params={'stage': 'Stage2_Sales', 'dags': DEPENDENT_DAGS},
        )
        
        log_stage2_start >> sales_group >> log_stage2_complete
    
    # ========================================
    # STAGE 3: ANALYTICS & REPORTS
    # ========================================
    
    with TaskGroup(
        group_id='stage3_reports',
        tooltip='Generate analytics reports (depends on ALL tables)'
    ) as stage3_group:
        
        log_stage3_start = PythonOperator(
            task_id='log_start',
            python_callable=log_stage_start,
            params={'stage': 'Stage3_Reports', 'dags': FINAL_DAGS},
        )
        
        with TaskGroup(
            group_id='reports',
            tooltip='Generate all analytics reports'
        ) as reports_group:
            trigger_reports = TriggerDagRunOperator(
                task_id='trigger',
                trigger_dag_id='etl_reports',
                wait_for_completion=False,
                reset_dag_run=True,
                poke_interval=30,
            )
            wait_reports = ExternalTaskSensor(
                task_id='wait_complete',
                external_dag_id='etl_reports',
                external_task_id='end',
                allowed_states=['success'],
                failed_states=['failed', 'skipped'],
                execution_date_fn=lambda dt: get_latest_execution_date(dt, 'etl_reports'),
                timeout=SENSOR_TIMEOUT,
                poke_interval=SENSOR_POKE_INTERVAL,
                mode='reschedule',
            )
            trigger_reports >> wait_reports
        
        log_stage3_complete = PythonOperator(
            task_id='log_complete',
            python_callable=log_stage_complete,
            params={'stage': 'Stage3_Reports', 'dags': FINAL_DAGS},
        )
        
        log_stage3_start >> reports_group >> log_stage3_complete
    
    # ========================================
    # FINALIZATION TASK GROUP
    # ========================================
    
    with TaskGroup(
        group_id='finalization',
        tooltip='Generate execution summary and finalize pipeline'
    ) as final_group:
        
        generate_summary = PythonOperator(
            task_id='generate_summary',
            python_callable=generate_execution_summary,
        )
    
    # ========================================
    # MAIN PIPELINE DEPENDENCY CHAIN
    # ========================================
    
    # Pipeline flow: Start → Init → Stage1 → Stage2 → Stage3 → Final → End
    start >> init_group >> stage1_group >> stage2_group >> stage3_group >> final_group >> end
