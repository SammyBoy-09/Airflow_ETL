# Product Requirements Document (PRD)
# Reusable ETL Pipeline Framework with Airflow

**Project Name:** Modular ETL Pipeline Framework with Multi-DAG Orchestration  
**Document Version:** 3.0  
**Date:** January 11, 2026  
**Status:** APPROVED - Implementation in Progress  
**Timeline:** 2 Days (~26.5 hours)

---

## 1. Executive Summary

### 1.1 Problem Statement
Build a **reusable, config-driven ETL framework** that:
- Handles multiple data sources (Customers, Sales, Products, Stores, Exchange Rates)
- Applies consistent data cleaning and validation rules
- Transforms data based on configurable rules
- Loads data with proper error handling
- Orchestrates complex multi-DAG dependencies with series and parallel execution
- Supports backfill, incremental loads, and error recovery

### 1.2 Solution Architecture
**Master Orchestrator DAG** managing 5 individual source DAGs with:
- **Task Groups** for logical organization (Extract → Clean → Quality → Load → Verify)
- **Series Execution** within each DAG for sequential operations
- **Parallel Execution** across multiple DAGs simultaneously
- **Multi-DAG Dependencies** using ExternalTaskSensor and TriggerDagRunOperator
- **Config-Driven + Code-Based** approach (mix of both)

### 1.3 Success Criteria
- ✅ All 32 tasks (T0001-T0032) completed
- ✅ Master DAG orchestrates 5 source DAGs in parallel
- ✅ Each source DAG uses task groups for organization
- ✅ Data quality ensured (duplicates removed, types validated, missing data handled)
- ✅ Customers: birthdays standardized (DD-MM-YYYY), emails validated, ages filled with mean
- ✅ Sales: dates standardized (DD-MM-YYYY), prices converted to USD, delivery status added
- ✅ Error handling with rejected_records table
- ✅ Execution summary and monitoring

---

## 2. Architecture Overview

### 2.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  Master Orchestrator DAG                    │
│                   (Runs daily at 2:00 AM)                   │
└─────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┴───────────────────┐
        │                                       │
        ▼                                       ▼
   Setup Phases                       Pipeline Execution
   (SERIES)                           (PARALLEL)
        │                                       │
        ├── Phase 1: Foundation                ├── customers_etl_dag
        ├── Phase 2: Data Quality              ├── sales_etl_dag
        ├── Phase 3: Transformations           ├── products_etl_dag
        ├── Phase 4: Loading                   ├── stores_etl_dag
        ├── Phase 5: Orchestration             └── exchange_rates_etl_dag
        │                                       │
        └──────────────────┬────────────────────┘
                           │
                           ▼
                    Monitoring & Summary
                    (SERIES)
```

### 2.2 Individual Source DAG Structure

Each source DAG follows this pattern with **Task Groups**:

```
source_etl_dag (e.g., customers_etl_dag)
│
├─ TaskGroup: EXTRACT
│  ├─ extract_data (PythonOperator)
│  └─ validate_raw_data (PythonOperator)
│  [SERIES execution]
│
├─ TaskGroup: CLEANING
│  ├─ handle_duplicates
│  ├─ standardize_dates/formats
│  ├─ typecast_columns
│  ├─ fill_missing_values
│  └─ validate_data_quality
│  [SERIES execution]
│
├─ TaskGroup: TRANSFORMATION
│  ├─ apply_transformations
│  ├─ create_derived_columns
│  └─ aggregate_data (if needed)
│  [SERIES or PARALLEL depending on independence]
│
├─ TaskGroup: QUALITY_CHECK
│  ├─ verify_no_duplicates
│  ├─ verify_format_compliance
│  ├─ verify_completeness
│  └─ verify_business_rules
│  [PARALLEL execution for independent checks]
│
├─ TaskGroup: LOAD
│  ├─ load_to_output_table (upsert/bulk)
│  └─ log_rejected_records
│  [SERIES execution]
│
└─ TaskGroup: VERIFY
   ├─ verify_record_count
   ├─ verify_data_integrity
   └─ generate_load_report
   [SERIES execution]

Execution Flow:
EXTRACT >> CLEANING >> TRANSFORMATION >> QUALITY_CHECK >> LOAD >> VERIFY
```

---

## 3. Complete Task Breakdown (T0001-T0032)

### Phase 1: Foundation & Setup (T0001-T0007) - 3 hours

#### T0001: Environment Setup & Pipeline Design
**Description:** Configure Airflow environment and define ETL pipeline architecture  
**Deliverables:**
- Verify Airflow 2.8.3+ running on Docker
- Verify PostgreSQL 15 backend connected
- Create architecture diagram
- Document DAG flow structure

**Input:** Existing Docker Compose setup  
**Output:** Confirmed working environment  
**Dependencies:** None  
**Estimated Time:** 30 minutes

---

#### T0002: Install Airflow, Design Data Models, Set Up Extraction Scripts
**Description:** Complete database schema design for all tables  
**Deliverables:**
- Create database schema (14 tables)
  - 5 input tables (raw data)
  - 5 output tables (cleaned data)
  - 2 summary tables (aggregations)
  - 2 metadata tables (rejected_records, pipeline_runs)
- Design data models using Pydantic
- Set up initial extraction framework

**Input:** Data source specifications  
**Output:** Database schema created, data models defined  
**Dependencies:** T0001  
**Estimated Time:** 45 minutes

**Database Tables:**
```sql
-- Input Tables (Raw Data)
CREATE TABLE input_customers (...);
CREATE TABLE input_sales (...);
CREATE TABLE input_products (...);
CREATE TABLE input_stores (...);
CREATE TABLE input_exchange_rates (...);

-- Output Tables (Cleaned Data)
CREATE TABLE output_customers_cleaned (...);
CREATE TABLE output_sales_cleaned (
    ...,
    price_usd DECIMAL(10,2),      -- Converted to USD
    total_amount DECIMAL(10,2),   -- price_usd * quantity
    delivery_status VARCHAR(20)   -- delivered/pending/lost
);
CREATE TABLE output_products_cleaned (...);
CREATE TABLE output_stores_cleaned (...);
CREATE TABLE output_exchange_rates_cleaned (...);

-- Summary Tables
CREATE TABLE daily_sales_summary (
    date DATE,
    total_sales DECIMAL(15,2),
    order_count INT,
    avg_order_value DECIMAL(10,2)
);
CREATE TABLE product_performance (...);

-- Metadata Tables
CREATE TABLE rejected_records (
    id SERIAL PRIMARY KEY,
    source_table VARCHAR(100),
    record_id VARCHAR(255),
    error_type VARCHAR(50),
    error_details TEXT,
    rejected_data JSONB,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE pipeline_runs (
    run_id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100),
    run_date DATE,
    status VARCHAR(20),
    records_processed INT,
    records_rejected INT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INT
);
```

---

#### T0003: Create Python Virtual Environment
**Description:** Set up venv for portability (works on any PC)  
**Deliverables:**
- Create venv in project directory
- Activate venv
- Create requirements.txt

**Input:** Python 3.12+  
**Output:** Activated venv  
**Dependencies:** T0001  
**Estimated Time:** 10 minutes

**Commands:**
```bash
cd D:\sam\Projects\Infosys\Airflow
python -m venv venv
venv\Scripts\activate
```

---

#### T0004: Folder Structure for ETL Pipeline Framework
**Description:** Organize project directories  
**Deliverables:**
```
D:\sam\Projects\Infosys\Airflow\
├── dags/                          # Airflow DAG files
│   ├── master_orchestrator_dag.py
│   ├── customers_etl_dag.py
│   ├── sales_etl_dag.py
│   ├── products_etl_dag.py
│   ├── stores_etl_dag.py
│   └── exchange_rates_etl_dag.py
│
├── scripts/                        # ETL utilities
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── cleaning_utils.py
│   │   ├── validation_utils.py
│   │   ├── transformation_utils.py
│   │   ├── load_utils.py
│   │   ├── config_loader.py
│   │   └── db_utils.py
│   ├── extract.py
│   ├── clean.py
│   ├── transform.py
│   └── load.py
│
├── config/                        # Configuration files
│   ├── customers_config.yaml
│   ├── sales_config.yaml
│   ├── products_config.yaml
│   ├── stores_config.yaml
│   ├── exchange_rates_config.yaml
│   ├── cleaning_rules.yaml
│   └── transformation_rules.yaml
│
├── data/
│   ├── raw/                       # Raw input files
│   ├── staging/                   # Temporary staging
│   └── processed/                 # Processed output
│
├── data_models/                   # Pydantic models
│   ├── __init__.py
│   ├── customer_model.py
│   ├── sales_model.py
│   └── shared_models.py
│
├── tests/                         # Unit tests
│   ├── test_cleaning.py
│   ├── test_validation.py
│   └── test_loading.py
│
├── docs/                          # Documentation
│   ├── PRD_ETL_FRAMEWORK.md
│   ├── DAG_FLOW_STRUCTURE.md
│   └── Implementation_Guide.md
│
├── venv/                          # Virtual environment
├── requirements.txt               # Dependencies
└── README.md                      # Project overview
```

**Input:** Project requirements  
**Output:** Complete folder structure  
**Dependencies:** T0001  
**Estimated Time:** 15 minutes

---

#### T0005: Install Core Libraries
**Description:** Install essential Python libraries  
**Deliverables:**
- requirements.txt with all dependencies
- Install all packages in venv

**Requirements:**
```txt
# Core ETL Libraries
pandas==2.1.4
pydantic==2.5.3
sqlalchemy==2.0.25
psycopg2-binary==2.9.9

# Airflow
apache-airflow==2.8.3

# Configuration
pyyaml==6.0.1

# Utilities
python-dateutil==2.8.2
pytz==2023.3

# Testing
pytest==7.4.4
pytest-cov==4.1.0
```

**Input:** requirements.txt  
**Output:** Installed packages  
**Dependencies:** T0003  
**Estimated Time:** 10 minutes

---

#### T0006: Build Starter Config Loader
**Description:** Create utility to load YAML configs  
**Deliverables:**
- config_loader.py module
- Support for YAML and JSON
- Config validation

**Code Structure:**
```python
# scripts/utils/config_loader.py
class ConfigLoader:
    def load_yaml(self, path: str) -> dict
    def load_json(self, path: str) -> dict
    def validate_config(self, config: dict, schema: dict) -> bool
```

**Input:** Config file specifications  
**Output:** ConfigLoader class  
**Dependencies:** T0005  
**Estimated Time:** 30 minutes

---

#### T0007: Implement Demo Script to Read/Write CSVs
**Description:** Basic CSV operations demo  
**Deliverables:**
- demo_csv_operations.py
- Read/write CSV with pandas
- Handle encoding, delimiters, quotes

**Input:** Sample CSV files  
**Output:** Working demo script  
**Dependencies:** T0005  
**Estimated Time:** 20 minutes

---

### Phase 2: Data Quality & Cleaning (T0008-T0012) - 4 hours

#### T0008: Build Reusable Cleaning Utilities
**Description:** Core cleaning functions  
**Deliverables:**
- cleaning_utils.py module with functions:
  - trim_whitespace()
  - fill_missing_values()
  - typecast_columns()
  - standardize_formats()
  - remove_special_chars()

**Input:** Raw DataFrame  
**Output:** Cleaned DataFrame  
**Dependencies:** T0005  
**Estimated Time:** 45 minutes

---

#### T0009: Handle Incorrect Data Types
**Description:** Type detection and correction  
**Deliverables:**
- Type validation logic
- Safe type conversion with error handling
- Logging of type corrections

**Input:** DataFrame with type issues  
**Output:** Type-corrected DataFrame  
**Dependencies:** T0008  
**Estimated Time:** 30 minutes

---

#### T0010: Duplicate Data Detection & Removal
**Description:** Deduplication logic  
**Deliverables:**
- duplicate_handler.py module
- Configurable duplicate detection rules
- Keep first/last/newest strategies
- Log duplicates to rejected_records

**Input:** DataFrame with potential duplicates  
**Output:** Deduplicated DataFrame  
**Dependencies:** T0008  
**Estimated Time:** 40 minutes

---

#### T0011: Missing Data Handling Strategies
**Description:** Multiple strategies for missing values  
**Deliverables:**
- missing_data_handler.py module
- Mean/median imputation
- Forward/backward fill
- Regression-based imputation (optional)
- Drop rows/columns strategy
- Configurable per-column strategies

**Input:** DataFrame with nulls  
**Output:** Completed DataFrame  
**Dependencies:** T0008  
**Estimated Time:** 50 minutes

---

#### T0012: Build Config-Driven Cleaning Rules
**Description:** YAML-based cleaning rule engine  
**Deliverables:**
- cleaning_rules_engine.py
- YAML config format for cleaning rules
- Apply rules from config to any dataset

**Example Config:**
```yaml
# config/customers_config.yaml
cleaning_rules:
  duplicates:
    enabled: true
    subset: ['customer_id']
    keep: 'first'
  
  columns:
    birthday:
      - standardize_date: 'DD-MM-YYYY'
    
    age:
      - typecast: 'int'
      - fill_missing: 'mean'
    
    email:
      - validate_email
      - fill_invalid: 'noemail@company.com'
```

**Input:** Cleaning requirements  
**Output:** Rule engine + configs  
**Dependencies:** T0006, T0008-T0011  
**Estimated Time:** 60 minutes

---

### Phase 3: Transformation Logic (T0013-T0017) - 4 hours

#### T0013: Aggregations (groupBy, sum, min, max)
**Description:** Build aggregation utilities  
**Deliverables:**
- aggregation_utils.py module
- GroupBy operations
- Sum, min, max, avg, count functions
- Config-driven aggregation rules

**Example:**
```python
# Daily sales summary
daily_summary = aggregate(
    df=sales_df,
    groupby=['date'],
    metrics={'total_sales': 'sum', 'orders': 'count', 'avg_value': 'mean'}
)
```

**Input:** Cleaned DataFrame  
**Output:** Aggregation module  
**Dependencies:** T0012  
**Estimated Time:** 40 minutes

---

#### T0014: Normalization & Scaling
**Description:** Data normalization techniques  
**Deliverables:**
- normalization_utils.py module
- Min-max scaling
- Z-score standardization
- Robust scaling

**Input:** Numeric columns  
**Output:** Normalized DataFrame  
**Dependencies:** T0012  
**Estimated Time:** 35 minutes

---

#### T0015: Feature Engineering Logic
**Description:** Derive new features  
**Deliverables:**
- feature_engineering.py module
- Derived column creation
- Mathematical operations
- Categorical encoding
- Binning

**Example:**
```python
# For sales: total_amount = price_usd * quantity
# For sales: delivery_status based on dates
```

**Input:** Base DataFrame  
**Output:** Enhanced DataFrame  
**Dependencies:** T0013  
**Estimated Time:** 50 minutes

---

#### T0016: Date/Time Transformations
**Description:** Date parsing and transformations  
**Deliverables:**
- datetime_utils.py module
- Parse multiple date formats
- Convert to DD-MM-YYYY
- Extract date components
- Calculate date differences

**Input:** Date columns in various formats  
**Output:** Standardized datetime columns  
**Dependencies:** T0008  
**Estimated Time:** 40 minutes

---

#### T0017: Config-Based Transformation Rules
**Description:** Transformation rule engine  
**Deliverables:**
- transformation_engine.py
- YAML config format
- Apply transformations from config

**Example Config:**
```yaml
# config/sales_config.yaml
transformations:
  derived_columns:
    - name: price_usd
      formula: 'cost * exchange_rate'
      lookup_table: 'exchange_rates'
      lookup_key: 'currency_code'
    
    - name: total_amount
      formula: 'price_usd * quantity'
    
    - name: delivery_status
      conditions:
        - if: 'date_delivered > order_date'
          then: 'delivered'
        - if: 'date_delivered is NULL and order_date < today'
          then: 'lost'
        - else: 'pending'
  
  aggregations:
    - table: 'daily_sales_summary'
      groupby: ['date']
      metrics:
        total_sales: 'sum:total_amount'
        order_count: 'count:order_id'
        avg_order_value: 'mean:total_amount'
```

**Input:** Transformation requirements  
**Output:** Rule engine + configs  
**Dependencies:** T0013-T0016  
**Estimated Time:** 60 minutes

---

### Phase 4: Loading Strategies (T0018-T0022) - 4 hours

#### T0018: Bulk Load Operations
**Description:** Efficient bulk inserts  
**Deliverables:**
- bulk_loader.py module
- Batch insert operations
- Transaction management
- Performance optimization

**Input:** Cleaned DataFrame  
**Output:** Data loaded to database  
**Dependencies:** T0002  
**Estimated Time:** 45 minutes

---

#### T0019: Incremental vs Full Loads
**Description:** Support both load types  
**Deliverables:**
- load_strategy.py module
- Full load (truncate + load)
- Incremental load (append new)
- CDC with watermarks

**Input:** DataFrame + load type  
**Output:** Loaded data  
**Dependencies:** T0018  
**Estimated Time:** 50 minutes

---

#### T0020: Handling Constraint Violations
**Description:** Handle database constraints  
**Deliverables:**
- constraint_handler.py module
- Detect violations
- Log to rejected_records
- Skip or retry logic

**Input:** Data with potential violations  
**Output:** Clean load with logged errors  
**Dependencies:** T0018  
**Estimated Time:** 40 minutes

---

#### T0021: Upsert Logic
**Description:** Insert-or-update operations  
**Deliverables:**
- upsert_handler.py module
- PostgreSQL ON CONFLICT DO UPDATE
- Configurable merge keys
- Update timestamp tracking

**Input:** DataFrame with updates  
**Output:** Data upserted  
**Dependencies:** T0018  
**Estimated Time:** 45 minutes

---

#### T0022: Error Table Creation (Rejects)
**Description:** Create and manage rejected_records table  
**Deliverables:**
- rejected_records table
- Logging utilities
- Query utilities to analyze rejections

**Input:** Database connection  
**Output:** Error tracking table  
**Dependencies:** T0002  
**Estimated Time:** 30 minutes

---

### Phase 5: Orchestration & DAG Management (T0023-T0027) - 5 hours

#### T0023: Build Master DAG to Trigger All Pipelines
**Description:** Create master orchestrator  
**Deliverables:**
- master_orchestrator_dag.py
- TriggerDagRunOperator for each child DAG
- Execution order management
- Status monitoring

**DAG Structure:**
```python
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator

with DAG('master_orchestrator_dag', ...) as dag:
    
    start = PythonOperator(task_id='start', ...)
    
    # Trigger all source DAGs in PARALLEL
    trigger_customers = TriggerDagRunOperator(
        task_id='trigger_customers_etl',
        trigger_dag_id='customers_etl_dag',
        wait_for_completion=True
    )
    
    trigger_sales = TriggerDagRunOperator(
        task_id='trigger_sales_etl',
        trigger_dag_id='sales_etl_dag',
        wait_for_completion=True
    )
    
    # ... more triggers ...
    
    end = PythonOperator(task_id='generate_summary', ...)
    
    # Execute in parallel
    start >> [trigger_customers, trigger_sales, ...] >> end
```

**Input:** Child DAG configurations  
**Output:** Master DAG  
**Dependencies:** T0002  
**Estimated Time:** 60 minutes

---

#### T0024: Event-Driven DAG Triggering
**Description:** File/event-based triggering  
**Deliverables:**
- FileSensor tasks
- ExternalTaskSensor for dependencies
- API endpoint triggers

**Input:** Event triggers  
**Output:** Event-driven execution  
**Dependencies:** T0023  
**Estimated Time:** 45 minutes

---

#### T0025: Multi-DAG Dependency Management
**Description:** Define DAG dependencies  
**Deliverables:**
- Dependency configuration in YAML
- ExternalTaskSensor implementation
- Dependency graph visualization

**Input:** DAG dependency graph  
**Output:** Enforced dependencies  
**Dependencies:** T0023  
**Estimated Time:** 50 minutes

---

#### T0026: Backfill & Catchup Features
**Description:** Historical data processing  
**Deliverables:**
- DAG catchup configuration
- Backfill command scripts
- Date range parameterization

**Input:** Date range  
**Output:** Historical data loaded  
**Dependencies:** T0023  
**Estimated Time:** 40 minutes

---

#### T0027: DAG Failure Handling Strategy
**Description:** Comprehensive failure handling  
**Deliverables:**
- Retry policies
- Email alerts on failure
- on_failure_callback functions
- Recovery procedures

**Input:** DAG failures  
**Output:** Alerts and recovery  
**Dependencies:** T0023  
**Estimated Time:** 45 minutes

---

### Phase 6: Integration & End-to-End (T0028-T0032) - 6.5 hours

#### T0028: Combine Ingestion → Cleaning → Validation → Transform → Load
**Description:** Build complete pipeline  
**Deliverables:**
- Complete ETL DAG template
- Extract → Clean → Transform → Load flow
- Quality gates between stages

**Input:** Raw data  
**Output:** End-to-end pipeline  
**Dependencies:** T0007-T0022  
**Estimated Time:** 90 minutes

---

#### T0029: Multi-Source Data Pipelines
**Description:** Create all 5 source DAGs  
**Deliverables:**
- customers_etl_dag.py
- sales_etl_dag.py
- products_etl_dag.py
- stores_etl_dag.py
- exchange_rates_etl_dag.py

**Input:** 5 data sources  
**Output:** 5 working DAGs  
**Dependencies:** T0028  
**Estimated Time:** 120 minutes

---

#### T0030: Build Reusable Pipeline Config
**Description:** Config template for new sources  
**Deliverables:**
- pipeline_config_template.yaml
- Config validation schema
- Pipeline generator script

**Input:** Pipeline requirements  
**Output:** Reusable template  
**Dependencies:** T0028  
**Estimated Time:** 45 minutes

---

#### T0031: Pipeline Execution Summary
**Description:** Build summary report  
**Deliverables:**
- Execution summary script
- Metrics dashboard queries
- Daily/weekly reports

**Input:** pipeline_runs table  
**Output:** Execution summary  
**Dependencies:** T0002, T0028  
**Estimated Time:** 60 minutes

---

#### T0032: Error Recovery Workflow
**Description:** Reprocess failed records  
**Deliverables:**
- error_recovery_dag.py
- Query rejected records
- Reprocessing logic
- Success tracking

**Input:** rejected_records table  
**Output:** Recovery workflow  
**Dependencies:** T0022, T0027  
**Estimated Time:** 70 minutes

---

## 4. Data Source Specific Requirements

### 4.1 Customers ETL (customers_etl_dag.py)

**Source:** input_customers table  
**Destination:** output_customers_cleaned table

**Cleaning Operations:**
1. **Delete Duplicates** - Remove all duplicate rows (keep first)
   - Duplicate detection on: customer_id
   - Strategy: Keep first occurrence

2. **Standardize Birthday** - Convert to DD-MM-YYYY format
   - Parse multiple input formats
   - Output: DD-MM-YYYY

3. **Typecast Age** - Convert to INT
   - Handle non-numeric values
   - Log conversion failures

4. **Fill Missing Ages** - Use mean age
   - Calculate mean age from valid records
   - Apply to missing values

5. **Validate Emails** - Regex validation
   - Pattern: ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$
   - Replace invalid/missing: "noemail@company.com"

**Task Group Flow:**
```
EXTRACT → CLEANING → QUALITY_CHECK → LOAD → VERIFY
```

**Expected Output Columns:**
```
customer_id, name, birthday (DD-MM-YYYY), age (INT), email (validated), address, ...
```

---

### 4.2 Sales ETL (sales_etl_dag.py)

**Source:** input_sales table  
**Destination:** output_sales_cleaned table + daily_sales_summary table  
**Lookup:** exchange_rates table (for USD conversion)

**Cleaning & Transformation Operations:**
1. **Standardize Dates** - Convert to DD-MM-YYYY format
   - order_date → DD-MM-YYYY
   - date_delivered → DD-MM-YYYY

2. **Convert Prices to USD** - Use exchange_rates table
   - Fetch exchange_rate based on currency_code and date
   - Create column: price_usd = cost * exchange_rate
   - Handle multiple currencies

3. **Add Delivery Status** - New column based on logic
   - If date_delivered > order_date: 'delivered'
   - If date_delivered is NULL and order_date < today: 'lost'
   - Else: 'pending'

4. **Calculate Total Amount** - Derived column
   - total_amount = price_usd * quantity

5. **Normalize Amounts** (Optional) - Min-max scaling
   - Apply to total_amount if needed for analysis

6. **Create Daily Summary** - Aggregation
   - Table: daily_sales_summary
   - GroupBy: date
   - Metrics:
     - total_sales = SUM(total_amount)
     - order_count = COUNT(order_id)
     - avg_order_value = AVG(total_amount)

**Task Group Flow:**
```
EXTRACT 
  → CLEANING (dates, USD conversion, delivery_status, total_amount)
  → TRANSFORMATION (normalize, aggregate)
  → QUALITY_CHECK
  → LOAD (output_sales_cleaned + daily_sales_summary)
  → VERIFY
```

**Expected Output Columns (output_sales_cleaned):**
```
order_id, customer_id, product_id, order_date (DD-MM-YYYY), 
date_delivered (DD-MM-YYYY), quantity, cost (original), 
currency_code, price_usd (converted), total_amount (calculated), 
delivery_status (derived), ...
```

**Expected Output Columns (daily_sales_summary):**
```
date (DD-MM-YYYY), total_sales, order_count, avg_order_value
```

---

### 4.3 Products, Stores, Exchange Rates ETL

**Similar structure with task groups:**
- EXTRACT
- CLEANING
- TRANSFORMATION (if needed)
- QUALITY_CHECK
- LOAD
- VERIFY

**Specific requirements TBD based on data analysis**

---

## 5. DAG Flow & Execution Patterns

### 5.1 Master Orchestrator DAG

```python
# master_orchestrator_dag.py

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 11),
    'email': ['admin@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'master_orchestrator_dag',
    default_args=default_args,
    description='Master DAG to orchestrate all ETL pipelines',
    schedule_interval='0 2 * * *',  # Daily at 2:00 AM
    catchup=False,
    tags=['master', 'orchestrator']
) as dag:
    
    start_task = PythonOperator(
        task_id='start_pipeline',
        python_callable=lambda: print("Starting ETL Pipeline")
    )
    
    ###########  T0023: Trigger Exchange Rates DAG (PHASE 1) #############
    trigger_exchange = TriggerDagRunOperator(
        task_id='trigger_exchange_rates_etl',
        trigger_dag_id='exchange_rates_etl_dag',
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed']
    )
    
    ###########  T0023: Trigger Stores DAG (PHASE 2a - depends on exchange) #############
    trigger_stores = TriggerDagRunOperator(
        task_id='trigger_stores_etl',
        trigger_dag_id='stores_etl_dag',
        wait_for_completion=True,
        poke_interval=30
    )
    
    ###########  T0023: Trigger Products DAG (PHASE 2b - depends on exchange, parallel with stores) #############
    trigger_products = TriggerDagRunOperator(
        task_id='trigger_products_etl',
        trigger_dag_id='products_etl_dag',
        wait_for_completion=True
    )
    
    ###########  T0023: Trigger Customers DAG (PHASE 3 - independent) #############
    trigger_customers = TriggerDagRunOperator(
        task_id='trigger_customers_etl',
        trigger_dag_id='customers_etl_dag',
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed']
    )
    
    ###########  T0023: Trigger Sales DAG (PHASE 4 - depends on all above) #############
    trigger_sales = TriggerDagRunOperator(
        task_id='trigger_sales_etl',
        trigger_dag_id='sales_etl_dag',
        wait_for_completion=True,
        poke_interval=30
    )
    
    ###########  T0031: Generate execution summary #############
    generate_summary = PythonOperator(
        task_id='generate_execution_summary',
        python_callable=lambda: print("Generate Summary")
    )
    
    ###########  T0023: End of Master Orchestration #############
    end_task = PythonOperator(
        task_id='end_pipeline',
        python_callable=lambda: print("Pipeline Complete")
    )
    
    ###########  T0023: EXECUTION DEPENDENCY FLOW #############
    # START -> exchange_rates -> [stores, products, customers] -> sales -> summary -> END
    start_task >> trigger_exchange >> [trigger_stores, trigger_products, trigger_customers] >> trigger_sales >> generate_summary >> end_task
```

**Execution Pattern:**
- START → All 5 DAGs in PARALLEL → SUMMARY → END
- Total time depends on slowest DAG

---

### 5.2 Individual Source DAG Template

```python
# customers_etl_dag.py (example)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 11),
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    'customers_etl_dag',
    default_args=default_args,
    description='ETL pipeline for customers data',
    schedule_interval=None,  # Triggered by master DAG
    catchup=False,
    tags=['etl', 'customers']
) as dag:
    
    # EXTRACT Task Group
    with TaskGroup('extract', tooltip='Extract raw data') as extract_group:
        ###########  T0028: Extract Customers Data #############
        extract_data = PythonOperator(
            task_id='extract_customers_data',
            python_callable=extract_customers_func
        )
        
        ###########  T0028: Validate Raw Data #############
        validate_raw = PythonOperator(
            task_id='validate_raw_data',
            python_callable=validate_raw_func
        )
        
        extract_data >> validate_raw
    
    # CLEANING Task Group
    with TaskGroup('cleaning', tooltip='Data cleaning operations') as cleaning_group:
        ###########  T0010: Remove Duplicates #############
        remove_duplicates = PythonOperator(
            task_id='remove_duplicates',
            python_callable=remove_duplicates_func
        )
        
        ###########  T0016: Standardize Birthday to DD-MM-YYYY #############
        standardize_birthday = PythonOperator(
            task_id='standardize_birthday',
            python_callable=standardize_birthday_func
        )
        
        ###########  T0009: Typecast Age to INT #############
        typecast_age = PythonOperator(
            task_id='typecast_age',
            python_callable=typecast_age_func
        )
        
        ###########  T0011: Fill Missing Ages with Mean #############
        fill_missing_ages = PythonOperator(
            task_id='fill_missing_ages',
            python_callable=fill_missing_ages_func
        )
        
        ###########  T0008: Validate & Standardize Emails #############
        validate_emails = PythonOperator(
            task_id='validate_emails',
            python_callable=validate_emails_func
        )
        
        # SERIES execution within cleaning group
        remove_duplicates >> standardize_birthday >> typecast_age >> fill_missing_ages >> validate_emails
    
    # QUALITY CHECK Task Group
    with TaskGroup('quality_check', tooltip='Data quality validation') as quality_group:
        ###########  T0012: Verify No Duplicates #############
        verify_no_dupes = PythonOperator(
            task_id='verify_no_duplicates',
            python_callable=verify_no_dupes_func
        )
        
        ###########  T0012: Verify Date Format #############
        verify_dates = PythonOperator(
            task_id='verify_date_format',
            python_callable=verify_dates_func
        )
        
        ###########  T0012: Verify Email Validity #############
        verify_emails = PythonOperator(
            task_id='verify_email_validity',
            python_callable=verify_emails_func
        )
        
        # PARALLEL execution (independent checks)
        [verify_no_dupes, verify_dates, verify_emails]
    
    # LOAD Task Group
    with TaskGroup('load', tooltip='Load to database') as load_group:
        ###########  T0021: Load to Output Table (Upsert) #############
        load_to_db = PythonOperator(
            task_id='load_to_output_table',
            python_callable=load_to_db_func
        )
        
        ###########  T0022: Log Rejected Records #############
        log_rejects = PythonOperator(
            task_id='log_rejected_records',
            python_callable=log_rejects_func
        )
        
        load_to_db >> log_rejects
    
    # VERIFY Task Group
    with TaskGroup('verify', tooltip='Verification checks') as verify_group:
        ###########  T0028: Verify Record Count #############
        verify_count = PythonOperator(
            task_id='verify_record_count',
            python_callable=verify_count_func
        )
        
        ###########  T0028: Verify Data Integrity #############
        verify_integrity = PythonOperator(
            task_id='verify_data_integrity',
            python_callable=verify_integrity_func
        )
        
        ###########  T0031: Generate Load Report #############
        generate_report = PythonOperator(
            task_id='generate_load_report',
            python_callable=generate_report_func
        )
        
        verify_count >> verify_integrity >> generate_report
    
    # Connect Task Groups in SERIES
    extract_group >> cleaning_group >> quality_group >> load_group >> verify_group
```

**Execution Pattern:**
- Each task group executes in SERIES
- Within groups:
  - Most tasks in SERIES (dependencies)
  - Quality checks in PARALLEL (independent)

---

## 5.5 Multi-DAG Dependency Architecture with ExternalTaskSensor (T0025)

### Complete Execution Flow Diagram:

```
MASTER ORCHESTRATOR DAG (T0023)
           |
    ┌──────┼──────┐
    |      |      |
    v      v      v
[Exchange][Stores]  [Customers]
 Rates    Products  (Independent)
  (T0029a)  (T0029b,c)  (T0029d)
    |      |      |
    └──────┼──────┘
           |
           v
      [Sales]  (T0029e - waits for all above)
```

### 5.5a Dependency Matrix:

| Source DAG | Execution Order | External Dependencies | Can Parallel With | ExternalTaskSensor Target |
|-----------|-----------------|----------------------|-----------------|---------------------------|
| exchange_rates_etl_dag | 1st (FIRST) | NONE | NONE | N/A |
| stores_etl_dag | 2nd | exchange_rates complete | products_etl | exchange_rates_etl_dag → verify.generate_load_report |
| products_etl_dag | 2nd | exchange_rates complete | stores_etl | exchange_rates_etl_dag → verify.generate_load_report |
| customers_etl_dag | 3rd | NONE | stores + products | N/A |
| sales_etl_dag | 4th (LAST) | exchange_rates, customers, products complete | NONE | All 4 DAGs → verify.generate_load_report |

### 5.5b ExternalTaskSensor Implementation for sales_etl_dag (T0025):

```python
###########  T0025: Multi-DAG Dependency Management #############

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 11),
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG('sales_etl_dag', default_args=default_args, ...) as dag:
    
    ###########  T0025: Wait for Exchange Rates DAG to complete #############
    wait_exchange_rates = ExternalTaskSensor(
        task_id='wait_exchange_rates',
        external_dag_id='exchange_rates_etl_dag',
        external_task_id='verify.generate_load_report',
        allowed_states=['success'],
        failed_states=['failed'],
        poke_interval=30,  # Check every 30 seconds
        timeout=3600,      # 1 hour timeout
        mode='poke'
    )
    
    ###########  T0025: Wait for Customers DAG to complete #############
    wait_customers = ExternalTaskSensor(
        task_id='wait_customers',
        external_dag_id='customers_etl_dag',
        external_task_id='verify.generate_load_report',
        allowed_states=['success'],
        failed_states=['failed'],
        poke_interval=30,
        timeout=3600
    )
    
    ###########  T0025: Wait for Products DAG to complete #############
    wait_products = ExternalTaskSensor(
        task_id='wait_products',
        external_dag_id='products_etl_dag',
        external_task_id='verify.generate_load_report',
        allowed_states=['success'],
        failed_states=['failed'],
        poke_interval=30,
        timeout=3600
    )
    
    ###########  T0028: Start Sales ETL after all dependencies met #############
    start_sales = PythonOperator(
        task_id='start_sales_etl',
        python_callable=lambda: print("All dependencies satisfied, starting sales ETL")
    )
    
    # ALL upstream DAGs MUST complete before sales ETL begins
    [wait_exchange_rates, wait_customers, wait_products] >> start_sales
```

---

## 6. Configuration Examples

### 6.1 Customers Config

```yaml
###########  T0012: Customers Cleaning Rules Config #############
# config/customers_config.yaml

source:
  type: 'postgres'
  table: 'input_customers'

destination:
  type: 'postgres'
  table: 'output_customers_cleaned'
  mode: 'upsert'  # insert or update
  merge_keys: ['customer_id']

cleaning_rules:
  duplicates:
    enabled: true
    subset: ['customer_id']
    keep: 'first'
    log_to_rejects: true
  
  date_columns:
    birthday:
      input_formats: ['%Y-%m-%d', '%d/%m/%Y', '%m-%d-%Y']
      output_format: '%d-%m-%Y'  # DD-MM-YYYY
      handle_invalid: 'reject'
  
  type_conversions:
    age:
      target_type: 'int'
      handle_errors: 'reject'
  
  missing_values:
    age:
      strategy: 'mean'
      scope: 'global'  # calculate mean across all records
  
  validations:
    email:
      pattern: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
      handle_invalid: 'replace'
      replacement_value: 'noemail@company.com'

quality_checks:
  - check: 'no_duplicates'
    column: 'customer_id'
  - check: 'date_format'
    column: 'birthday'
    format: '%d-%m-%Y'
  - check: 'type_match'
    column: 'age'
    expected_type: 'int'
  - check: 'email_validity'
    column: 'email'
```

---

### 6.2 Sales Config

```yaml
###########  T0017: Sales Transformation Rules Config #############
# config/sales_config.yaml

source:
  type: 'postgres'
  table: 'input_sales'

destination:
  - type: 'postgres'
    table: 'output_sales_cleaned'
    mode: 'upsert'
    merge_keys: ['order_id']
  
  - type: 'postgres'
    table: 'daily_sales_summary'
    mode: 'bulk'

lookups:
  exchange_rates:
    table: 'exchange_rates'
    key: 'currency_code'
    value: 'exchange_rate'
    date_column: 'date'

cleaning_rules:
  date_columns:
    order_date:
      input_formats: ['%Y-%m-%d', '%d/%m/%Y']
      output_format: '%d-%m-%Y'
    
    date_delivered:
      input_formats: ['%Y-%m-%d', '%d/%m/%Y']
      output_format: '%d-%m-%Y'
      allow_null: true

transformations:
  derived_columns:
    - name: 'price_usd'
      formula: 'cost * exchange_rate'
      lookup:
        table: 'exchange_rates'
        on: 'currency_code'
        select: 'exchange_rate'
        date_match: 'order_date'
    
    - name: 'total_amount'
      formula: 'price_usd * quantity'
    
    - name: 'delivery_status'
      conditions:
        - if: 'date_delivered > order_date'
          then: 'delivered'
        - if: 'date_delivered is NULL and order_date < today()'
          then: 'lost'
        - else: 'pending'
  
  aggregations:
    daily_sales_summary:
      groupby: ['order_date']
      metrics:
        total_sales:
          column: 'total_amount'
          function: 'sum'
        order_count:
          column: 'order_id'
          function: 'count'
        avg_order_value:
          column: 'total_amount'
          function: 'mean'

quality_checks:
  - check: 'date_format'
    columns: ['order_date', 'date_delivered']
  - check: 'positive_values'
    columns: ['price_usd', 'total_amount', 'quantity']
  - check: 'valid_status'
    column: 'delivery_status'
    allowed_values: ['delivered', 'pending', 'lost']
```

---

## 7. Implementation Timeline

### Day 1 (14 hours)

**Morning (6 hours):**
- [x] T0001: Environment verification (0.5h)
- [ ] T0002: Database schema creation (0.75h)
- [ ] T0003: venv setup (0.17h)
- [ ] T0004: Folder structure (0.25h)
- [ ] T0005: Install libraries (0.17h)
- [ ] T0006: Config loader (0.5h)
- [ ] T0007: Demo CSV script (0.33h)
- [ ] T0008: Cleaning utilities (0.75h)
- [ ] T0009: Type handler (0.5h)
- [ ] T0010: Duplicate handler (0.67h)
- [ ] T0011: Missing data handler (0.83h)

**Afternoon (4 hours):**
- [ ] T0012: Cleaning rules engine (1h)
- [ ] T0013: Aggregations (0.67h)
- [ ] T0014: Normalization (0.58h)
- [ ] T0015: Feature engineering (0.83h)
- [ ] T0016: DateTime utils (0.67h)

**Evening (4 hours):**
- [ ] T0017: Transform rules engine (1h)
- [ ] T0018: Bulk loader (0.75h)
- [ ] T0019: Load strategies (0.83h)
- [ ] T0020: Constraint handler (0.67h)
- [ ] T0021: Upsert handler (0.75h)

---

### Day 2 (12.5 hours)

**Morning (5 hours):**
- [ ] T0022: Error tables (0.5h)
- [ ] T0023: Master DAG (1h)
- [ ] T0024: Event triggers (0.75h)
- [ ] T0025: Multi-DAG dependencies (0.83h)
- [ ] T0026: Backfill features (0.67h)
- [ ] T0027: Failure handling (0.75h)
- [ ] T0028: End-to-end pipeline (1.5h)

**Afternoon (5 hours):**
- [ ] T0029: Multi-source DAGs (2h)
  - [ ] customers_etl_dag.py
  - [ ] sales_etl_dag.py
  - [ ] products_etl_dag.py
  - [ ] stores_etl_dag.py
  - [ ] exchange_rates_etl_dag.py
- [ ] T0030: Reusable config (0.75h)
- [ ] T0031: Execution summary (1h)
- [ ] T0032: Error recovery (1.17h)

**Evening (2.5 hours):**
- [ ] Testing (1h)
- [ ] Deployment (0.5h)
- [ ] Verification (0.5h)
- [ ] Documentation (0.5h)

---

## 8. Technology Stack

**Orchestration:**
- Apache Airflow 2.8.3+ (Docker)
- PostgreSQL 15 backend

**Language & Environment:**
- Python 3.12
- venv (virtual environment)

**Core Libraries:**
- pandas 2.1.4 (data manipulation)
- pydantic 2.5.3 (validation)
- sqlalchemy 2.0.25 (database ORM)
- psycopg2-binary 2.9.9 (PostgreSQL driver)
- pyyaml 6.0.1 (config parsing)

**Deployment:**
- Docker Compose
- Windows environment

---

## 9. Success Metrics

### Completeness
- [ ] All 32 tasks completed
- [ ] All 6 DAG files created and working
- [ ] All utilities have unit tests
- [ ] All configs documented

### Functionality
- [ ] Master DAG orchestrates 5 source DAGs
- [ ] Task groups working correctly
- [ ] Series and parallel execution as designed
- [ ] Error handling functional
- [ ] Customers cleaning: duplicates removed, dates standardized, emails validated
- [ ] Sales transformations: USD conversion, delivery status, total amount calculated

### Quality
- [ ] Data quality checks passing
- [ ] Rejected records tracked
- [ ] Execution summary generated
- [ ] Performance acceptable

### Reusability
- [ ] Config-driven approach working
- [ ] New pipeline can be added via config
- [ ] Components reusable across sources

---

## 10. Appendices

### Appendix A: Folder Structure (Detailed)

```
D:\sam\Projects\Infosys\Airflow\
├── dags/
│   ├── master_orchestrator_dag.py
│   ├── customers_etl_dag.py
│   ├── sales_etl_dag.py
│   ├── products_etl_dag.py
│   ├── stores_etl_dag.py
│   └── exchange_rates_etl_dag.py
│
├── scripts/
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── cleaning_utils.py
│   │   ├── validation_utils.py
│   │   ├── transformation_utils.py
│   │   ├── load_utils.py
│   │   ├── config_loader.py
│   │   └── db_utils.py
│   ├── extract.py
│   ├── clean.py
│   ├── transform.py
│   └── load.py
│
├── config/
│   ├── customers_config.yaml
│   ├── sales_config.yaml
│   ├── products_config.yaml
│   ├── stores_config.yaml
│   ├── exchange_rates_config.yaml
│   ├── cleaning_rules.yaml
│   └── transformation_rules.yaml
│
├── data/
│   ├── raw/
│   ├── staging/
│   └── processed/
│
├── data_models/
│   ├── __init__.py
│   ├── customer_model.py
│   ├── sales_model.py
│   └── shared_models.py
│
├── tests/
│   ├── test_cleaning.py
│   ├── test_validation.py
│   └── test_loading.py
│
├── docs/
│   ├── PRD_ETL_FRAMEWORK.md
│   ├── DAG_FLOW_STRUCTURE.md
│   └── Implementation_Guide.md
│
├── venv/
├── requirements.txt
└── README.md
```

### Appendix B: Database Schema (Complete)

```sql
-- Input Tables
CREATE TABLE input_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(200),
    birthday VARCHAR(50),  -- Various formats
    age VARCHAR(10),       -- May be text
    email VARCHAR(200),
    address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE input_sales (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    order_date VARCHAR(50),      -- Various formats
    date_delivered VARCHAR(50),  -- Various formats
    quantity INT,
    cost DECIMAL(10,2),
    currency_code VARCHAR(3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE input_products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(200),
    category VARCHAR(100),
    unit_price DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE input_stores (
    store_id VARCHAR(50) PRIMARY KEY,
    store_name VARCHAR(200),
    location VARCHAR(200),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE input_exchange_rates (
    id SERIAL PRIMARY KEY,
    currency_code VARCHAR(3),
    date DATE,
    exchange_rate DECIMAL(10,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(currency_code, date)
);

-- Output Tables
CREATE TABLE output_customers_cleaned (
    customer_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(200),
    birthday VARCHAR(10),   -- DD-MM-YYYY format
    age INT,                -- Integer type
    email VARCHAR(200),     -- Validated
    address TEXT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE output_sales_cleaned (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    order_date VARCHAR(10),        -- DD-MM-YYYY
    date_delivered VARCHAR(10),    -- DD-MM-YYYY
    quantity INT,
    cost DECIMAL(10,2),            -- Original cost
    currency_code VARCHAR(3),
    price_usd DECIMAL(10,2),       -- Converted to USD
    total_amount DECIMAL(10,2),    -- price_usd * quantity
    delivery_status VARCHAR(20),   -- delivered/pending/lost
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE output_products_cleaned (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(200),
    category VARCHAR(100),
    unit_price DECIMAL(10,2),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE output_stores_cleaned (
    store_id VARCHAR(50) PRIMARY KEY,
    store_name VARCHAR(200),
    location VARCHAR(200),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE output_exchange_rates_cleaned (
    id SERIAL PRIMARY KEY,
    currency_code VARCHAR(3),
    date DATE,
    exchange_rate DECIMAL(10,6),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(currency_code, date)
);

-- Summary Tables
CREATE TABLE daily_sales_summary (
    id SERIAL PRIMARY KEY,
    date VARCHAR(10),           -- DD-MM-YYYY
    total_sales DECIMAL(15,2),
    order_count INT,
    avg_order_value DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date)
);

CREATE TABLE product_performance (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(50),
    period VARCHAR(20),         -- daily/weekly/monthly
    sales_count INT,
    total_revenue DECIMAL(15,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Metadata Tables
CREATE TABLE rejected_records (
    id SERIAL PRIMARY KEY,
    source_table VARCHAR(100),
    record_id VARCHAR(255),
    error_type VARCHAR(50),
    error_details TEXT,
    rejected_data JSONB,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE pipeline_runs (
    run_id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100),
    run_date DATE,
    status VARCHAR(20),
    records_processed INT,
    records_rejected INT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INT,
    error_message TEXT
);
```

---

**Document Status:** APPROVED  
**Implementation Status:** Ready to Begin  
**Last Updated:** January 11, 2026, 1:30 PM  
**Version:** 3.0
