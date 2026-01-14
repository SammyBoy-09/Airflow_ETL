# ================================================================================
# TEAM 1 - ETL PIPELINE PROJECT TRACKER
# ================================================================================
# Last Updated: January 14, 2026
# Purpose: Comprehensive tracking of all tasks, decisions, and progress
# ================================================================================

## 📋 PROJECT OVERVIEW

| Item | Details |
|------|---------|
| **Project Name** | Amazon ETL Pipeline Framework |
| **Team** | TEAM 1 |
| **Total Tasks** | 32 (T0001 - T0032) |
| **Total Phases** | 6 |
| **Technology Stack** | Python 3.11, Airflow 2.8.3, PostgreSQL 15, Docker |
| **Conda Environment** | KB_1978 |
| **Project Root** | D:\sam\Projects\Infosys\Airflow |

---

## 📊 PHASE SUMMARY

| Phase | Tasks | Description | Status |
|-------|-------|-------------|--------|
| **Phase 1** | T0001-T0007 | Environment & Config Setup | ✅ COMPLETE |
| **Phase 2** | T0008-T0012 | Data Quality & Cleaning Utilities | ✅ COMPLETE |
| **Phase 3** | T0013-T0017 | Data Transformation Utilities | ✅ COMPLETE |
| **Phase 4** | T0018-T0022 | Loading Strategies | ✅ COMPLETE |
| **Phase 5** | T0023-T0027 | Orchestration & Scheduling | 🔲 NOT STARTED |
| **Phase 6** | T0028-T0032 | Combined Pipeline & Recovery | 🔲 NOT STARTED |

---

## 📁 OUTPUT DELIVERABLES

### 5 Cleaned Output Tables
| # | Table | Source File | Output File | Key Cleaning Operations |
|---|-------|-------------|-------------|------------------------|
| 1 | **Customers** | Customers.csv | customers_cleaned.csv | Dedupe, birthday→datetime, age→int (mean fill), email validation |
| 2 | **Sales** | Sales.csv | sales_cleaned.csv | Date standardization, Delivery_Status column, Total_Amount_USD (JOIN Products) |
| 3 | **Products** | Products.csv | products_cleaned.csv | Standard cleaning, z-score for numeric columns |
| 4 | **Stores** | Stores.csv | stores_cleaned.csv | Standard cleaning |
| 5 | **Exchange_Rates** | Exchange_Rates.csv | exchange_rates_cleaned.csv | Standard cleaning |

### 9 Reports/Summaries
| # | Report | Description | Output Format |
|---|--------|-------------|---------------|
| 1 | Customer Summary | Demographics, age distribution, location | CSV |
| 2 | Product Performance | Best/worst sellers, category analysis | CSV |
| 3 | Order Status | Delivered/Delivering Soon/To Be Shipped/Lost counts | CSV |
| 4 | Sales Trends (Daily) | Revenue by day/month, growth rates | CSV |
| 5 | Data Quality Scorecard | Completeness %, validity %, issues | CSV |
| 6 | Customer Segmentation | RFM analysis (Recency, Frequency, Monetary) | CSV |
| 7 | Store Performance | Revenue by store, regional analysis | CSV |
| 8 | Anomaly Detection | Outliers, suspicious patterns | CSV |
| 9 | DAG Execution Summary | Pipeline run metrics, task status | CSV + Database |

---

## 🔧 DETAILED TASK TRACKER

### ═══════════════════════════════════════════════════════════════
### PHASE 1: Environment & Configuration Setup (T0001-T0007)
### ═══════════════════════════════════════════════════════════════

| Task ID | Description | Status | File(s) Created |
|---------|-------------|--------|-----------------|
| T0001 | Set up Python environment with dependencies | ✅ | requirements.txt |
| T0002 | Create folder structure (dags, scripts, config, data, logs, tests, plugins) | ✅ | Project folders |
| T0003 | Build YAML config loader utility | ✅ | scripts/config_loader.py |
| T0004 | Create amazon_etl_config.yaml | ✅ | config/amazon_etl_config.yaml |
| T0005 | Create amazon_cleaning_rules.yaml | ✅ | config/amazon_cleaning_rules.yaml |
| T0006 | Build config validation function | ✅ | scripts/config_loader.py |
| T0007 | Add environment-specific config support | ✅ | scripts/config_loader.py |

**Phase 1 Files:**
- `requirements.txt` - Python dependencies
- `scripts/config_loader.py` - YAML config loading and validation
- `config/amazon_etl_config.yaml` - Main ETL configuration
- `config/amazon_cleaning_rules.yaml` - Data cleaning rules

---

### ═══════════════════════════════════════════════════════════════
### PHASE 2: Data Quality & Cleaning Utilities (T0008-T0012)
### ═══════════════════════════════════════════════════════════════

| Task ID | Description | Status | File(s) Created |
|---------|-------------|--------|-----------------|
| T0008 | Build reusable cleaning utilities | ✅ | scripts/cleaning_utils.py, scripts/utils/table_cleaners.py |
| T0009 | Create type casting functions | ✅ | scripts/utils/validation_utils.py |
| T0010 | Build null/missing value handler | ✅ | scripts/utils/duplicate_missing_handler.py |
| T0011 | Create duplicate detection logic | ✅ | scripts/utils/duplicate_missing_handler.py |
| T0012 | Build validation framework | ✅ | scripts/utils/validation_utils.py |

**Phase 2 Files:**
- `scripts/cleaning_utils.py` - Core cleaning utilities
- `scripts/utils/table_cleaners.py` - Table-specific cleaners (Customers, Sales, Products, Stores, Exchange_Rates)
- `scripts/utils/validation_utils.py` - Data validation and type casting
- `scripts/utils/duplicate_missing_handler.py` - Duplicate removal and missing value handling
- `scripts/utils/cleaning_engine.py` - Rule-based cleaning engine

---

### ═══════════════════════════════════════════════════════════════
### PHASE 3: Data Transformation Utilities (T0013-T0017)
### ═══════════════════════════════════════════════════════════════

| Task ID | Description | Status | File(s) Created |
|---------|-------------|--------|-----------------|
| T0013 | Aggregation utilities (sum, avg, count, group) | ✅ | scripts/utils/aggregation_utils.py |
| T0014 | Normalization & scaling (z-score as NEW column) | ✅ | scripts/utils/normalization_utils.py |
| T0015 | Feature engineering (derived columns) | ✅ | scripts/utils/feature_engineering_utils.py |
| T0016 | Date/time transformation utilities | ✅ | scripts/utils/datetime_utils.py |
| T0017 | Transformation orchestrator | ✅ | scripts/utils/transformation_orchestrator.py |

**Phase 3 Files:**
- `scripts/utils/aggregation_utils.py` (382 lines) - GroupBy aggregations, pivot tables, rolling windows
- `scripts/utils/normalization_utils.py` (351+ lines) - Z-score normalization (adds as NEW column)
- `scripts/utils/feature_engineering_utils.py` (417 lines) - Derived columns, calculations
- `scripts/utils/datetime_utils.py` (151 lines) - Date parsing, formatting, calculations
- `scripts/utils/transformation_orchestrator.py` (330 lines) - Pipeline orchestration

---

### ═══════════════════════════════════════════════════════════════
### PHASE 4: Loading Strategies (T0018-T0022) ✅ COMPLETE
### ═══════════════════════════════════════════════════════════════

| Task ID | Description | Status | File(s) Created |
|---------|-------------|--------|------------------|
| T0018 | Implement bulk load utility | ✅ | scripts/utils/bulk_loader.py |
| T0019 | Build incremental load strategy | ✅ | scripts/utils/load_strategy.py |
| T0020 | Create constraint violation handler | ✅ | scripts/utils/constraint_handler.py |
| T0021 | Build upsert (insert/update) logic | ✅ | scripts/utils/upsert_handler.py |
| T0022 | Create rejected_records error table | ✅ | scripts/utils/rejected_records_handler.py |

**Phase 4 Files:**
- `scripts/utils/bulk_loader.py` (~350 lines) - Batch database loading with configurable chunk size
- `scripts/utils/load_strategy.py` (~400 lines) - Full vs Incremental load with primary key comparison
- `scripts/utils/constraint_handler.py` (~370 lines) - Pre-load validation, Option A: Log, Skip, Continue
- `scripts/utils/upsert_handler.py` (~380 lines) - PostgreSQL ON CONFLICT upsert logic
- `scripts/utils/rejected_records_handler.py` (~450 lines) - rejected_records table + tracking

**Phase 4 Key Features:**
- Database: Uses Airflow PostgreSQL container
- Primary Keys: CustomerKey, Order Number, ProductKey, StoreKey, Date (Exchange_Rates)
- Incremental Strategy: Primary key comparison
- Constraint Violations: Option A (Log, Skip, Continue)
- Rejected Records: Full tracking with DAG run ID, error type, raw data

---

### ═══════════════════════════════════════════════════════════════
### PHASE 5: Orchestration & Scheduling (T0023-T0027) 🔲 NOT STARTED
### ═══════════════════════════════════════════════════════════════

| Task ID | Description | Status | File(s) To Create |
|---------|-------------|--------|-------------------|
| T0023 | Build master DAG structure | 🔲 | dags/master_etl_dag.py |
| T0024 | Implement event-based triggers | 🔲 | dags/trigger_utils.py |
| T0025 | Create multi-DAG dependencies | 🔲 | dags/dag_dependencies.py |
| T0026 | Build backfill mechanism | 🔲 | scripts/utils/backfill_handler.py |
| T0027 | Implement failure handling | 🔲 | scripts/utils/failure_handler.py |

---

### ═══════════════════════════════════════════════════════════════
### PHASE 6: Combined Pipeline & Recovery (T0028-T0032) 🔲 NOT STARTED
### ═══════════════════════════════════════════════════════════════

| Task ID | Description | Status | File(s) To Create |
|---------|-------------|--------|-------------------|
| T0028 | Build combined E-T-L pipeline DAG | 🔲 | dags/combined_etl_dag.py |
| T0029 | Create 5 source-specific DAGs | 🔲 | dags/customers_dag.py, dags/sales_dag.py, etc. |
| T0030 | Implement reusable config pattern | 🔲 | config/dag_configs/ |
| T0031 | Build execution summary (DAG tracker) | ✅ | scripts/utils/dag_execution_tracker.py |
| T0032 | Create error recovery mechanism | 🔲 | scripts/utils/error_recovery.py |

---

## 📝 KEY DECISIONS LOG

### Decision 1: Z-Score Implementation
- **Date:** January 14, 2026
- **Decision:** Z-score is added as a **NEW column** (e.g., `Quantity_zscore`)
- **Rationale:** Original values preserved for analysis
- **Implementation:** `normalization_utils.py` updated with `add_as_new_column=True` parameter

### Decision 2: Delivery Status Logic
- **Date:** January 14, 2026
- **Decision:** Delivery status calculated from Order Date and Delivery Date
- **Logic:**
  - Delivery Date exists → "Delivered"
  - No Delivery Date + ≤30 days → "Delivering Soon"
  - No Delivery Date + 31-365 days → "To Be Shipped"
  - No Delivery Date + >365 days → "Lost"
- **Implementation:** `table_cleaners.py` - `SalesCleaner._get_delivery_status()`

### Decision 3: Sales Total Amount
- **Date:** January 14, 2026
- **Decision:** JOIN Sales with Products to calculate `Total_Amount_USD`
- **Formula:** `Quantity × Unit Price USD` (from Products.csv)
- **Implementation:** `table_cleaners.py` - `SalesCleaner.clean()`

### Decision 4: Email Validation
- **Date:** January 14, 2026
- **Decision:** Replace invalid/missing emails with placeholder values
- **Values:**
  - Missing email → `'no-email@unknown.com'`
  - Invalid email → `'invalid-email@unknown.com'`
- **Implementation:** `table_cleaners.py` - `CustomersCleaner.clean()`

### Decision 5: Reports Count
- **Date:** January 14, 2026
- **Decision:** 8 reports + 1 DAG execution summary = 9 total
- **Rationale:** Sufficient for current analysis needs

### Decision 6: Output Storage
- **Date:** January 14, 2026
- **Decision:** All outputs saved as CSV; DAG summary also saved to database
- **Rationale:** CSV for portability, DB for querying execution history

### Decision 7: Phase 4 Database Configuration
- **Date:** January 14, 2026
- **Decision:** Use Airflow PostgreSQL container for data storage
- **Connection:** `postgresql://airflow:airflow@localhost:5432/airflow`
- **Rationale:** Reuse existing infrastructure, no separate DB needed

### Decision 8: Primary Keys for Tables
- **Date:** January 14, 2026
- **Decision:** Confirmed primary keys for upsert/incremental loads:
  - Customers: `CustomerKey`
  - Sales: `Order Number`
  - Products: `ProductKey`
  - Stores: `StoreKey`
  - Exchange_Rates: `Date` (per currency)

### Decision 9: Constraint Violation Handling
- **Date:** January 14, 2026
- **Decision:** Option A - Log error, skip record, continue loading others
- **Rationale:** Maximizes data loaded while capturing failures for review

### Decision 10: Incremental Load Strategy
- **Date:** January 14, 2026
- **Decision:** Primary key comparison to detect new records
- **Additional:** For Sales, optional date filter on `Order Date`

---

## 📁 COMPLETE FILE INVENTORY

### Configuration Files
```
config/
├── amazon_etl_config.yaml      # Main ETL configuration
├── amazon_cleaning_rules.yaml  # Data cleaning rules
└── transformation_config.yaml  # Transformation settings
```

### Scripts - Core
```
scripts/
├── config_loader.py            # T0003-T0007: YAML config loader
├── cleaning_utils.py           # T0008: Core cleaning utilities
├── Extract.py                  # Data extraction
├── Load.py                     # Data loading
└── TransformAmazon.py          # Amazon-specific transformations
```

### Scripts - Utilities
```
scripts/utils/
├── __init__.py
├── aggregation_utils.py        # T0013: Aggregation functions
├── bulk_loader.py              # T0018: High-performance batch loading
├── cleaning_engine.py          # Rule-based cleaning engine
├── constraint_handler.py       # T0020: Pre-load validation, Option A
├── dag_execution_tracker.py    # T0031: Pipeline execution tracking
├── datetime_utils.py           # T0016: Date/time utilities
├── db_utils.py                 # Database utilities
├── duplicate_missing_handler.py # T0010-T0011: Duplicate/missing handler
├── feature_engineering_utils.py # T0015: Feature engineering
├── load_strategy.py            # T0019: Full vs Incremental load
├── normalization_utils.py      # T0014: Z-score normalization (NEW columns)
├── rejected_records_handler.py # T0022: Error table + tracking
├── report_generators.py        # 8 report generators
├── table_cleaners.py           # T0008-T0012: Table-specific cleaners
├── transformation_orchestrator.py # T0017: Transformation orchestrator
├── transformation_utils.py     # General transformations
├── upsert_handler.py           # T0021: PostgreSQL ON CONFLICT upsert
└── validation_utils.py         # T0009, T0012: Validation framework
```

### DAGs
```
dags/
├── amazon_etl_dag.py           # Basic ETL DAG
└── amazon_etl_phase123.py      # Phase 1-3 integrated DAG
```

### Data Structure
```
data/
├── raw/
│   └── dataset/
│       ├── Customers.csv
│       ├── Sales.csv
│       ├── Products.csv
│       ├── Stores.csv
│       └── Exchange_Rates.csv
├── staging/
└── processed/
    ├── customers_cleaned.csv    # Output
    ├── sales_cleaned.csv        # Output
    ├── products_cleaned.csv     # Output
    ├── stores_cleaned.csv       # Output
    └── exchange_rates_cleaned.csv # Output
```

### Documentation
```
docs/
├── PHASE_1_SUMMARY.md
├── PHASE_2_SUMMARY.md
├── PRD_ETL_FRAMEWORK.md
├── ENHANCEMENT_SUMMARY.md
└── PROJECT_TRACKER.md          # THIS FILE
```

---

## 🎯 NEXT STEPS

### Immediate (Phase 4)
1. [ ] T0018: Build bulk_loader.py for batch database inserts
2. [ ] T0019: Implement incremental vs full load strategy
3. [ ] T0020: Create constraint violation handler
4. [ ] T0021: Build upsert logic (insert or update)
5. [ ] T0022: Create rejected_records error table

### After Phase 4
- Phase 5: Master DAG, triggers, dependencies, backfill, failure handling
- Phase 6: Combined pipeline, 5 source DAGs, reusable config, error recovery

---

## 🔗 QUICK REFERENCE

### Table-Specific Cleaning Rules

#### Customers Table
| Column | Cleaning Operation |
|--------|-------------------|
| CustomerKey | Deduplicate (keep first) |
| Birthday | Convert to datetime (YYYY-MM-DD) |
| Age | Typecast to int, fill missing with mean |
| Email | Validate regex, replace invalid with placeholder |

#### Sales Table
| Column | Cleaning Operation |
|--------|-------------------|
| Order Date | Convert to datetime |
| Delivery Date | Convert to datetime, fill missing with 'NA' |
| Delivery_Status | NEW column: Delivered/Delivering Soon/To Be Shipped/Lost |
| Total_Amount_USD | NEW column: Quantity × Unit Price USD (from Products JOIN) |

### Report Generators
| Report Class | Method | Output |
|-------------|--------|--------|
| CustomerSummaryReport | generate() | customer_summary.csv |
| ProductPerformanceReport | generate() | product_performance.csv |
| OrderStatusReport | generate() | order_status.csv |
| SalesTrendsReport | generate() | sales_trends_daily.csv |
| DataQualityScorecardReport | generate() | data_quality_scorecard.csv |
| CustomerSegmentationReport | generate() | customer_segmentation.csv |
| StorePerformanceReport | generate() | store_performance.csv |
| AnomalyDetectionReport | generate() | anomaly_detection.csv |
| DAGExecutionSummary | save_to_csv() | dag_execution_summary.csv |

---

## 📞 TEAM 1 TASK COMMENT FORMAT

All code files must include the standard TEAM 1 header:

```python
# ========================================
# TEAM 1 - T00XX: Task Description
# ========================================
```

Example:
```python
# ========================================
# TEAM 1 - T0008: Build reusable cleaning utilities
# ========================================
```

---

## 📅 SESSION HISTORY

### Session: January 14, 2026 (Continued)
- **Phase 4 COMPLETED** - All 5 tasks (T0018-T0022)
- Created `bulk_loader.py` - High-performance batch loading
- Created `load_strategy.py` - Full vs Incremental load logic
- Created `constraint_handler.py` - Pre-load validation, Option A behavior
- Created `upsert_handler.py` - PostgreSQL ON CONFLICT upsert
- Created `rejected_records_handler.py` - Error table with full tracking
- Updated PROJECT_TRACKER.md with new decisions and progress
- Ready to proceed with Phase 5

### Session: January 14, 2026 (Earlier)
- Reviewed Phase 1-3 completion status
- Confirmed 5 cleaned output tables
- Confirmed 9 reports (8 + DAG summary)
- Updated z-score to add as NEW column (not replace)
- Created PROJECT_TRACKER.md for comprehensive tracking

---

## 🎯 NEXT STEPS

### Immediate (Phase 5: T0023-T0027)
1. [ ] T0023: Build master DAG structure
2. [ ] T0024: Implement event-based triggers
3. [ ] T0025: Create multi-DAG dependencies
4. [ ] T0026: Build backfill mechanism
5. [ ] T0027: Implement failure handling

### After Phase 5
- Phase 6: Combined pipeline, 5 source DAGs, reusable config, error recovery
- Integration testing with Docker Airflow
- Full pipeline execution test

---

*This document should be updated after each significant change or task completion.*
