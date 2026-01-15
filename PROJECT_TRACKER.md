# ================================================================================
# TEAM 1 - ETL PIPELINE PROJECT TRACKER
# ================================================================================
# Last Updated: January 15, 2026 (Reports Enhancement & Database Cleanup)
# Purpose: Comprehensive tracking of all tasks, decisions, and progress
# Architecture: Extract.py → Transform.py → Load.py → ReportGenerator.py
# ================================================================================

## 📋 PROJECT OVERVIEW

| Item | Details |
|------|---------|
| **Project Name** | Amazon ETL Pipeline Framework |
| **Team** | TEAM 1 |
| **Total Tasks** | 37 (T0001 - T0037) |
| **Total Sprints** | 7 |
| **Technology Stack** | Python 3.11, Airflow 2.8.3, PostgreSQL 15, Docker |
| **Conda Environment** | KB_1978 |
| **Project Root** | D:\sam\Projects\Infosys\Airflow |

---

## 📊 SPRINT SUMMARY

| Sprint | Tasks | Description | Status |
|--------|-------|-------------|--------|
| **Sprint 1** | T0001-T0007 | Environment Setup & Pipeline Design | ✅ COMPLETE |
| **Sprint 2** | T0008-T0012 | Data Quality & Cleaning Utilities | ✅ COMPLETE |
| **Sprint 3** | T0013-T0017 | Aggregations & Transformations | ✅ COMPLETE |
| **Sprint 4** | T0018-T0022 | Loading Strategies | ✅ COMPLETE |
| **Sprint 5** | T0023-T0027 | Orchestration & Scheduling | ✅ COMPLETE |
| **Sprint 6** | T0028-T0032 | Combined Pipeline & Recovery | ✅ COMPLETE |
| **Sprint 7** | T0033-T0037 | API Service Development | 🔲 NOT STARTED |

---

## 📁 OUTPUT DELIVERABLES

### 5 Cleaned Output Tables
| # | Table | Source File | Output File | Key Cleaning Operations |
|---|-------|-------------|-------------|------------------------|
| 1 | **Customers** | Customers.csv | customers_cleaned.csv | Dedupe, birthday→1900-01-01 placeholder, age→int (mean fill), email validation (remove invalid), `loyalty_category` column |
| 2 | **Sales** | Sales.csv | sales_cleaned.csv | Date standardization, Delivery Date→1900-01-01 placeholder, `delivery_status` column, `total_amount_usd` column (JOIN Products) |
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
### SPRINT 5: Orchestration & Scheduling (T0023-T0027) ✅ COMPLETE
### ═══════════════════════════════════════════════════════════════

| Task ID | Description | Status | File(s) Created |
|---------|-------------|--------|-----------------|
| T0023 | Build master DAG to trigger all pipelines | ✅ | dags/etl_master_orchestrator.py |
| T0024 | Event-driven DAG triggering | ✅ | dags/etl_*.py (TriggerDagRunOperator) |
| T0025 | Multi-DAG dependency management | ✅ | dags/etl_sales.py, dags/etl_reports.py (ExternalTaskSensor) |
| T0026 | Backfill & catchup features | ✅ | All DAGs (catchup parameter, schedule_interval) |
| T0027 | DAG failure handling strategy | ✅ | dags/dag_base.py (retries, callbacks, email alerts) |

**Sprint 5 Files:**
- `dags/dag_base.py` - Shared DAG configuration (defaults, callbacks, connections)
- `dags/etl_customers.py` - Customers dimension table ETL
- `dags/etl_products.py` - Products dimension table ETL
- `dags/etl_stores.py` - Stores dimension table ETL
- `dags/etl_exchange_rates.py` - Exchange rates table ETL
- `dags/etl_sales.py` - Sales fact table ETL (depends on Products)
- `dags/etl_reports.py` - Report generation (depends on ALL 5 tables)
- `dags/etl_master_orchestrator.py` - Master orchestrator with TaskGroups

**Sprint 5 Key Features:**
- **7 DAGs Total:** 5 table DAGs + 1 reports DAG + 1 master orchestrator
- **ExternalTaskSensor:** Cross-DAG dependency management
- **TriggerDagRunOperator:** Programmatic DAG triggering
- **TaskGroups:** Visual organization in Airflow UI
- **Error Handling:** Retries (3), retry_delay (1 min), failure callbacks

---

### ═══════════════════════════════════════════════════════════════
### SPRINT 6: Combined Pipeline & Recovery (T0028-T0032) ✅ COMPLETE
### ═══════════════════════════════════════════════════════════════

| Task ID | Description | Status | File(s) Created |
|---------|-------------|--------|-----------------|
| T0028 | Combine ingestion → cleaning → validation → transform → load | ✅ | All ETL DAGs (full pipeline per table) |
| T0029 | Multi-source data pipelines | ✅ | 5 source-specific DAGs (customers, products, stores, sales, exchange_rates) |
| T0030 | Build reusable pipeline config | ✅ | dags/dag_base.py (shared config), config/*.yaml |
| T0031 | Pipeline execution summary | ✅ | dags/etl_reports.py (9 reports), etl_master_orchestrator.py (JSON summary) |
| T0032 | Error recovery workflow | ✅ | dags/dag_base.py (retries, callbacks), rejected_records table |

**Sprint 6 Files:**
- `dags/etl_customers.py` - Full E-T-L with loyalty_category, rejected_records tracking
- `dags/etl_sales.py` - Full E-T-L with delivery_status, total_amount_usd columns
- `dags/etl_master_orchestrator.py` - Full pipeline orchestration with TaskGroups
- `data/processed/reports/orchestrator_execution_summary.json` - Execution summary output
- `data/reports/*.csv` - 9 generated reports

**Sprint 6 Key Features:**
- **Complete E-T-L Pipeline:** Each DAG runs extract → transform → load
- **5 Source-Specific DAGs:** One per data source with proper dependencies
- **Reusable Config:** dag_base.py with DEFAULT_ARGS, SCHEDULE_MIDNIGHT_IST, etc.
- **Execution Summary:** JSON summary with stage timing and DAG status
- **Error Recovery:** Retries, rejected_records table, failure callbacks

**Sprint 6 Enhancements (January 15, 2026):**

**Customers Table Enhancements:**
- Birthday: Empty values → `1900-01-01` placeholder date
- Email: Invalid emails removed (tracked in rejected_records)
- New column: `loyalty_category` (Premium/Standard/Basic based on age)

**Sales Table Enhancements:**
- Delivery Date: Empty values → `1900-01-01` placeholder date
- New column: `delivery_status` (Delivered/Shipped/In Transit/Lost)
- New column: `total_amount_usd` (Quantity × Unit Price USD from Products)

**New Database Tables:**
- `etl_output.rejected_records` - Tracks all rejected records (append-only)
  - Columns: table_name, record_id, reason, original_data, rejected_at, dag_run_id
- `etl_output.dag_run_summary` - DAG execution history (append-only)
  - Columns: dag_id, run_id, execution_date, table_name, rows_extracted, rows_loaded, rows_rejected, status

**Reports Fix (January 15, 2026):**
- `customer_purchase_analysis.csv` now includes ALL customers (11,887 rows)
- Missing Name/Country filled from raw Customers.csv (for rejected customers)
- Fallback to 'Unknown' for any remaining null values
- Fixed `.head(100)` limit that was artificially restricting output

**New Report Files Added (January 15, 2026):**
- `rejected_records_summary.csv` - Aggregated count by table_name and reason
- `rejected_records_detail.csv` - Full history of all 87,837 rejected records
- `dag_run_summary.csv` - Complete DAG execution history with rows extracted/loaded/rejected

**Database Schema Cleanup (January 15, 2026):**
- **Dropped `etl` schema** - Removed 20 old project tables
- **Cleaned `public` schema** - Removed 7 leftover ETL tables:
  - customers_cleaned, customers_summary, products_summary
  - sellers_summary, rejected_records, etl_rejected_records, etl_load_errors
- **Final Structure:**
  - `public` - Airflow internal tables only (53 tables)
  - `etl_output` - Current project output (7 tables)

---

### ═══════════════════════════════════════════════════════════════
### SPRINT 7: API Service Development (T0033-T0037) 🔲 NOT STARTED
### ═══════════════════════════════════════════════════════════════

| Task ID | Description | Status | File(s) To Create |
|---------|-------------|--------|-------------------|
| T0033 | Build Flask/FastAPI service | 🔲 | api/app.py |
| T0034 | Expose pipeline run status | 🔲 | api/routes/status.py |
| T0035 | Expose metadata summary | 🔲 | api/routes/metadata.py |
| T0036 | Fetch logs via API | 🔲 | api/routes/logs.py |
| T0037 | Pagination & filtering | 🔲 | api/utils/pagination.py |

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
### Scripts - Main ETL Pipeline (NEW ARCHITECTURE)
```
scripts/
├── Extract.py              # EXTRACT: Load data from CSVs → staging
├── Transform.py            # TRANSFORM: Clean all 5 tables
├── Load.py                 # LOAD: Load to PostgreSQL database
├── ReportGenerator.py      # REPORTS: Generate all 9 reports
├── config_loader.py        # YAML/JSON config loading utility
└── cleaning_utils.py       # Legacy cleaning utilities
```

### Scripts - Reusable Utilities (12 files)
```
scripts/utils/
├── __init__.py                 # Package init (generic exports only)
├── aggregation_utils.py        # T0013: Generic aggregation functions
├── bulk_loader.py              # T0018: High-performance batch loading
├── constraint_handler.py       # T0020: Pre-load validation
├── dag_execution_tracker.py    # T0031: Pipeline execution tracking
├── datetime_utils.py           # T0016: Date/time utilities
├── feature_engineering_utils.py # T0015: Feature engineering
├── load_strategy.py            # T0019: Full vs Incremental load
├── normalization_utils.py      # T0014: Z-score normalization
├── rejected_records_handler.py # T0022: Error table + tracking
├── transformation_orchestrator.py # T0017: Pipeline orchestrator
├── upsert_handler.py           # T0021: PostgreSQL ON CONFLICT upsert
└── validation_utils.py         # T0008, T0012: Validation framework

PROJECT-SPECIFIC CODE MOVED TO MAIN SCRIPTS:
- table_cleaners logic      → Transform.py
- report_generators logic   → ReportGenerator.py
```

### DAGs (7 files)
```
dags/
├── dag_base.py                 # Shared DAG configuration (defaults, callbacks)
├── etl_customers.py            # T0029: Customers dimension table ETL
├── etl_products.py             # T0029: Products dimension table ETL
├── etl_stores.py               # T0029: Stores dimension table ETL
├── etl_exchange_rates.py       # T0029: Exchange rates table ETL
├── etl_sales.py                # T0029: Sales fact table ETL (depends on products)
├── etl_reports.py              # T0031: Report generation (depends on ALL 5 tables)
├── etl_master_orchestrator.py  # T0023: Master orchestrator with TaskGroups
└── amazon_etl_phase123.py      # Legacy Phase 1-3 DAG
```

### Data Structure
```
data/
├── raw/
│   └── dataset/
│       ├── Customers.csv       # 16,029 rows
│       ├── Sales.csv           # 62,884 rows
│       ├── Products.csv        # 2,517 rows
│       ├── Stores.csv          # 67 rows
│       └── Exchange_Rates.csv  # 3,655 rows
├── staging/                    # Extracted raw data (CSVs)
├── processed/                  # Cleaned output (5 tables)
│   ├── customers_cleaned.csv   # 15,266 rows (763 dupes removed)
│   ├── sales_cleaned.csv       # 62,884 rows (with Delivery_Status, Total_Amount_USD)
│   ├── products_cleaned.csv    # 2,517 rows
│   ├── stores_cleaned.csv      # 67 rows
│   └── exchange_rates_cleaned.csv # 3,655 rows
└── reports/                    # Generated reports (9 reports)
    ├── customer_summary.csv
    ├── product_performance.csv
    ├── order_status.csv
    ├── sales_trends_daily.csv
    ├── data_quality_scorecard.csv
    ├── customer_segmentation.csv
    ├── store_performance.csv
    ├── anomaly_detection.csv
    └── dag_execution_summary.csv
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

### Immediate (Sprint 7: T0033-T0037) - API Service Development
1. [ ] T0033: Build Flask/FastAPI service
2. [ ] T0034: Expose pipeline run status endpoint
3. [ ] T0035: Expose metadata summary endpoint
4. [ ] T0036: Fetch logs via API endpoint
5. [ ] T0037: Implement pagination & filtering

### After Sprint 7
- Integration testing with full pipeline
- Performance optimization
- Documentation updates
- Production deployment preparation

---

## ✅ DATABASE VERIFICATION (January 14, 2026)

### PostgreSQL Tables Loaded (etl_output schema)
| Table | Row Count | Status |
|-------|-----------|--------|
| customers | 15,266 | ✅ Loaded |
| products | 2,517 | ✅ Loaded |
| stores | 67 | ✅ Loaded |
| sales | 26,326 | ✅ Loaded |
| exchange_rates | 3,655 | ✅ Loaded |

### Airflow DAGs Status
| DAG | Status | Description |
|-----|--------|-------------|
| etl_customers | ✅ Working | Customers dimension table |
| etl_products | ✅ Working | Products dimension table |
| etl_stores | ✅ Working | Stores dimension table |
| etl_exchange_rates | ✅ Working | Exchange rates table |
| etl_sales | ✅ Working | Sales fact table (depends on products) |
| etl_reports | ✅ Working | 9 analytics reports |
| etl_master_orchestrator | ✅ Working | Master orchestrator with TaskGroups |

### Generated Reports (9 total)
| Report | Status |
|--------|--------|
| customer_summary.csv | ✅ Generated |
| product_performance.csv | ✅ Generated |
| order_status.csv | ✅ Generated |
| sales_trends_daily.csv | ✅ Generated |
| data_quality_scorecard.csv | ✅ Generated |
| customer_segmentation.csv | ✅ Generated |
| store_performance.csv | ✅ Generated |
| anomaly_detection.csv | ✅ Generated |
| dag_execution_summary.csv | ✅ Generated |

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

### Session: January 14, 2026 (Sprint 5 & 6 Complete)
- **Sprint 5 COMPLETED** - All 5 tasks (T0023-T0027)
- **Sprint 6 COMPLETED** - All 5 tasks (T0028-T0032)
- Created 7 Airflow DAGs with proper dependencies
- Implemented ExternalTaskSensor for cross-DAG dependencies
- Implemented TriggerDagRunOperator for DAG triggering
- Created etl_master_orchestrator.py with TaskGroups
- Loaded all 5 tables to PostgreSQL (etl_output schema)
- Generated 9 analytics reports
- All DAGs tested and working in Docker Airflow environment
- Ready to proceed with Sprint 7 (API Service)

### Session: January 14, 2026 (Phase 4 Complete)
- **Phase 4 COMPLETED** - All 5 tasks (T0018-T0022)
- Created `bulk_loader.py` - High-performance batch loading
- Created `load_strategy.py` - Full vs Incremental load logic
- Created `constraint_handler.py` - Pre-load validation, Option A behavior
- Created `upsert_handler.py` - PostgreSQL ON CONFLICT upsert
- Created `rejected_records_handler.py` - Error table with full tracking
- Updated PROJECT_TRACKER.md with new decisions and progress

### Session: January 14, 2026 (Earlier)
- Reviewed Phase 1-3 completion status
- Confirmed 5 cleaned output tables
- Confirmed 9 reports (8 + DAG summary)
- Updated z-score to add as NEW column (not replace)
- Created PROJECT_TRACKER.md for comprehensive tracking

---

## 🎯 NEXT STEPS

### Immediate (Sprint 7: T0033-T0037) - API Service Development
1. [ ] T0033: Build Flask/FastAPI service
2. [ ] T0034: Expose pipeline run status endpoint
3. [ ] T0035: Expose metadata summary endpoint
4. [ ] T0036: Fetch logs via API endpoint
5. [ ] T0037: Implement pagination & filtering

---

*This document should be updated after each significant change or task completion.*
