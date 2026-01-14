# Task Analysis: T0007–T0032

Deep dive into what each task requires and how they map to the proposed DAG structure.

---

## **Layer 1: Extract (T0007, T0024, T0029)**

### T0007 — Implement demo script to read/write CSVs [DONE]
- **What:** Read raw CSV files, output to intermediate format
- **Current:** `Extract.py` reads amazon.csv
- **New requirement:** Handle 5 datasets (Customers, Sales, Products, Stores, Exchange_Rates)
- **Output:** Raw data in memory (DataFrame) + log counts

### T0024 — Event-driven DAG triggering [DONE]
- **What:** Wait for raw files before extracting
- **Current:** FileSensor in `amazon_etl_dag.py`
- **New requirement:** Wait for all 5 CSV files in `data/raw/dataset/`
- **Implementation:** `extract_all_dag.py` starts with 5 FileSensors in parallel

### T0029 — Multi-source data pipelines [PENDING]
- **What:** Support multiple sources via pluggable adapters
- **Current:** Only CSV support
- **New requirement:** Load 5 different CSV files with potential different schemas
- **Implementation:** `Extract.py` with dataset-specific logic (column mapping, etc.)

**Deliverable:** `extract_all_dag.py` with 5 parallel FileSensor → extract task groups

---

## **Layer 2: Transform (T0008–T0017, T0028)**

### T0008–T0017 — Data cleaning & transformations [DONE for Amazon]
- **What:** Normalize, validate, derive, aggregate data
- **Current:** `TransformAmazon.py` with specific rules
- **New requirement:** Per-dataset transforms:
  - **Customers:** Dedup, standardize birthday, typecast age, fill missing age (mean), validate emails
  - **Sales:** Standardize dates, add delivery status, calculate total amount
  - **Products/Stores/Exchange:** Load-as-is (minimal transforms)

### T0028 — Combine ingestion → cleaning → validation → transform → load [PENDING]
- **What:** Unified flow ensuring each step completes before next
- **Current:** Separate scripts, manual orchestration in DAG
- **New requirement:** Clear handoffs via:
  - Data stored in `etl.input.*` after extract
  - Data stored in `etl.output.*` after transform
  - Rejection threshold: 30% (fail if >30% rejected)

**Deliverable:** 
- `Transform.py` with pluggable dataset handlers
- `transform_all_dag.py` with task groups for each dataset
- Internal dependencies: Products → Sales (Sales enriches from Products)

---

## **Layer 3: Validation & Quality (T0020–T0022, T0028)**

### T0020 — Handling constraint violations [DONE for Amazon]
- **What:** Catch DB integrity errors per-row
- **Current:** In `Load.py` during insert
- **New requirement:** Same approach for all 5 datasets

### T0022 — Error table creation (rejected records) [DONE]
- **What:** Log invalid rows with error context
- **Current:** `rejected_records` table in `public` schema
- **New requirement:** Persist in `etl.rejected_records` with dataset + timestamp

### T0028 validation component:
- **Acceptance criterion:** If rejected rows > 30% → fail task + alert
- **Implementation:** Check row counts before/after transform, raise error if threshold exceeded

**Deliverable:** Enhanced `Load.py` with rejection threshold checks

---

## **Layer 4: Load (T0007, T0018–T0021, T0028)**

### T0007 — Local file outputs (CSV/XLSX) [DONE for Amazon]
- **Current:** Saves to `data/processed/`
- **New requirement:** Save cleaned data per dataset (e.g., `customers_cleaned.csv`, `sales_cleaned.csv`)

### T0018 — Bulk load operations [DONE]
- **Current:** `to_sql(..., chunksize=1000)`
- **New requirement:** Same approach for all 5 datasets

### T0019 — Incremental vs Full loads [DONE]
- **Current:** Configurable via `load_type` parameter
- **New requirement:** Use same approach; default to full load for now

### T0021 — Upsert logic [DONE]
- **Current:** Optional per-key upsert
- **New requirement:** Not needed for now (full load sufficient)

**Deliverable:** `load_and_report_dag.py` with load task groups

---

## **Layer 5: Config & Orchestration (T0023, T0025, T0030)**

### T0023 — Build master DAG to trigger all pipelines [PENDING]
- **What:** Central orchestrator
- **Current:** Manual or via Airflow UI
- **New requirement:** Optional master DAG that:
  - Triggers extract_all_dag
  - Waits for completion
  - Triggers transform_all_dag
  - Waits for completion
  - Triggers load_and_report_dag
- **Implementation:** `master_orchestrator_dag.py` with ExternalTaskSensors

### T0025 — Multi-DAG dependency management [PENDING]
- **What:** Enforce sequence: extract → transform → load
- **Current:** Manual monitoring
- **New requirement:** Each DAG waits for previous via `ExternalTaskSensor`:
  - `transform_all_dag.py` waits for `extract_all_dag` completion
  - `load_and_report_dag.py` waits for `transform_all_dag` completion

### T0030 — Build reusable pipeline config [PENDING]
- **What:** Centralize paths, table names, thresholds, schedules
- **Current:** Hardcoded in scripts
- **New requirement:** `config/pipeline.yaml` with:
  ```yaml
  datasets:
    customers:
      source: data/raw/dataset/Customers.csv
      input_table: etl.input.customers
      output_table: etl.output.customers_cleaned
      transforms:
        - dedup
        - birthday_standardize
        - age_typecast
        - age_fillna_mean
        - email_validate
    sales:
      source: data/raw/dataset/Sales.csv
      input_table: etl.input.sales
      output_table: etl.output.sales_cleaned
      transforms:
        - date_standardize
        - delivery_status
        - total_amount_calc
    ...
  
  thresholds:
    rejection_rate: 0.30  # Fail if >30% rejected
  
  schedules:
    extract: "0 0 * * *"      # Daily midnight
    transform: "0 2 * * *"    # 2 AM (after extract)
    load: "0 4 * * *"         # 4 AM (after transform)
  ```

**Deliverable:** `config/pipeline.yaml` read by all DAGs

---

## **Layer 6: Reporting & Summary (T0026, T0027, T0031, T0032)**

### T0026 — Backfill/catchup controls [DONE]
- **What:** Historical run support with limits
- **Current:** In `amazon_etl_dag.py`
- **New requirement:** Apply to all 3 new DAGs:
  ```python
  catchup=True
  start_date=datetime(2024, 1, 1)
  max_active_runs=3
  ```

### T0027 — DAG failure handling strategy [DONE]
- **What:** Retries + callbacks + logs
- **Current:** In `amazon_etl_dag.py`
- **New requirement:** Apply to all 3 new DAGs with:
  - Retry: 3 times
  - Backoff: exponential (5min, 10min, 20min)
  - Callback: Log error context + optionally alert

### T0031 — Pipeline execution summary [PENDING]
- **What:** Per-run metrics + summary artifact
- **New requirement:** After load completes:
  - Create row in `etl.pipeline_runs` with:
    ```
    run_id, execution_date, total_rows_read, total_rows_loaded,
    total_rejected, rejection_rate, status, error_msg, duration
    ```
  - Generate markdown summary: `data/processed/pipeline_summary_{date}.md`

**Deliverable:** `generate_reports` task group in `load_and_report_dag.py`

### T0032 — Error recovery workflow [PENDING]
- **What:** Safe recovery for failures
- **New requirement:**
  - Retry failed tasks automatically (already via T0027)
  - If all retries fail: log to `rejected_records` + dead-letter schema
  - Optional CLI/DAG task to reprocess a specific dataset from a failed run

**Deliverable:** Documented recovery procedure + optional reprocess CLI

---

## **PostgreSQL Schema Evolution**

### Current (T0001–T0022):
```sql
public.*     — Airflow metadata (task_instance, dag_run, etc.)
etl.customers_raw, customers_cleaned, etc.
```

### New (T0028–T0032):
```sql
etl.input.*          — Raw extracted data (customers, sales, products, stores, exchange_rates)
etl.output.*         — Cleaned transformed data (same 5)
etl.rejected_records — Failed rows with error + dataset context
etl.pipeline_runs    — Per-run execution summary
```

---

## **File Structure (Final)**

```
dags/
├── extract_all_dag.py           # T0024, T0025, T0029
├── transform_all_dag.py         # T0025, T0028, T0029
├── load_and_report_dag.py       # T0031, T0032
└── master_orchestrator_dag.py   # T0023 (optional)

scripts/
├── Extract.py                   # T0007, T0024, T0029
├── Transform.py                 # T0008–T0017, T0028
├── Load.py                       # T0007, T0018–T0022, T0028
└── postgres_config.py           # Updated schemas

config/
└── pipeline.yaml                # T0030 (dataset mappings, thresholds, schedules)

data/
├── raw/dataset/                 # Input CSVs (T0029)
├── processed/                   # Output CSVs + summaries (T0031)
└── staging/                     # Temp files if needed
```

---

## **Execution Order (DAG Schedule)**

1. **extract_all_dag** (0:00 AM daily)
   - FileSensors wait for 5 CSVs
   - 5 parallel extracts → `etl.input.*`

2. **transform_all_dag** (2:00 AM daily, depends on extract completion)
   - Products → Sales (internal series)
   - Others in parallel
   - Output → `etl.output.*`
   - If rejection >30% → fail + alert

3. **load_and_report_dag** (4:00 AM daily, depends on transform completion)
   - 5 parallel loads to final tables
   - Generate summary → `etl.pipeline_runs`
   - Output markdown report

4. **master_orchestrator_dag** (optional, 5:00 AM)
   - Monitors all 3 above
   - Triggers alerts if any fails

---

## **Dependencies Summary**

```
extract_all_dag
    ↓ (ExternalTaskSensor)
transform_all_dag
    ├── extract_products → transform_products
    ├── extract_sales → transform_sales → (needs products data)
    ├── extract_customers → transform_customers
    ├── extract_stores → transform_stores
    └── extract_exchange → transform_exchange
    ↓ (ExternalTaskSensor)
load_and_report_dag
    ├── load_products
    ├── load_sales → (needs products loaded first)
    ├── load_customers
    ├── load_stores
    ├── load_exchange
    └── generate_reports (depends on all loads)
```

---

## **Ready to Implement?**

With this analysis, the implementation path is clear:
1. Update `postgres_config.py` with `etl.input`, `etl.output`, `pipeline_runs` schemas
2. Create unified `Extract.py` with dataset router
3. Create unified `Transform.py` with per-dataset handlers
4. Update `Load.py` with rejection threshold checks
5. Create `config/pipeline.yaml`
6. Create 3 DAGs with proper dependencies
7. Test end-to-end

**Proceed?**
