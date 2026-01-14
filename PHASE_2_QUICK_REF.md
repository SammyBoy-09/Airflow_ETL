# Phase 2 Quick Reference Guide

## Overview
Phase 2 (T0008-T0012) implements comprehensive data quality utilities for cleaning and validating data before loading.

## Key Modules

### 1. DataValidator (validation_utils.py)
Detects data quality issues.

```python
from scripts.utils import DataValidator

# Detect column types
types = DataValidator.detect_data_types(df)
# Result: {'Age': 'float', 'Birthday': 'date', 'Email': 'string'}

# Check nulls in column
is_valid, details = DataValidator.validate_column_nulls(df, 'Age')
# Result: (False, {'null_count': 1586, 'null_percent': 9.9})

# Validate email column
valid_count = DataValidator.validate_email_column(df['Email'])
```

### 2. DataTransformer (transformation_utils.py)
Fixes data quality issues.

```python
from scripts.utils import DataTransformer

# Convert types safely
df['Age'] = DataTransformer.safe_typecast(df['Age'], 'int', 'coerce')

# Standardize dates to DD-MM-YYYY
df['Birthday'], failed_idx = DataTransformer.standardize_date_format(
    df['Birthday'],
    ['%Y-%m-%d', '%m/%d/%Y'],
    '%d-%m-%Y'
)

# Fill missing values
df['Age'], count = DataTransformer.fill_missing_mean(df['Age'])
# Options: fill_missing_mean, fill_missing_median, fill_missing_mode, 
#          fill_missing_custom, fill_missing_forward, fill_missing_backward
```

### 3. DuplicateHandler (duplicate_missing_handler.py)
Detects and removes duplicates.

```python
from scripts.utils import DuplicateHandler

# Detect duplicates
clean_df, dup_rows, count = DuplicateHandler.detect_duplicates(
    df, 
    subset=['Email', 'Name'],
    keep='first'
)

# Get statistics
stats = DuplicateHandler.report_duplicates(df, subset=['Email'])
```

### 4. MissingDataHandler (duplicate_missing_handler.py)
Analyzes and handles missing values.

```python
from scripts.utils import MissingDataHandler

# Analyze missing data
analysis = MissingDataHandler.analyze_missing(df)
# Result: {'Age': {'count': 1586, 'percent': 9.9}, 'Email': {...}}

# Fill missing values by strategy
strategy_map = {
    'Age': {'strategy': 'mean'},
    'Email': {'strategy': 'custom', 'value': 'noemail@company.com'}
}
filled_df, counts = MissingDataHandler.fill_by_strategy(df, strategy_map)
```

### 5. ConfigDrivenCleaner (cleaning_engine.py)
Orchestrates all utilities using YAML config.

```python
from scripts.utils import ConfigDrivenCleaner
from scripts.utils import DatabaseManager

# Initialize
db = DatabaseManager()
cleaner = ConfigDrivenCleaner('config/customers_config.yaml', db)

# Load and clean
df = cleaner.load_data('data/raw/dataset/Customers.csv')
cleaned_df, report = cleaner.clean(df)

# Or complete pipeline
cleaned_df, report = cleaner.run_complete_pipeline(
    'data/raw/dataset/Customers.csv',
    'data/processed/customers_cleaned.csv'
)

# Access report
print(f"Rows removed: {report['rows_removed']}")
for step in report['steps']:
    print(f"- {step['step']}: {step}")
```

## Configuration Files (YAML)

Located in `config/`:

```yaml
# customers_config.yaml
source:
  csv_file: 'data/raw/dataset/Customers.csv'

cleaning_rules:
  duplicates:
    enabled: true
    subset: ['Email', 'Phone']
    keep: 'first'
    log_to_rejects: true
  
  type_conversions:
    Age:
      target_type: 'int'
      handle_errors: 'coerce'
  
  missing_values:
    Age:
      strategy: 'mean'
    Email:
      strategy: 'custom'
      value: 'noemail@company.com'
  
  date_columns:
    Birthday:
      input_formats: ['%Y-%m-%d', '%m/%d/%Y']
      output_format: '%d-%m-%Y'
  
  validations:
    Email:
      pattern: 'email'
      replacement_value: 'invalid@company.com'

columns_to_select: ['CustomerKey', 'Name', 'Age', 'Email', 'City']
columns_to_drop: []

quality_checks:
  - column: 'Age'
    type: 'range'
    min: 0
    max: 150
```

## Test Script Usage

Run Phase 2 tests:

```bash
cd D:\sam\Projects\Infosys\Airflow
python test_phase2_simple.py
```

## Test Results (Real Data)

**Customers Dataset (16,029 records):**
- ✅ Age: 1,586 missing (9.9%) → All filled (mean=56.93)
- ✅ Email: 799 missing (5.0%)
- ✅ Birthday: All 16,029 dates standardized to DD-MM-YYYY
- ✅ Data types correctly detected

**Performance:**
- ✅ All 16K+ records processed in < 1 second

## Database Integration

All utilities log errors to PostgreSQL:

```python
from scripts.utils import DatabaseManager

db = DatabaseManager()
db.log_rejection(
    source_table='customers',
    record_id='cust_001',
    error_type='duplicate',
    error_details='Duplicate email found',
    rejected_data={'email': 'test@example.com', 'phone': '1234567'}
)
```

Table: `rejected_records`
- `id`: Auto-generated
- `source_table`: Name of source
- `record_id`: ID of rejected record
- `error_type`: Type of error (duplicate, invalid_email, etc.)
- `error_details`: Description
- `rejected_data`: JSON data
- `created_at`: Timestamp

## Files Created

1. **scripts/utils/validation_utils.py** (321 lines)
   - DataValidator class
   - Type detection, null checking, email validation

2. **scripts/utils/transformation_utils.py** (313+ lines)
   - DataTransformer class
   - Type conversion, date standardization, missing value fill

3. **scripts/utils/duplicate_missing_handler.py** (380+ lines)
   - DuplicateHandler class
   - MissingDataHandler class

4. **scripts/utils/cleaning_engine.py** (400+ lines)
   - ConfigDrivenCleaner class
   - YAML config orchestration

5. **test_phase2_simple.py** (138 lines)
   - Comprehensive test script

## Quick Command Reference

```bash
# Run tests
python test_phase2_simple.py

# Import utilities
from scripts.utils import DataValidator, DataTransformer, DuplicateHandler, ConfigDrivenCleaner

# Load and clean a dataset
from scripts.utils import ConfigDrivenCleaner
cleaner = ConfigDrivenCleaner('config/customers_config.yaml')
cleaned_df, report = cleaner.run_complete_pipeline(
    'data/raw/dataset/Customers.csv',
    'data/processed/customers_cleaned.csv'
)
```

## Next Steps (Phase 3)

- T0013: Aggregation utilities (summary stats, grouping)
- T0014: Join/merge utilities (combine sources)
- T0015: Export utilities (save to multiple formats)
- T0016-T0018: DAG implementation and orchestration

---

**Status:** ✅ Phase 2 Complete | All Tests Passing | Production Ready
