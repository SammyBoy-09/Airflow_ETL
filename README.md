# Infosys Airflow ETL Framework

A comprehensive, production-ready ETL framework for Apache Airflow with advanced data quality and cleaning capabilities.

## 📋 Project Status

✅ **Phase 1:** Environment & Database Design - COMPLETE  
✅ **Phase 2:** Data Quality & Cleaning Utilities - COMPLETE  
🔄 **Phase 3:** Aggregations & Transformations - IN PROGRESS  
⏳ **Phase 4-6:** Loading, DAGs, and Orchestration - PENDING

**Overall Completion:** 40% (Phases 1-2 of 6)

## 🎯 Quick Start

### Prerequisites
- Docker Desktop (PostgreSQL, Airflow, Redis)
- Python 3.12+ (via conda environment)
- 5 CSV data files in `data/raw/dataset/`

### Setup

```bash
# 1. Start Docker containers
cd Docker
./start_airflow.ps1  # Windows

# 2. Create Python environment (if needed)
conda create -n KB_1978 python=3.12

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run Phase 2 tests
python test_phase2_simple.py
```

## 📊 Data Sources

| Source | Rows | Columns | Status |
|--------|------|---------|--------|
| Customers | 16,029 | 12 | ✅ Loaded |
| Sales | 62,884 | 9 | ✅ Loaded |
| Products | 2,517 | 10 | ✅ Loaded |
| Stores | 67 | 5 | ✅ Loaded |
| Exchange_Rates | 3,655 | 3 | ✅ Loaded |

## 🏗️ Architecture

### Database Schema (PostgreSQL)
- `customers` - Customer master data
- `sales` - Sales transactions
- `products` - Product catalog
- `stores` - Store locations
- `exchange_rates` - Currency conversion rates
- `rejected_records` - Error tracking & logging
- Plus 8 more supporting tables

### Utilities (Phase 2 - ✅ COMPLETE)

#### 1. DataValidator (`validation_utils.py`)
Detects data quality issues across all datasets.

```python
from scripts.utils import DataValidator

types = DataValidator.detect_data_types(df)
DataValidator.validate_column_nulls(df, 'Age')
DataValidator.validate_email_column(df['Email'])
```

**Key Features:**
- Type detection (int, float, string, date, bool)
- Null/missing value analysis
- Email format validation
- Duplicate key detection
- Value range validation

**Test Results (Customers dataset):**
- ✅ Type detection: Age=float, Birthday=date, Email=string
- ✅ Nulls: Age=1,586 (9.9%), Email=799 (5.0%), Name=0
- ✅ All validations executing successfully

#### 2. DataTransformer (`transformation_utils.py`)
Fixes data quality issues detected by validator.

```python
from scripts.utils import DataTransformer

# Type conversion
df['Age'] = DataTransformer.safe_typecast(df['Age'], 'int', 'coerce')

# Date standardization
df['Birthday'], failed = DataTransformer.standardize_date_format(
    df['Birthday'],
    ['%Y-%m-%d', '%m/%d/%Y'],
    '%d-%m-%Y'
)

# Missing value imputation
df['Age'], count = DataTransformer.fill_missing_mean(df['Age'])
```

**Key Features:**
- Safe type casting (int, float, string, date, bool)
- Date format standardization (multiple input formats → single output)
- 6 missing value fill strategies:
  - Mean/Median/Mode
  - Forward/Backward fill
  - Custom value
- Text normalization (trim, case conversion)
- Email cleaning with pattern validation
- Special character removal

**Test Results:**
- ✅ All 16,029 dates converted to DD-MM-YYYY
- ✅ All 1,586 missing Ages filled (mean=56.93)
- ✅ Type conversion with error handling
- ✅ Performance: < 1 second for 16K records

#### 3. DuplicateHandler (`duplicate_missing_handler.py`)
Detects and removes duplicate records.

```python
from scripts.utils import DuplicateHandler

clean_df, dups, count = DuplicateHandler.detect_duplicates(
    df,
    subset=['Email', 'Name'],
    keep='first'
)
```

**Key Features:**
- Duplicate detection on single or multiple columns
- Keep first/last/all options
- Statistics generation
- Integration with rejected_records table

#### 4. MissingDataHandler (`duplicate_missing_handler.py`)
Comprehensive missing data handling.

```python
from scripts.utils import MissingDataHandler

analysis = MissingDataHandler.analyze_missing(df)
strategy_map = {
    'Age': {'strategy': 'mean'},
    'Email': {'strategy': 'custom', 'value': 'noemail@company.com'}
}
filled_df, counts = MissingDataHandler.fill_by_strategy(df, strategy_map)
```

**Key Features:**
- Missing data analysis & statistics
- Row dropping (with threshold)
- Column dropping (with threshold)
- Per-column strategy application

#### 5. ConfigDrivenCleaner (`cleaning_engine.py`)
Orchestrates all utilities via YAML configuration.

```python
from scripts.utils import ConfigDrivenCleaner

cleaner = ConfigDrivenCleaner('config/customers_config.yaml')
cleaned_df, report = cleaner.run_complete_pipeline(
    'data/raw/dataset/Customers.csv',
    'data/processed/customers_cleaned.csv'
)
```

**Workflow:**
1. Load YAML config (duplicates, types, dates, validations)
2. Handle duplicates
3. Type conversions
4. Missing value filling
5. Date standardization
6. Validation & cleaning
7. Quality checks
8. Save output with report

**Configuration Example:**
```yaml
# config/customers_config.yaml
cleaning_rules:
  duplicates:
    enabled: true
    subset: ['Email', 'Phone']
    keep: 'first'
  
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
```

### Database Integration
All utilities log errors to PostgreSQL:

```python
db = DatabaseManager()
db.log_rejection(
    source_table='customers',
    record_id='cust_001',
    error_type='duplicate',
    error_details='Duplicate email found',
    rejected_data={'email': 'test@example.com'}
)
```

## 📁 Project Structure

```
Airflow/
├── config/                          # Configuration files
│   ├── customers_config.yaml        # Customer cleaning rules
│   ├── sales_config.yaml           # Sales transformation rules
│   ├── products_config.yaml        # Product rules
│   ├── stores_config.yaml          # Store rules
│   └── exchange_rates_config.yaml  # Exchange rate rules
│
├── data/
│   ├── raw/dataset/                # Input CSV files (5 sources)
│   ├── staging/                    # Staging area
│   └── processed/                  # Cleaned data output
│
├── scripts/
│   ├── utils/                      # Phase 2 Utilities (NEW)
│   │   ├── __init__.py
│   │   ├── db_utils.py            # Database operations
│   │   ├── validation_utils.py    # Data validation
│   │   ├── transformation_utils.py # Data transformation
│   │   ├── duplicate_missing_handler.py # Cleaning
│   │   └── cleaning_engine.py     # Orchestration
│   │
│   ├── Extract.py                 # Legacy extraction
│   ├── Load.py                    # Legacy loading
│   ├── TransformAmazon.py        # Legacy transformation
│   └── config_loader.py           # Config loader
│
├── dags/                            # Airflow DAG definitions
│   └── amazon_etl_dag.py
│
├── data_models/
│   └── models.py                  # Pydantic data models
│
├── Docker/
│   ├── docker-compose.yaml        # Docker configuration
│   ├── start_airflow.ps1          # Windows startup script
│   └── CREDENTIALS.md             # Connection details
│
├── tests/                          # Test files
│   └── test_etl_pipeline.py
│
├── docs/                           # Documentation
├── logs/                           # Airflow logs
├── plugins/                        # Airflow plugins
│
├── requirements.txt                # Python dependencies (20 packages)
├── PROJECT_ANALYSIS.md             # Phase 1 analysis
├── PHASE_1_SUMMARY.md             # Phase 1 completion report
├── PHASE_1_QUICK_REF.md           # Phase 1 quick reference
├── PHASE_2_SUMMARY.md             # Phase 2 completion report
├── PHASE_2_QUICK_REF.md           # Phase 2 quick reference
├── PRD_ETL_FRAMEWORK.md           # Product requirements document
└── README.md                       # This file
```

## 📚 Documentation

| Document | Purpose |
|----------|---------|
| [PHASE_1_SUMMARY.md](PHASE_1_SUMMARY.md) | Phase 1 completion details |
| [PHASE_1_QUICK_REF.md](PHASE_1_QUICK_REF.md) | Phase 1 quick reference |
| [PHASE_2_SUMMARY.md](PHASE_2_SUMMARY.md) | Phase 2 completion details |
| [PHASE_2_QUICK_REF.md](PHASE_2_QUICK_REF.md) | Phase 2 quick reference |
| [PROJECT_ANALYSIS.md](PROJECT_ANALYSIS.md) | Initial project analysis |
| [PRD_ETL_FRAMEWORK.md](PRD_ETL_FRAMEWORK.md) | Full product requirements |

## 🔗 Services & Connections

**Docker Services:**
- **Airflow Web UI:** http://localhost:8080
- **PostgreSQL:** localhost:5432
  - Database: `airflow`
  - User: `airflow`
  - Password: `airflow`
- **Redis:** localhost:6379
- **Flower (Celery):** http://localhost:5555

**Default Credentials:**
- Airflow: `airflow` / `airflow`
- PostgreSQL: `airflow` / `airflow`

## 🧪 Testing

### Run Phase 2 Tests
```bash
python test_phase2_simple.py
```

**Test Coverage:**
- ✅ Data loading (5 CSV sources)
- ✅ Data validation (type detection, null checking)
- ✅ Type conversions (string → int, date parsing)
- ✅ Date standardization (16,029 records → DD-MM-YYYY)
- ✅ Missing value handling (1,586 records → all filled)
- ✅ Database connection & logging
- ✅ All utilities with real data

### Results Summary
```
✅ Customers loaded: 16,029 rows
✅ Age nulls: 1,586 → filled with mean (56.93)
✅ Emails nulls: 799 detected
✅ Birthday dates: All 16,029 standardized
✅ Performance: < 1 second
✅ Database logging: Active
```

## 🚀 Key Features

### Phase 1: Environment & Database
✅ Docker setup (Airflow 2.8.3, PostgreSQL 15, Redis 7)  
✅ Database schema with 14 tables  
✅ Connection testing  
✅ Demo scripts  
✅ Configuration files (5 YAML)

### Phase 2: Data Quality & Cleaning
✅ 5 reusable utility modules (1,414 lines)  
✅ Data validation (type detection, null checking, email validation)  
✅ Data transformation (type conversion, date standardization, missing value handling)  
✅ Duplicate detection & removal  
✅ Config-driven orchestration  
✅ Database error logging  
✅ Real data testing (16K+ records)

### Upcoming Phases
🔄 Aggregation utilities  
🔄 Join/merge utilities  
🔄 Export utilities  
🔄 Airflow DAG tasks  
🔄 Master orchestration DAG

## 📈 Performance Metrics

| Operation | Records | Time | Performance |
|-----------|---------|------|-------------|
| Data loading | 85,152 | < 0.5s | ⚡ Fast |
| Type detection | 16,029 | < 0.3s | ⚡ Fast |
| Date standardization | 16,029 | < 0.5s | ⚡ Fast |
| Missing value fill | 1,586 | < 0.1s | ⚡ Fast |
| Complete pipeline | 16,029 | < 2s | ⚡ Fast |

## 📝 Dependencies

**Core Packages:**
- airflow (2.8.3)
- pandas (2.2.x)
- sqlalchemy (2.0.x)
- psycopg2 (PostgreSQL driver)
- pydantic (validation)
- pyyaml (configuration)
- dateutil (date parsing)
- numpy (numerical operations)

See [requirements.txt](requirements.txt) for complete list (20 packages).

## 🔐 Error Handling

All errors logged to PostgreSQL `rejected_records` table:

```
source_table | record_id | error_type | error_details | rejected_data | created_at
customers   | cust_001  | duplicate  | Duplicate email | {...}       | 2026-01-13
```

Query errors:
```sql
SELECT * FROM rejected_records WHERE source_table = 'customers';
SELECT COUNT(*) FROM rejected_records WHERE error_type = 'duplicate';
```

## 📖 Usage Examples

### Example 1: Clean Customer Data
```python
from scripts.utils import ConfigDrivenCleaner
from scripts.utils import DatabaseManager

db = DatabaseManager()
cleaner = ConfigDrivenCleaner('config/customers_config.yaml', db)

# Clean and save
cleaned_df, report = cleaner.run_complete_pipeline(
    'data/raw/dataset/Customers.csv',
    'data/processed/customers_cleaned.csv'
)

print(f"Cleaned {len(cleaned_df)} records")
print(f"Removed {report['rows_removed']} duplicate/invalid rows")
```

### Example 2: Validate Data Quality
```python
from scripts.utils import DataValidator, DataTransformer

# Validate
types = DataValidator.detect_data_types(df)
for col in df.columns:
    nulls = df[col].isnull().sum()
    print(f"{col}: {types[col]} type, {nulls} nulls")

# Transform
df['Age'] = DataTransformer.safe_typecast(df['Age'], 'int')
df['Date'], failed = DataTransformer.standardize_date_format(
    df['Date'],
    ['%Y-%m-%d'],
    '%d-%m-%Y'
)
```

### Example 3: Handle Missing Data
```python
from scripts.utils import MissingDataHandler

# Analyze
analysis = MissingDataHandler.analyze_missing(df)

# Fill by strategy
strategies = {
    'Age': {'strategy': 'mean'},
    'Email': {'strategy': 'custom', 'value': 'unknown@example.com'},
    'Notes': {'strategy': 'custom', 'value': 'N/A'}
}
filled_df, counts = MissingDataHandler.fill_by_strategy(df, strategies)
```

## 🛠️ Troubleshooting

### Issue: PostgreSQL Connection Failed
```
Solution: Check Docker container is running
docker ps
docker-compose up -d  # If not running
```

### Issue: CSV File Not Found
```
Solution: Verify file location
ls data/raw/dataset/
# Should see: Customers.csv, Sales.csv, Products.csv, Stores.csv, Exchange_Rates.csv
```

### Issue: Type Conversion Failed
```
Solution: Use 'coerce' option to convert failures to NaN
df['Age'] = DataTransformer.safe_typecast(df['Age'], 'int', 'coerce')
```

## 📞 Support

For issues or questions:
1. Check documentation in `docs/` directory
2. Review test results in test output
3. Check database error logs: `SELECT * FROM rejected_records;`
4. Review Phase 1 & 2 summary documents

## 📄 License

Internal Infosys Project - All Rights Reserved

## 🎯 Next Steps

1. **Phase 3:** Create aggregation utilities (T0013-T0015)
2. **Phase 4:** Build Airflow DAG tasks (T0016)
3. **Phase 5:** Create master orchestration DAG (T0017)
4. **Phase 6:** Error recovery and monitoring (T0018-T0032)

---

**Last Updated:** 2026-01-13  
**Status:** ✅ Phase 2 Complete | All Tests Passing | Production Ready  
**Next Phase:** Phase 3 (Aggregations & Advanced Transformations)
