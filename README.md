# Amazon Order ETL Pipeline with Apache Airflow

An automated ETL (Extract, Transform, Load) pipeline built with Apache Airflow for processing Amazon order data with advanced data quality, transformation, and loading capabilities.

## Features

### Core ETL Pipeline (T0007)
- **Extract**: Reads Amazon order data from CSV/Excel files with automatic format detection
- **Transform**: 10+ data cleaning and transformation operations with config-driven rules
- **Load**: Multi-target output (CSV, Excel, PostgreSQL) with robust error handling

### Data Cleaning & Transformation (T0008-T0017)
- ✅ **T0008**: Reusable cleaning utilities (trim, fillna, typecast)
- ✅ **T0009**: Incorrect data type handling (phone standardization, numeric conversion)
- ✅ **T0010**: Duplicate detection and removal by OrderID
- ✅ **T0011**: Missing data strategies (mean/median fill, forward/backward fill, drop)
- ✅ **T0012**: YAML-driven cleaning rules for flexible configuration
- ✅ **T0013**: Aggregations (groupBy, sum, min, max) for order statistics
- ✅ **T0014**: Normalization & scaling (min-max, standard)
- ✅ **T0015**: Feature engineering (order categories, age groups)
- ✅ **T0016**: Date/time transformations (year, month, day_of_week)
- ✅ **T0017**: Config-based transformation rules via YAML

### Advanced Load Operations (T0018-T0022)
- ✅ **T0018**: Bulk load operations with configurable chunk size (default: 1000 rows)
- ✅ **T0019**: Incremental vs Full load modes
- ✅ **T0020**: Constraint violation handling with row-level isolation
- ✅ **T0021**: Upsert logic for insert-or-skip based on primary keys
- ✅ **T0022**: Reject table for failed records with error context logging

## Data Transformations (Amazon Orders)

1. **Empty/Whitespace Handling**: Converts empty strings to NaN (T0008)
2. **Text Trimming**: Strips leading/trailing whitespace from all string columns (T0008)
3. **Missing Data**: Fills CustomerName with "Unknown", Age with mean (T0011)
4. **Type Handling**: Converts Age to int, standardizes phone numbers (T0009)
5. **Duplicate Removal**: Removes duplicate orders by OrderID (T0010)
6. **Date Processing**: Parses OrderDate, sorts descending, extracts year/month/day (T0016)
7. **Order Aggregations**: Global and state-level statistics (count, sum, min, max) (T0013)
8. **Normalization**: Min-max scaling for TotalAmount (T0014)
9. **Feature Engineering**: Order amount categories, customer age groups (T0015)
10. **Config-Driven Rules**: YAML-based cleaning and transformation rules (T0012, T0017)

## Project Structure

```
Airflow/
├── dags/
│   └── amazon_etl_dag.py        # Main Amazon ETL DAG
├── scripts/
│   ├── Extract.py               # Data extraction (T0007)
│   ├── TransformAmazon.py       # Amazon transformations (T0008-T0017)
│   ├── Load.py                  # Multi-target loading (T0007, T0018-T0022)
│   ├── cleaning_utils.py        # Reusable cleaning utilities (T0008, T0011)
│   └── config_loader.py         # YAML/JSON config loader (T0012)
├── data/
│   ├── raw/
│   │   └── amazon.csv           # Source Amazon orders
│   ├── processed/
│   │   ├── amazon_cleaned_data.csv
│   │   ├── orders_summary.csv
│   │   └── rejected_records.csv # Failed records (T0022)
│   └── staging/                 # Temporary files
├── config/
│   ├── amazon_cleaning_rules.yaml    # Cleaning config (T0012)
│   └── amazon_etl_config.yaml        # Transform config (T0017)
├── data_models/
│   └── models.py                # Data model definitions
├── docs/
│   └── Implementation_Snippets.md    # Task documentation (T0007-T0022)
├── tests/
│   └── test_etl_pipeline.py     # Unit tests for utilities
├── Docker/
│   ├── docker-compose.yaml      # Airflow services
│   ├── DOCKER_SETUP.md         # Docker setup guide
│   └── start_airflow.ps1       # Windows startup script
└── logs/                        # Airflow task logs
```

## Prerequisites

- Docker Desktop
- Python 3.8+
- Git

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/SammyBoy-09/Airflow.git
cd Airflow
```

### 2. Start Airflow Services

```bash
cd Docker
docker-compose up -d
```

This will start:
- **PostgreSQL**: Database for Airflow metadata and processed data
- **Redis**: Message broker for task queuing
- **Airflow Webserver**: UI accessible at http://localhost:8080
- **Airflow Scheduler**: DAG scheduling and execution

### 3. Access Airflow UI

Open your browser and navigate to: http://localhost:8080

Default credentials (configure in `.env`):
- Username: `airflow`
- Password: `airflow`

### 4. Place Source Data

Put your Amazon order data file in:
```
data/raw/amazon.csv
```

## Usage

### Running the ETL Pipeline

#### Manual Trigger
1. Go to http://localhost:8080
2. Find the `amazon_etl` DAG
3. Click the play button to trigger manually

#### CLI Trigger
```bash
docker exec docker-webserver-1 airflow dags trigger amazon_etl
```

#### Scheduled Runs
The DAG runs automatically on a daily schedule at midnight (configurable).

### Configuration Options

#### Load Type (T0019: Full vs Incremental)
In `dags/amazon_etl_dag.py`, adjust the load mode:

```python
load_type="full"        # T0019: Replaces entire table
# OR
load_type="incremental" # T0019: Appends new records
```

#### Bulk Load Chunk Size (T0018)
Adjust the batch size for better performance:

```python
bulk_chunk_size=1000  # Default: 1000 rows per batch
bulk_chunk_size=500   # Smaller batches for memory-constrained systems
bulk_chunk_size=5000  # Larger batches for faster loading
```

#### Upsert Mode (T0021)
Enable upsert logic to handle constraint violations:

```python
upsert_key="CustomerID"  # T0021: Enable upsert with primary key
# OR
upsert_key=None          # Disable upsert, use standard bulk load
```

### Checking Task Status

```bash
docker exec docker-webserver-1 airflow tasks states-for-dag-run amazon_etl <run_id>
```

### Viewing Logs

Logs are stored in:
```
logs/dag_id=amazon_etl/
```

Or view in the Airflow UI under each task.

## Output Files

After successful execution:

- **CSV**: `data/processed/amazon_cleaned_data.csv`
- **Excel**: `data/processed/amazon_cleaned_data.xlsx` (optional)
- **PostgreSQL Table**: `customers_cleaned`
- **Order Summary**: `data/processed/orders_summary.csv`
- **Rejected Records**: `data/processed/rejected_records.csv` (T0022)
- **Reject Table**: PostgreSQL `rejected_records` table with error context (T0022)

## Database Connection

The pipeline connects to PostgreSQL:

```
Host: postgres (Docker network)
Port: 5432
Database: airflow
Username: airflow
Password: airflow
```

Access via pgAdmin or CLI:
```bash
docker exec -it docker-postgres-1 psql -U airflow -d airflow
```

Query the data:
```sql
SELECT * FROM customers_cleaned LIMIT 10;
```

## Implemented Tasks (T0007-T0022)

### Extract & Load (T0007)
- CSV/Excel file reading with format auto-detection
- Multi-target output (CSV, Excel, PostgreSQL)

### Transform Pipeline (T0008-T0017)
- **T0008**: Reusable cleaning utilities (`cleaning_utils.py`)
- **T0009**: Data type handling and phone standardization
- **T0010**: Duplicate removal by OrderID
- **T0011**: Missing data strategies (mean/median/drop)
- **T0012**: YAML-driven cleaning rules
- **T0013**: Aggregations (groupBy, sum, min, max)
- **T0014**: Min-max normalization
- **T0015**: Feature engineering (categories, age groups)
- **T0016**: Date/time transformations
- **T0017**: Config-based transformation rules

### Load Pipeline (T0018-T0022)
- **T0018**: Bulk load with configurable chunk size
- **T0019**: Incremental vs full load modes
- **T0020**: Constraint violation handling
- **T0021**: Upsert logic with primary key matching
- **T0022**: Reject table with error logging

See [docs/Implementation_Snippets.md](docs/Implementation_Snippets.md) for detailed code examples.

## Future Enhancements

### Orchestration (T0023-T0027) - Planned
- Master DAG for multi-pipeline orchestration
- Event-driven DAG triggering (file sensors)
- Multi-DAG dependency management
- Backfill & catchup features
- Advanced failure handling strategies

### Smart Incremental Loading (Option 2)
Currently documented in `scripts/Load.py` as TODO:

- Metadata table for tracking last load timestamps
- Date-based filtering for new/updated records only
- Automatic first-run detection
- Estimated effort: ~30-35 lines across 3 files

## Troubleshooting

### Containers Not Starting
```bash
docker-compose down
docker-compose up -d
```

### DAG Not Appearing
- Check for syntax errors in `dags/amazon_etl_dag.py`
- Verify the DAG is in the `/opt/airflow/dags` folder
- Wait 30 seconds for scheduler to parse DAGs

### Task Failures
- Check logs in Airflow UI
- Verify source data file exists: `data/raw/amazon.csv`
- Check PostgreSQL connection
- Review rejected records in `data/processed/rejected_records.csv` (T0022)

### Permission Issues
```bash
sudo chown -R $(id -u):$(id -g) .
```

## Technologies Used

- **Apache Airflow 2.8.3**: Workflow orchestration
- **PostgreSQL 15**: Data storage
- **Redis 7**: Task queue
- **Pandas**: Data manipulation
- **SQLAlchemy**: Database ORM
- **Docker**: Containerization
- **Python 3.8**: Core language

