# Airflow ETL Pipeline Project

A complete ETL (Extract, Transform, Load) pipeline system built with Apache Airflow, featuring REST API access, web dashboard, and full Docker containerization.

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Accessing Services](#accessing-services)
- [Running DAGs](#running-dags)
- [Common Commands](#common-commands)
- [Troubleshooting](#troubleshooting)
- [Development](#development)
- [Documentation](#documentation)

## 🎯 Overview

This project implements a comprehensive ETL pipeline for processing e-commerce data (customers, products, stores, sales, exchange rates) with the following capabilities:

- **5 Source DAGs**: Individual pipelines for each data source
- **1 Master Orchestrator**: Coordinates all pipeline executions
- **1 Reporting DAG**: Generates 9 analytical reports
- **REST API**: 13 endpoints for monitoring and querying
- **Web Dashboard**: Interactive UI for easy pipeline monitoring
- **Full Containerization**: 6 Docker services working together

### Data Processing

- **Input**: CSV files from `data/raw/dataset/`
- **Output**: PostgreSQL database (`etl_output` schema)
- **Processing**: Data cleaning, transformation, validation, and reporting
- **Volume**: ~47,000+ records processed across 5 tables

## ✨ Features

### Sprint Coverage (8 Sprints Completed)

- ✅ **Sprint 2**: Data Quality & Cleaning
- ✅ **Sprint 3**: Advanced Transformations
- ✅ **Sprint 4**: Database Loading Strategies
- ✅ **Sprint 5**: DAG Orchestration
- ✅ **Sprint 6**: Combined E-T-L Pipelines
- ✅ **Sprint 7**: REST API & Monitoring
- ✅ **Sprint 8**: Docker Deployment

### Key Capabilities

- 🔄 Event-driven DAG triggering with ExternalTaskSensor
- 📊 9 automated business reports (customer segmentation, sales trends, product performance, etc.)
- 🔍 Data quality validation with rejected records tracking
- 🎯 Incremental and full load strategies
- 🚀 Bulk operations with optimized chunking
- 📈 Real-time monitoring via REST API
- 🌐 User-friendly web dashboard
- 🗄️ Database visualization with pgAdmin

## 🔧 Prerequisites

Before you begin, ensure you have the following installed:

- **Docker Desktop**: Version 20.10 or higher
- **Docker Compose**: Version 2.0 or higher
- **Git**: For cloning the repository
- **4GB RAM**: Minimum available memory for Docker
- **10GB Disk Space**: For images and volumes

### Check Installations

```powershell
# Check Docker
docker --version
docker-compose --version

# Check Docker is running
docker ps
```

## 🚀 Quick Start

### 1. Clone the Repository

```powershell
git clone https://github.com/SammyBoy-09/Airflow_ETL.git
cd Airflow_ETL
```

### 2. Prepare Data Files

Ensure your CSV data files are in the correct location:

```
data/raw/dataset/
├── Customers.csv
├── Products.csv
├── Stores.csv
├── Sales.csv
└── Exchange_Rates.csv
```

### 3. Start All Services

```powershell
cd Docker
docker-compose up -d
```

This single command will:
- Pull required Docker images (first time only)
- Create a PostgreSQL database
- Initialize Airflow metadata
- Start 6 services (Postgres, Redis, Airflow Webserver, Airflow Scheduler, REST API, pgAdmin)
- Set up networking and volumes

### 4. Wait for Initialization

Services take ~2-3 minutes to fully initialize. Check status:

```powershell
docker-compose ps
```

All services should show as "healthy" or "running".

### 5. Access the UI

Open your browser and navigate to:
- **Airflow UI**: http://localhost:8080
  - Username: `airflow`
  - Password: `airflow`

### 6. Start the Web Dashboard (Optional but Recommended)

The web dashboard provides a user-friendly interface for monitoring DAGs. Open a new terminal:

```powershell
# Activate Python environment

# Navigate to project root
cd \Airflow

# Start the Flask web dashboard
python scripts/api/web_dashboard.py
```

The dashboard will start on **http://localhost:5000** with output:
```
🚀 Airflow API Web Dashboard
API Endpoint: http://localhost:8000
Dashboard URL: http://localhost:5000
```

**Note**: Keep this terminal running to use the dashboard. The REST API (port 8000) must be running in Docker for the dashboard to work.

### 7. Trigger the Master DAG

In the Airflow UI (http://localhost:8080):
1. Find `etl_master_orchestrator` in the DAG list
2. Toggle the DAG to "On" (unpause)
3. Click the "Play" button → "Trigger DAG"

This will automatically run all 5 source pipelines and generate reports!

**Alternative**: Use the Web Dashboard at http://localhost:5000 for an easier monitoring experience!

## 📁 Project Structure

```
Airflow_ETL/
├── dags/                          # Airflow DAG definitions
│   ├── etl_master_orchestrator.py # Master coordination DAG
│   ├── etl_customers.py           # Customer dimension pipeline
│   ├── etl_products.py            # Product dimension pipeline
│   ├── etl_stores.py              # Store dimension pipeline
│   ├── etl_exchange_rates.py      # Exchange rate pipeline
│   ├── etl_sales.py               # Sales fact pipeline
│   ├── etl_reports.py             # Business reporting DAG
│   └── dag_base.py                # Shared configuration
│
├── scripts/                       # Utility modules
│   ├── Extract.py                 # Data extraction logic
│   ├── Transform.py               # Data transformation logic
│   ├── Load.py                    # Database loading logic
│   ├── ReportGenerator.py         # Report generation
│   ├── api/                       # REST API service
│   │   ├── main.py               # FastAPI application
│   │   ├── web_dashboard.py      # Flask web UI
│   │   ├── routes/               # API endpoints
│   │   ├── models/               # Pydantic models
│   │   └── utils/                # API utilities
│   └── utils/                     # Shared utilities
│
├── config/                        # YAML configurations
│   ├── customers_config.yaml
│   ├── products_config.yaml
│   ├── stores_config.yaml
│   ├── sales_config.yaml
│   └── exchange_rates_config.yaml
│
├── data/                          # Data directories
│   ├── raw/dataset/              # Source CSV files
│   ├── staging/                  # Intermediate processing
│   ├── processed/                # Cleaned data
│   └── reports/                  # Generated reports
│
├── Docker/                        # Docker configuration
│   ├── docker-compose.yaml       # Service orchestration
│   ├── Dockerfile                # Airflow image
│   ├── Dockerfile.api            # API service image
│   └── .env                      # Environment variables
│
└── docs/                          # Documentation
    └── Implementation_Snippets.md # Code reference
```

## 🌐 Accessing Services

After running `docker-compose up -d`, access these services:

| Service | URL | Credentials | Purpose |
|---------|-----|-------------|---------|
| **Airflow UI** | http://localhost:8080 | airflow / airflow | DAG management & monitoring |
| **REST API** | http://localhost:8000 | API Key: dev-key-12345 | Programmatic access |
| **API Docs** | http://localhost:8000/docs | - | Interactive API documentation |
| **Web Dashboard** | http://localhost:5000 | - | User-friendly monitoring UI (run `python scripts/api/web_dashboard.py`) |
| **pgAdmin** | http://localhost:5050 | admin@admin.com / admin | Database visualization |
| **PostgreSQL** | localhost:5434 | airflow / airflow | Direct database access |

### Connecting to PostgreSQL in pgAdmin

1. Open http://localhost:5050
2. Login with: `admin@admin.com` / `admin`
3. Right-click "Servers" → "Register" → "Server"
4. **General Tab**: Name = `Airflow DB`
5. **Connection Tab**:
   - Host: `postgres` (Docker network name)
   - Port: `5432` (internal port)
   - Database: `airflow`
   - Username: `airflow`
   - Password: `airflow`
6. Click "Save"

## 🎯 Running DAGs

### Option 1: Master Orchestrator (Recommended)

Runs all pipelines in the correct order:

```
1. Navigate to Airflow UI (http://localhost:8080)
2. Find "etl_master_orchestrator"
3. Toggle to ON (unpause)
4. Click Play → Trigger DAG
```

**Execution Order**:
1. Customers, Products, Stores, Exchange Rates (parallel)
2. Sales (waits for Products to complete)
3. Reports (waits for all pipelines to complete)

### Option 2: Individual DAGs

Run specific pipelines independently:

- `etl_customers` - Customer dimension
- `etl_products` - Product dimension
- `etl_stores` - Store dimension
- `etl_exchange_rates` - Exchange rate dimension
- `etl_sales` - Sales fact table
- `etl_reports` - Generate 9 business reports

### Option 3: API Trigger

```powershell
# Trigger via REST API
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/dags/etl_master_orchestrator/trigger" `
  -Method POST `
  -Headers @{"X-API-Key"="dev-key-12345"}
```

### Monitoring Execution

**Via Airflow UI**:
- Click on DAG name → Graph view
- See task status (green=success, red=failed, yellow=running)
- Click task → Logs to see execution details

**Via Web Dashboard**:
- Open http://localhost:5000
- Select DAG from dropdown
- View runs, tasks, and logs

**Via REST API**:
```powershell
# Get DAG status
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/dags/etl_master_orchestrator/status" `
  -Headers @{"X-API-Key"="dev-key-12345"}

# Get recent runs
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/dags/etl_master_orchestrator/runs?page_size=10" `
  -Headers @{"X-API-Key"="dev-key-12345"}
```

## 🛠️ Common Commands

### Docker Management

```powershell
# Start all services
cd Docker
docker-compose up -d

# Stop all services
docker-compose down

# View service status
docker-compose ps

# View logs for specific service
docker-compose logs -f airflow-webserver
docker-compose logs -f airflow-scheduler
docker-compose logs -f api

# View logs for all services
docker-compose logs -f

# Restart a specific service
docker-compose restart airflow-webserver

# Restart all services
docker-compose restart

# Stop and remove all containers + volumes (CAUTION: deletes database!)
docker-compose down -v

# Rebuild images after code changes
docker-compose build
docker-compose up -d
```

### Airflow CLI Commands

Execute commands inside the webserver container:

```powershell
# Access Airflow CLI
docker exec -it airflow-webserver bash

# List all DAGs
airflow dags list

# Trigger a specific DAG
airflow dags trigger etl_master_orchestrator

# Test a specific task
airflow tasks test etl_customers extract 2026-01-19

# Clear task state to re-run
airflow tasks clear etl_sales --yes

# Check DAG structure
airflow dags show etl_master_orchestrator

# List DAG runs
airflow dags list-runs -d etl_master_orchestrator

# Create Airflow user
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

### Database Access

```powershell
# Connect to PostgreSQL via Docker
docker exec -it airflow-postgres psql -U airflow -d airflow

# Common SQL queries
# List all tables
\dt etl_output.*

# Count records
SELECT 'customers' as table, COUNT(*) FROM etl_output.customers
UNION ALL
SELECT 'products', COUNT(*) FROM etl_output.products
UNION ALL
SELECT 'stores', COUNT(*) FROM etl_output.stores
UNION ALL
SELECT 'sales', COUNT(*) FROM etl_output.sales
UNION ALL
SELECT 'exchange_rates', COUNT(*) FROM etl_output.exchange_rates;

# Exit psql
\q
```

### Python Environment (Local Development)

```powershell
# Activate conda environment
conda activate KB_1978

# Install dependencies
pip install -r requirements.txt

# Run REST API locally (not in Docker)
cd scripts/api
uvicorn main:app --reload --port 8000

# Run Flask dashboard locally
python web_dashboard.py

# Run tests
python test_api.py
```

## 🐛 Troubleshooting

### Services Not Starting

**Problem**: Docker containers show as unhealthy

```powershell
# Check detailed logs
docker-compose logs

# Check specific service
docker-compose logs airflow-webserver

# Common fix: restart services
docker-compose down
docker-compose up -d
```

### Port Already in Use

**Problem**: Error "port is already allocated"

```powershell
# Check what's using the port
netstat -ano | findstr :8080
netstat -ano | findstr :5434

# Kill the process (replace PID)
taskkill /PID <PID> /F

# Or change port in Docker/.env
POSTGRES_PORT=5435  # Change to different port
```

### Database Connection Failed

**Problem**: "Could not connect to PostgreSQL"

1. Wait 2-3 minutes for full initialization
2. Check PostgreSQL health:
   ```powershell
   docker-compose ps postgres
   ```
3. Verify .env file has correct credentials
4. Restart postgres service:
   ```powershell
   docker-compose restart postgres
   ```

### DAG Not Appearing in UI

**Problem**: New DAG not visible

1. Check DAG for syntax errors:
   ```powershell
   docker exec -it airflow-webserver python /opt/airflow/dags/your_dag.py
   ```
2. Check scheduler logs:
   ```powershell
   docker-compose logs airflow-scheduler
   ```
3. Refresh DAG list (wait 30 seconds or restart scheduler)

### Task Failing with Import Errors

**Problem**: "ModuleNotFoundError"

1. Ensure module exists in `scripts/` or `scripts/utils/`
2. Check PYTHONPATH is set correctly in docker-compose.yaml
3. Rebuild Docker image:
   ```powershell
   docker-compose build airflow-webserver
   docker-compose up -d
   ```

### Out of Memory Errors

**Problem**: Container crashes with OOM

1. Increase Docker memory allocation:
   - Docker Desktop → Settings → Resources → Memory
   - Allocate at least 4GB
2. Reduce concurrent task execution in dag_base.py:
   ```python
   'max_active_runs': 1  # Reduce from 3
   ```

### API Returns 404 or 500 Errors

**Problem**: REST API endpoints not working

```powershell
# Check API container logs
docker-compose logs api

# Verify API is running
Invoke-RestMethod -Uri "http://localhost:8000/health"

# Check database connection from API
docker exec -it airflow-api python -c "from utils.airflow_client import AirflowClient; print('OK')"
```

### Volumes Not Persisting

**Problem**: Data lost after restart

1. Don't use `docker-compose down -v` (deletes volumes)
2. Use `docker-compose down` to preserve data
3. Check volume status:
   ```powershell
   docker volume ls
   docker volume inspect docker_postgres-db-volume
   ```

## 💻 Development

### Adding New DAGs

1. Create new DAG file in `dags/` directory
2. Import from `dag_base.py` for consistency:
   ```python
   from dag_base import DEFAULT_ARGS, DEFAULT_DAG_CONFIG, get_db_config
   ```
3. Test locally:
   ```powershell
   python dags/your_new_dag.py
   ```
4. Restart scheduler to pick up changes:
   ```powershell
   docker-compose restart airflow-scheduler
   ```

### Modifying Existing Code

For changes to `scripts/` or `dags/`:
- Files are mounted as volumes - changes reflect immediately
- No need to rebuild Docker images
- Restart scheduler if DAG structure changes

For changes to API code (`scripts/api/`):
- Restart API service:
  ```powershell
  docker-compose restart api
  ```

For changes to Docker configuration:
```powershell
# Rebuild and restart
docker-compose build
docker-compose up -d
```

### Testing Changes

```powershell
# Test a specific task without running full DAG
docker exec -it airflow-webserver \
  airflow tasks test etl_customers extract 2026-01-19

# Run Python scripts manually
docker exec -it airflow-webserver python /opt/airflow/scripts/Extract.py
```

## 📚 Documentation

Comprehensive documentation is available in the following files:

| Document | Description |
|----------|-------------|
| [PROJECT_TRACKER.md](PROJECT_TRACKER.md) | Master tracking with all sprints, tasks, and credentials |
| [TEAM1_TASK_TRACKER.md](TEAM1_TASK_TRACKER.md) | Detailed Team 1 implementation tracker |
| [TEAM2_TASK_TRACKER.md](TEAM2_TASK_TRACKER.md) | Team 2 planning tracker |
| [API_ROUTES_GUIDE.md](API_ROUTES_GUIDE.md) | Complete REST API reference (850+ lines) |
| [Docker/API_SERVICE_GUIDE.md](Docker/API_SERVICE_GUIDE.md) | Docker-specific API usage guide |
| [scripts/api/README.md](scripts/api/README.md) | API developer documentation |
| [docs/Implementation_Snippets.md](docs/Implementation_Snippets.md) | Code examples for all 42 tasks |

### Key Resources

- **Task Reference**: All 42 tasks (T0007-T0042) documented in Implementation_Snippets.md
- **API Examples**: 13 endpoints with curl/PowerShell examples in API_ROUTES_GUIDE.md
- **Configuration**: YAML files in `config/` directory for each data source
- **Credentials**: All access information in PROJECT_TRACKER.md

## 🎓 Learning Resources

### Airflow Concepts

- **DAG**: Directed Acyclic Graph - defines workflow
- **Task**: Single unit of work (extract, transform, load)
- **Operator**: Template for a task (PythonOperator, BashOperator)
- **Sensor**: Waits for condition (ExternalTaskSensor)
- **XCom**: Cross-communication between tasks

### Project Patterns

- **Master-Child DAGs**: Orchestrator triggers individual pipelines
- **ExternalTaskSensor**: Wait for upstream DAG completion
- **Task Groups**: Organize related tasks visually
- **Bulk Operations**: Process data in optimized chunks
- **Rejected Records**: Track and store failed validations

## 📊 Expected Output

After successful execution:

### Database Tables

```sql
-- etl_output schema contains:
customers         (15,266 rows)
products          (2,517 rows)
stores            (67 rows)
exchange_rates    (3,655 rows)
sales             (26,326 rows)
```

### Generated Reports

Located in `data/reports/`:

1. `customer_summary.csv` - Customer statistics
2. `customer_segmentation.csv` - RFM analysis
3. `order_status.csv` - Order tracking
4. `sales_trends_daily.csv` - Daily sales patterns
5. `product_performance.csv` - Top products
6. `store_performance.csv` - Store rankings
7. `anomaly_detection.csv` - Outlier identification
8. `data_quality_scorecard.csv` - Quality metrics
9. `dag_execution_summary.csv` - Pipeline performance

### Logs

- Task logs: `logs/dag_id={dag_name}/run_id={run_id}/task_id={task_name}/`
- Scheduler logs: `logs/scheduler/`

## 🤝 Contributing

This project was developed as part of Infosys training program. For questions or issues:

1. Check existing documentation
2. Review logs for error messages
3. Consult troubleshooting section
4. Contact project maintainers


## Quick Reference Card

```powershell
# START EVERYTHING
cd Docker
docker-compose up -d

# START WEB DASHBOARD (in separate terminal)
conda activate KB_1978
cd D:\sam\Projects\Infosys\Airflow
python scripts/api/web_dashboard.py

# CHECK STATUS
docker-compose ps

# VIEW LOGS
docker-compose logs -f

# STOP EVERYTHING
docker-compose down

# AIRFLOW UI
http://localhost:8080
airflow / airflow

# TRIGGER MASTER DAG
# Go to UI → etl_master_orchestrator → Play → Trigger

# CHECK RESULTS
# pgAdmin: http://localhost:5050
# Web Dashboard: http://localhost:5000
```

---

**Last Updated**: January 19, 2026  

