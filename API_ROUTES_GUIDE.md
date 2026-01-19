# ═══════════════════════════════════════════════════════════════════════
# REST API - COMPLETE ROUTES GUIDE
# Team 1 - Sprint 7 (Tasks T0033-T0037)
# ═══════════════════════════════════════════════════════════════════════

# 📡 Airflow ETL Pipeline REST API - Complete Routes Documentation

## 🎯 Base Information

**Base URL**: `http://localhost:8000`  
**Version**: v1.0.0  
**Authentication**: API Key (X-API-Key header)  
**Default API Keys**: `dev-key-12345`, `test-key-67890`

---

## 📋 Table of Contents

1. [Public Endpoints](#public-endpoints)
2. [DAG Endpoints](#dag-endpoints)
3. [Metadata Endpoints](#metadata-endpoints)
4. [Log Endpoints](#log-endpoints)
5. [Request Examples](#request-examples)
6. [Response Schemas](#response-schemas)
7. [Error Handling](#error-handling)

---

## 🌐 PUBLIC ENDPOINTS

These endpoints do **NOT** require authentication.

### 1. Root Endpoint
**GET** `/`

Get API information and version.

**Response:**
```json
{
  "name": "Airflow ETL Pipeline API",
  "version": "1.0.0",
  "description": "REST API for monitoring and managing Airflow ETL pipelines",
  "documentation": "/docs"
}
```

**Example:**
```powershell
Invoke-RestMethod -Uri "http://localhost:8000/"
```

---

### 2. Health Check
**GET** `/health`

Check API health and database connectivity.

**Response:**
```json
{
  "status": "healthy",
  "version": "1.0.0",
  "timestamp": "2026-01-19T14:30:00.123456",
  "database_connected": true
}
```

**Status Values:**
- `healthy`: API and database are operational
- `unhealthy`: Database connection failed

**Example:**
```powershell
Invoke-RestMethod -Uri "http://localhost:8000/health"
```

---

## 🔒 DAG ENDPOINTS

**Authentication Required**: All endpoints require `X-API-Key` header  
**Base Path**: `/api/v1/dags`

### 1. List All DAGs
**GET** `/api/v1/dags`

Get a list of all DAGs in the Airflow system.

**Response:**
```json
[
  {
    "dag_id": "etl_customers",
    "is_paused": false,
    "is_active": true,
    "last_parsed_time": "2026-01-19T14:00:00",
    "last_run_date": "2026-01-19T12:00:00",
    "next_run_date": "2026-01-19T15:00:00",
    "schedule_interval": "0 * * * *",
    "description": "Customer data ETL pipeline",
    "tags": ["etl", "customers"],
    "owners": ["airflow"]
  }
]
```

**Example:**
```powershell
$headers = @{"X-API-Key" = "dev-key-12345"}
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/dags" -Headers $headers
```

---

### 2. Get DAG Status
**GET** `/api/v1/dags/{dag_id}/status`

Get current status and latest run information for a specific DAG.

**Path Parameters:**
- `dag_id` (string, required): DAG identifier (e.g., `etl_customers`)

**Response:**
```json
{
  "dag_info": {
    "dag_id": "etl_customers",
    "description": "Customer data ETL pipeline",
    "schedule_interval": "0 * * * *",
    "is_paused": false,
    "is_active": true,
    "tags": ["etl", "customers"],
    "owners": ["airflow"],
    "last_run_date": "2026-01-19T12:00:00",
    "next_run_date": "2026-01-19T15:00:00"
  },
  "latest_run": {
    "dag_id": "etl_customers",
    "run_id": "manual__2026-01-19T12:00:00+00:00",
    "execution_date": "2026-01-19T12:00:00",
    "start_date": "2026-01-19T12:00:05",
    "end_date": "2026-01-19T12:05:30",
    "state": "success",
    "run_type": "manual",
    "duration_seconds": 325.0,
    "conf": {}
  },
  "task_instances": [
    {
      "task_id": "extract_customers",
      "dag_id": "etl_customers",
      "run_id": "manual__2026-01-19T12:00:00+00:00",
      "state": "success",
      "start_date": "2026-01-19T12:00:05",
      "end_date": "2026-01-19T12:02:30",
      "duration_seconds": 145.5,
      "try_number": 1
    }
  ],
  "total_runs": 47,
  "success_count": 45,
  "failed_count": 2
}
```

**Example:**
```powershell
$headers = @{"X-API-Key" = "dev-key-12345"}
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/dags/etl_customers/status" -Headers $headers
```

---

### 3. Get DAG Runs (Paginated)
**GET** `/api/v1/dags/{dag_id}/runs`

Get paginated list of DAG runs with optional filtering.

**Path Parameters:**
- `dag_id` (string, required): DAG identifier

**Query Parameters:**
- `page` (integer, default: 1): Page number (1-indexed)
- `page_size` (integer, default: 50, max: 500): Items per page
- `state` (string, optional): Filter by run state (`success`, `failed`, `running`)
- `start_date` (datetime, optional): Filter runs after this date
- `end_date` (datetime, optional): Filter runs before this date

**Response:**
```json
{
  "items": [
    {
      "run_id": "manual__2026-01-19T12:00:00",
      "dag_id": "etl_customers",
      "execution_date": "2026-01-19T12:00:00",
      "start_date": "2026-01-19T12:00:05",
      "end_date": "2026-01-19T12:05:30",
      "state": "success",
      "run_type": "manual",
      "external_trigger": false
    }
  ],
  "total": 47,
  "page": 1,
  "page_size": 50,
  "total_pages": 1,
  "has_next": false,
  "has_previous": false
}
```

**Examples:**
```powershell
# Get all runs
$headers = @{"X-API-Key" = "dev-key-12345"}
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/dags/etl_customers/runs" -Headers $headers

# Get only failed runs
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/dags/etl_customers/runs?state=failed" -Headers $headers

# Pagination
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/dags/etl_customers/runs?page=2&page_size=20" -Headers $headers
```

---

### 4. Get Specific DAG Run
**GET** `/api/v1/dags/{dag_id}/runs/{run_id}`

Get detailed information about a specific DAG run.

**Path Parameters:**
- `dag_id` (string, required): DAG identifier
- `run_id` (string, required): Run identifier

**Response:**
```json
{
  "run_id": "manual__2026-01-19T12:00:00",
  "dag_id": "etl_customers",
  "execution_date": "2026-01-19T12:00:00",
  "start_date": "2026-01-19T12:00:05",
  "end_date": "2026-01-19T12:05:30",
  "state": "success",
  "run_type": "manual",
  "external_trigger": false,
  "conf": {}
}
```

**Example:**
```powershell
$headers = @{"X-API-Key" = "dev-key-12345"}
$run_id = "manual__2026-01-19T12:00:00"
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/dags/etl_customers/runs/$run_id" -Headers $headers
```

---

### 5. Get Task Instances for a Run
**GET** `/api/v1/dags/{dag_id}/runs/{run_id}/tasks`

Get all task instances for a specific DAG run.

**Path Parameters:**
- `dag_id` (string, required): DAG identifier
- `run_id` (string, required): Run identifier

**Response:**
```json
[
  {
    "task_id": "extract_customers",
    "dag_id": "etl_customers",
    "execution_date": "2026-01-19T12:00:00",
    "start_date": "2026-01-19T12:00:05",
    "end_date": "2026-01-19T12:02:30",
    "duration": 145.5,
    "state": "success",
    "try_number": 1,
    "max_tries": 3,
    "operator": "PythonOperator",
    "pool": "default_pool",
    "queue": "default"
  }
]
```

**Example:**
```powershell
$headers = @{"X-API-Key" = "dev-key-12345"}
$run_id = "manual__2026-01-19T12:00:00"
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/dags/etl_customers/runs/$run_id/tasks" -Headers $headers
```

---

## 📊 METADATA ENDPOINTS

**Authentication Required**: All endpoints require `X-API-Key` header  
**Base Path**: `/api/v1/metadata`

### 1. Get Metadata Summary
**GET** `/api/v1/metadata/summary`

Get comprehensive pipeline metadata including DAG statistics and table information.

**Response:**
```json
{
  "pipeline_name": "Airflow ETL Pipeline API",
  "total_dags": 7,
  "active_dags": 6,
  "paused_dags": 1,
  "total_runs_today": 24,
  "successful_runs_today": 22,
  "failed_runs_today": 2,
  "tables": [
    {
      "table_name": "customers_cleaned",
      "schema": "etl_output",
      "row_count": 15420,
      "last_updated": "2026-01-19T12:05:30",
      "columns": ["customer_id", "name", "email", "phone"]
    }
  ],
  "last_refresh": "2026-01-19T14:30:00"
}
```

**Example:**
```powershell
$headers = @{"X-API-Key" = "dev-key-12345"}
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/metadata/summary" -Headers $headers
```

---

### 2. Get Table Statistics
**GET** `/api/v1/metadata/tables`

Get statistics for all tables in the ETL output schema.

**Response:**
```json
[
  {
    "table_name": "customers_cleaned",
    "schema": "etl_output",
    "row_count": 15420,
    "last_updated": "2026-01-19T12:05:30",
    "columns": ["customer_id", "name", "email", "phone"]
  },
  {
    "table_name": "sales_cleaned",
    "schema": "etl_output",
    "row_count": 45680,
    "last_updated": "2026-01-19T12:08:15",
    "columns": ["sale_id", "customer_id", "amount", "date"]
  }
]
```

**Example:**
```powershell
$headers = @{"X-API-Key" = "dev-key-12345"}
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/metadata/tables" -Headers $headers
```

---

### 3. Get Pipeline Metrics
**GET** `/api/v1/metadata/metrics`

Get key performance metrics for the pipeline.

**Response:**
```json
{
  "metrics": {
    "total_dags": 7,
    "active_dags": 6,
    "runs_today": 24,
    "success_rate": 91.67,
    "successful_runs": 22,
    "failed_runs": 2
  },
  "timestamp": "2026-01-19T14:30:00.123456"
}
```

**Example:**
```powershell
$headers = @{"X-API-Key" = "dev-key-12345"}
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/metadata/metrics" -Headers $headers
```

---

## 📝 LOG ENDPOINTS

**Authentication Required**: All endpoints require `X-API-Key` header  
**Base Path**: `/api/v1/logs`

### 1. List Available Logs
**GET** `/api/v1/logs/dags/{dag_id}`

List all available log files for a DAG.

**Path Parameters:**
- `dag_id` (string, required): DAG identifier

**Response:**
```json
{
  "dag_id": "etl_customers",
  "log_files": [
    {
      "run_id": "manual__2026-01-19T12:00:00",
      "task_id": "extract_customers",
      "try_number": 1,
      "path": "/opt/airflow/logs/dag_id=etl_customers/run_id=manual__2026-01-19T12:00:00/task_id=extract_customers/attempt=1.log",
      "size": 4582
    }
  ],
  "total_logs": 1
}
```

**Example:**
```powershell
$headers = @{"X-API-Key" = "dev-key-12345"}
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/logs/dags/etl_customers" -Headers $headers
```

---

### 2. Get DAG Run Logs
**GET** `/api/v1/logs/dags/{dag_id}/runs/{run_id}`

Fetch logs for a specific DAG run.

**Path Parameters:**
- `dag_id` (string, required): DAG identifier
- `run_id` (string, required): Run identifier

**Query Parameters:**
- `max_lines` (integer, optional): Maximum number of log lines to return (1-10000)

**Response:**
```json
{
  "dag_id": "etl_customers",
  "run_id": "manual__2026-01-19T12:00:00",
  "logs": "[2026-01-19 12:00:05] INFO - Starting DAG run...\n[2026-01-19 12:00:06] INFO - Extracting customer data...\n[2026-01-19 12:02:30] INFO - Extraction complete: 15,420 records\n",
  "truncated": false,
  "line_count": 45
}
```

**Example:**
```powershell
$headers = @{"X-API-Key" = "dev-key-12345"}
$run_id = "manual__2026-01-19T12:00:00"
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/logs/dags/etl_customers/runs/$run_id" -Headers $headers
```

---

### 3. Get Task Logs
**GET** `/api/v1/logs/dags/{dag_id}/runs/{run_id}/tasks/{task_id}`

Fetch logs for a specific task instance.

**Path Parameters:**
- `dag_id` (string, required): DAG identifier
- `run_id` (string, required): Run identifier
- `task_id` (string, required): Task identifier

**Query Parameters:**
- `try_number` (integer, default: 1): Task attempt number
- `max_lines` (integer, optional): Maximum number of log lines to return

**Response:**
```json
{
  "dag_id": "etl_customers",
  "run_id": "manual__2026-01-19T12:00:00",
  "task_id": "extract_customers",
  "try_number": 1,
  "logs": "[2026-01-19 12:00:05] INFO - Starting task execution...\n[2026-01-19 12:00:06] INFO - Connecting to database...\n[2026-01-19 12:02:30] INFO - Task completed successfully\n",
  "truncated": false,
  "line_count": 38
}
```

**Example:**
```powershell
$headers = @{"X-API-Key" = "dev-key-12345"}
$run_id = "manual__2026-01-19T12:00:00"
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/logs/dags/etl_customers/runs/$run_id/tasks/extract_customers" -Headers $headers
```

---

## 💡 REQUEST EXAMPLES

### PowerShell Examples

#### Basic Request with Authentication
```powershell
$headers = @{
    "X-API-Key" = "dev-key-12345"
    "Content-Type" = "application/json"
}

Invoke-RestMethod -Uri "http://localhost:8000/api/v1/dags" `
                  -Method Get `
                  -Headers $headers
```

#### With Query Parameters
```powershell
$headers = @{"X-API-Key" = "dev-key-12345"}
$params = @{
    page = 1
    page_size = 20
    state = "failed"
}
$queryString = ($params.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join "&"
$uri = "http://localhost:8000/api/v1/dags/etl_customers/runs?$queryString"

Invoke-RestMethod -Uri $uri -Headers $headers
```

#### Error Handling
```powershell
try {
    $headers = @{"X-API-Key" = "dev-key-12345"}
    $response = Invoke-RestMethod -Uri "http://localhost:8000/api/v1/dags/invalid_dag/status" -Headers $headers
} catch {
    $errorDetails = $_.ErrorDetails.Message | ConvertFrom-Json
    Write-Host "Error: $($errorDetails.detail)"
}
```

### curl Examples

#### Basic Request
```bash
curl -H "X-API-Key: dev-key-12345" \
     http://localhost:8000/api/v1/dags
```

#### With Query Parameters
```bash
curl -H "X-API-Key: dev-key-12345" \
     "http://localhost:8000/api/v1/dags/etl_customers/runs?page=1&page_size=20&state=failed"
```

#### Pretty Print JSON
```bash
curl -H "X-API-Key: dev-key-12345" \
     http://localhost:8000/api/v1/metadata/summary | python -m json.tool
```

### Python Examples

#### Using requests library
```python
import requests

# Configuration
BASE_URL = "http://localhost:8000"
API_KEY = "dev-key-12345"
headers = {"X-API-Key": API_KEY}

# Get all DAGs
response = requests.get(f"{BASE_URL}/api/v1/dags", headers=headers)
dags = response.json()

# Get DAG status
dag_id = "etl_customers"
response = requests.get(f"{BASE_URL}/api/v1/dags/{dag_id}/status", headers=headers)
status = response.json()
print(f"DAG {dag_id} status: {status['last_run']['state']}")

# Get paginated runs
params = {"page": 1, "page_size": 20, "state": "success"}
response = requests.get(
    f"{BASE_URL}/api/v1/dags/{dag_id}/runs",
    headers=headers,
    params=params
)
runs = response.json()
print(f"Total runs: {runs['total']}")
```

#### Using httpx (async)
```python
import httpx
import asyncio

async def get_dag_status(dag_id: str):
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"http://localhost:8000/api/v1/dags/{dag_id}/status",
            headers={"X-API-Key": "dev-key-12345"}
        )
        return response.json()

# Run async
status = asyncio.run(get_dag_status("etl_customers"))
print(status)
```

---

## 📦 RESPONSE SCHEMAS

### PaginatedResponse
Used for paginated list endpoints.

```json
{
  "items": [],           // Array of items
  "total": 100,          // Total number of items
  "page": 1,             // Current page (1-indexed)
  "page_size": 50,       // Items per page
  "total_pages": 2,      // Total number of pages
  "has_next": true,      // More pages available
  "has_previous": false  // Previous pages available
}
```

### ErrorResponse
Standard error response format.

```json
{
  "detail": "Error message describing what went wrong",
  "status_code": 404,
  "timestamp": "2026-01-19T14:30:00.123456"
}
```

---

## ⚠️ ERROR HANDLING

### HTTP Status Codes

| Code | Status | Description |
|------|--------|-------------|
| 200 | OK | Request successful |
| 400 | Bad Request | Invalid request parameters |
| 401 | Unauthorized | Missing authentication |
| 403 | Forbidden | Invalid API key |
| 404 | Not Found | Resource not found |
| 500 | Internal Server Error | Server error |
| 503 | Service Unavailable | Database connection failed |

### Error Response Format

All errors return a JSON response:

```json
{
  "detail": "DAG 'invalid_dag' not found",
  "status_code": 404,
  "timestamp": "2026-01-19T14:30:00.123456"
}
```

### Common Error Scenarios

#### 1. Missing API Key
```json
{
  "detail": "Missing API Key",
  "status_code": 401
}
```

#### 2. Invalid API Key
```json
{
  "detail": "Invalid API Key",
  "status_code": 403
}
```

#### 3. Resource Not Found
```json
{
  "detail": "DAG 'unknown_dag' not found",
  "status_code": 404
}
```

#### 4. Validation Error
```json
{
  "detail": [
    {
      "loc": ["query", "page"],
      "msg": "value is not a valid integer",
      "type": "type_error.integer"
    }
  ]
}
```

---

## 🔐 AUTHENTICATION

### API Key Header

All authenticated endpoints require the `X-API-Key` header:

```
X-API-Key: dev-key-12345
```

### Default API Keys

- `dev-key-12345` (Development)
- `test-key-67890` (Testing)

### Custom API Keys

Configure in environment variables or `docker-compose.yaml`:

```env
API_KEYS=your-custom-key-1,your-custom-key-2,your-custom-key-3
```

### Disabling Authentication

For development only:

```env
API_KEY_ENABLED=false
```

---

## 📊 RATE LIMITING

Currently, there is **no rate limiting** implemented. For production use, consider adding:

- Rate limiting middleware (e.g., `slowapi`)
- Per-key quotas
- IP-based throttling

---

## 🌐 CORS Configuration

Default allowed origins:
- `http://localhost:3000`
- `http://localhost:8080`
- `*` (all origins)

Configure in environment:
```env
CORS_ORIGINS=http://localhost:3000,http://localhost:8080
```

---

## 📚 INTERACTIVE DOCUMENTATION

### Swagger UI
**URL**: http://localhost:8000/docs

Features:
- Interactive API testing
- Schema exploration
- Built-in authentication
- Try it out functionality

### ReDoc
**URL**: http://localhost:8000/redoc

Features:
- Clean, readable documentation
- Printable format
- Search functionality
- Examples and schemas

---

## 🎯 QUICK START

### 1. Start the API
```powershell
# Using Docker
cd D:\sam\Projects\Infosys\Airflow\Docker
docker-compose up -d api

# Local development
cd D:\sam\Projects\Infosys\Airflow
$env:API_KEY_ENABLED="true"
$env:API_KEYS="dev-key-12345"
uvicorn scripts.api.main:app --reload --host 0.0.0.0 --port 8000
```

### 2. Test Health
```powershell
Invoke-RestMethod -Uri "http://localhost:8000/health"
```

### 3. Test Authenticated Endpoint
```powershell
$headers = @{"X-API-Key" = "dev-key-12345"}
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/dags" -Headers $headers
```

### 4. Explore Documentation
Open http://localhost:8000/docs in your browser

---

## 📞 SUPPORT

- **API Documentation**: http://localhost:8000/docs
- **Project Docs**: [scripts/api/README.md](scripts/api/README.md)
- **Docker Guide**: [Docker/API_SERVICE_GUIDE.md](Docker/API_SERVICE_GUIDE.md)
- **Sprint Summary**: [SPRINT7_COMPLETION_SUMMARY.md](SPRINT7_COMPLETION_SUMMARY.md)

---

**Team 1 - Sprint 7 Complete** 🎉  
**Tasks**: T0033-T0037  
**Generated**: January 19, 2026
