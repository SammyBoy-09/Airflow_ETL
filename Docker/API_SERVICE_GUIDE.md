# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 7: REST API Service - Docker Integration
# Tasks: T0033-T0037
# ═══════════════════════════════════════════════════════════════════════

# REST API Service - Quick Start Guide

## 🎯 Overview

The REST API service is now integrated into the Docker Compose setup. It provides monitoring and management capabilities for Airflow ETL pipelines.

## 🚀 Starting the Services

### Start Everything (Recommended)
```powershell
cd D:\sam\Projects\Infosys\Airflow\Docker
docker-compose up -d
```

This will start:
- **PostgreSQL** (port 5434)
- **Redis** (cache)
- **Airflow Webserver** (port 8080)
- **Airflow Scheduler**
- **REST API** (port 8000) ⭐ NEW

### Start Only the API
```powershell
docker-compose up -d postgres redis api
```

### Check Service Status
```powershell
docker-compose ps
```

## 📡 Accessing Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin/admin |
| **REST API** | http://localhost:8000 | API Key required |
| API Docs (Swagger) | http://localhost:8000/docs | - |
| API Docs (ReDoc) | http://localhost:8000/redoc | - |

## 🔐 API Authentication

The API uses API key authentication via the `X-API-Key` header.

**Default API Keys:**
- `dev-key-12345`
- `test-key-67890`

### Example Requests

#### Health Check (No Auth)
```powershell
Invoke-RestMethod -Uri "http://localhost:8000/health"
```

#### List DAGs (Authenticated)
```powershell
$headers = @{"X-API-Key" = "dev-key-12345"}
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/dags" -Headers $headers
```

#### Get DAG Status
```powershell
$headers = @{"X-API-Key" = "dev-key-12345"}
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/dags/etl_customers/status" -Headers $headers
```

#### Get Metadata Summary
```powershell
$headers = @{"X-API-Key" = "dev-key-12345"}
Invoke-RestMethod -Uri "http://localhost:8000/api/v1/metadata/summary" -Headers $headers
```

## 📊 Available Endpoints

### Public Endpoints
- `GET /` - API information
- `GET /health` - Health check with database status

### DAG Endpoints (Authenticated)
- `GET /api/v1/dags` - List all DAGs
- `GET /api/v1/dags/{dag_id}/status` - Get DAG status
- `GET /api/v1/dags/{dag_id}/runs` - Get DAG runs (paginated)
- `GET /api/v1/dags/{dag_id}/runs/{run_id}` - Get specific run
- `GET /api/v1/dags/{dag_id}/runs/{run_id}/tasks` - Get task instances

### Metadata Endpoints (Authenticated)
- `GET /api/v1/metadata/summary` - Complete metadata summary
- `GET /api/v1/metadata/tables` - Table statistics
- `GET /api/v1/metadata/metrics` - Pipeline metrics

### Log Endpoints (Authenticated)
- `GET /api/v1/logs/dags/{dag_id}` - List available logs
- `GET /api/v1/logs/dags/{dag_id}/runs/{run_id}` - Get DAG run logs
- `GET /api/v1/logs/dags/{dag_id}/runs/{run_id}/tasks/{task_id}` - Get task logs

## ⚙️ Configuration

### Environment Variables

Set these in `.env` file or directly in `docker-compose.yaml`:

```env
# API Authentication
API_KEY_ENABLED=true
API_KEYS=dev-key-12345,test-key-67890,prod-key-xyz

# Pagination
DEFAULT_PAGE_SIZE=50
MAX_PAGE_SIZE=500

# CORS (for web frontends)
CORS_ORIGINS=http://localhost:3000,http://localhost:8080,*
```

### Custom API Keys

Edit `Docker/docker-compose.yaml`:
```yaml
api:
  environment:
    API_KEYS: "your-custom-key-1,your-custom-key-2"
```

Then restart:
```powershell
docker-compose restart api
```

## 🔍 Monitoring & Logs

### View API Logs
```powershell
docker-compose logs -f api
```

### View All Service Logs
```powershell
docker-compose logs -f
```

### Check API Health
```powershell
# PowerShell
Invoke-RestMethod -Uri "http://localhost:8000/health"

# curl
curl http://localhost:8000/health
```

## 🛠️ Troubleshooting

### API Not Starting
```powershell
# Check logs
docker-compose logs api

# Restart API service
docker-compose restart api
```

### Database Connection Issues
```powershell
# Ensure PostgreSQL is running
docker-compose ps postgres

# Restart all services
docker-compose restart
```

### Port Conflicts
If port 8000 is in use, edit `docker-compose.yaml`:
```yaml
api:
  ports:
    - "8001:8000"  # Changed from 8000:8000
```

## 🧪 Testing the API

### Using Swagger UI (Recommended)
1. Open http://localhost:8000/docs
2. Click "Authorize" button
3. Enter API key: `dev-key-12345`
4. Test any endpoint with "Try it out"

### Using PowerShell
```powershell
# Run the test guide
cd D:\sam\Projects\Infosys\Airflow
.\scripts\api\TEST_GUIDE.ps1
```

### Using curl
```bash
# Health check
curl http://localhost:8000/health

# List DAGs
curl -H "X-API-Key: dev-key-12345" http://localhost:8000/api/v1/dags

# Get metadata
curl -H "X-API-Key: dev-key-12345" http://localhost:8000/api/v1/metadata/summary
```

## 📦 Docker Commands Reference

```powershell
# Start all services
docker-compose up -d

# Stop all services
docker-compose down

# Restart API only
docker-compose restart api

# View API logs
docker-compose logs -f api

# Check service status
docker-compose ps

# Rebuild API service (after code changes)
docker-compose up -d --build api

# Stop and remove all containers, networks, volumes
docker-compose down -v
```

## 🎉 What's New

The Docker setup now includes:
- ✅ **FastAPI REST API** on port 8000
- ✅ **Auto-installed dependencies** (FastAPI, SQLAlchemy, uvicorn)
- ✅ **Health checks** for reliability
- ✅ **Read-only volume mounts** for security
- ✅ **Environment-based configuration**
- ✅ **Automatic restart** on failure
- ✅ **Integration with existing PostgreSQL**

## 📚 Additional Resources

- **API Documentation**: [scripts/api/README.md](../scripts/api/README.md)
- **Sprint 7 Summary**: [SPRINT7_COMPLETION_SUMMARY.md](../SPRINT7_COMPLETION_SUMMARY.md)
- **Docker Setup**: [DOCKER_SETUP_GUIDE.md](DOCKER_SETUP_GUIDE.md)

---

**Team 1 - Sprint 7 Complete** 🎉
Tasks: T0033-T0037
