# Docker Setup & Deployment Guide

## 🐳 Overview

This guide explains how to run the complete Multi-Source ETL Pipeline using Docker Compose.

---

## 📋 Prerequisites

### System Requirements
- Docker Desktop 4.0+ installed and running
- Docker Compose 2.0+ (included with Docker Desktop)
- 4GB RAM available
- 2GB disk space for images and volumes

### Verify Installation
```bash
docker --version
docker-compose --version
```

---

## 🚀 Quick Start (3 Steps)

### Step 1: Navigate to Docker Directory
```bash
cd D:\sam\Projects\Infosys\Airflow\Docker
```

### Step 2: Start Services
```bash
docker-compose up -d
```

This will:
- Create and start PostgreSQL container
- Create and start Redis container
- Create and start Airflow Webserver
- Create and start Airflow Scheduler
- Initialize Airflow database
- Create admin user (admin/admin)

### Step 3: Verify Everything Started
```bash
docker-compose ps
```

Expected output:
```
NAME                           STATUS
docker-postgres-1              Up (healthy)
docker-redis-1                 Up (healthy)
docker-webserver-1             Up (healthy)
docker-scheduler-1             Up (healthy)
```

---

## 🌐 Access Services

### Airflow Web UI
- **URL:** http://localhost:8080
- **Username:** admin
- **Password:** admin

### PostgreSQL Database
- **Host:** localhost
- **Port:** 5432
- **Database:** airflow
- **Username:** airflow
- **Password:** airflow (from .env)

### Connect from Host
```bash
# Using psql (if installed)
psql -h localhost -U airflow -d airflow

# Using Python
python -c "import psycopg2; conn = psycopg2.connect('host=localhost user=airflow password=airflow dbname=airflow'); print('Connected!')"
```

---

## 📂 Architecture

### Services

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| postgres | postgres:15 | 5432 | Database for Airflow & ETL data |
| redis | redis:7 | 6379 | Cache & message broker |
| webserver | apache/airflow:2.8.3 | 8080 | Airflow UI & REST API |
| scheduler | apache/airflow:2.8.3 | - | DAG scheduler |

### Volumes

| Volume | Mount Point | Purpose |
|--------|-------------|---------|
| ../dags | /opt/airflow/dags | Airflow DAG definitions |
| ../logs | /opt/airflow/logs | Airflow execution logs |
| ../scripts | /opt/airflow/scripts | Python ETL scripts |
| ../data | /opt/airflow/data | Input/output data |
| ../config | /opt/airflow/config | Pipeline configuration |
| postgres-db | /var/lib/postgresql/data | PostgreSQL data persistence |
| redis-data | /data | Redis persistence |

### Network
All services connected via `etl-network` bridge network for inter-service communication.

---

## 🔧 Configuration

### Environment Variables (.env)

Located in `Docker/.env`:

```env
AIRFLOW_UID=50000
AIRFLOW_GID=50000
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
AIRFLOW__CORE__FERNET_KEY=lZIYHPYioP9lMFymp6vDwi_SArOuwh2gD3R8rD8FB0o=
```

### Customize Settings

Edit `Docker/.env` to change:
- PostgreSQL credentials
- Airflow admin user (see webserver command in docker-compose.yaml)
- Fernet key (generate new one for production)

### Generate New Fernet Key
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

---

## 📊 Working with the Pipeline

### Trigger a DAG
```bash
# Via Web UI
# 1. Go to http://localhost:8080
# 2. Find "unified_etl_dag"
# 3. Click "Trigger DAG"

# Or via CLI
docker-compose exec scheduler airflow dags trigger unified_etl_dag
```

### Monitor Execution
```bash
# Check logs
docker-compose logs -f scheduler

# See webserver logs
docker-compose logs -f webserver
```

### Execute Python Scripts Inside Container
```bash
# Extract data
docker-compose exec webserver python /opt/airflow/scripts/Extract.py

# Transform data
docker-compose exec webserver python /opt/airflow/scripts/TransformAmazon.py

# Load data
docker-compose exec webserver python /opt/airflow/scripts/Load.py
```

### Connect to PostgreSQL Inside Container
```bash
# Interactive psql session
docker-compose exec postgres psql -U airflow -d airflow

# Run SQL query
docker-compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM etl.output_customers_cleaned;"
```

---

## 🛠️ Common Operations

### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f scheduler
docker-compose logs -f webserver
docker-compose logs -f postgres
```

### Restart Services
```bash
# Restart all
docker-compose restart

# Restart specific service
docker-compose restart scheduler
docker-compose restart webserver
```

### Stop Services
```bash
# Stop all (keeps data)
docker-compose stop

# Stop specific service
docker-compose stop scheduler
```

### Start Services
```bash
# Start all
docker-compose start

# Start specific service
docker-compose start scheduler
```

### Full Reset (WARNING: Deletes data)
```bash
# Stop and remove containers, networks
docker-compose down

# Also remove volumes
docker-compose down -v

# Start fresh
docker-compose up -d
```

### View Service Status
```bash
docker-compose ps
docker-compose ps -a  # Include stopped
```

---

## 📝 Check Data in Database

### Via psql
```bash
# Connect to database
docker-compose exec postgres psql -U airflow -d airflow

# View tables
\dt etl.*

# Query data
SELECT * FROM etl.output_customers_cleaned LIMIT 5;
SELECT * FROM etl.rejected_records ORDER BY logged_at DESC LIMIT 5;
SELECT * FROM etl.pipeline_runs ORDER BY completed_at DESC LIMIT 5;

# Exit
\q
```

### Via Python Script
```bash
docker-compose exec webserver python -c "
import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host='postgres',
    user='airflow',
    password='airflow',
    database='airflow'
)

df = pd.read_sql('SELECT * FROM etl.output_customers_cleaned;', conn)
print(df)
"
```

---

## 🐛 Troubleshooting

### Services Not Starting

**Check logs:**
```bash
docker-compose logs postgres
docker-compose logs webserver
docker-compose logs scheduler
```

**Common issues:**
- Port 5432 or 8080 already in use (change in docker-compose.yaml)
- Insufficient disk space
- Docker daemon not running

### Database Connection Issues

**Test connection:**
```bash
docker-compose exec webserver python -c "
import psycopg2
conn = psycopg2.connect(host='postgres', user='airflow', password='airflow', database='airflow')
print('✓ Connected!')
"
```

### Airflow Not Creating Tables

**Check if scheduler initialized database:**
```bash
docker-compose logs scheduler | grep -i "init\|upgrade"
```

**Manually initialize:**
```bash
docker-compose exec webserver airflow db init
```

### High Disk Usage

**Clean up:**
```bash
docker-compose down -v  # Remove volumes
docker system prune     # Remove unused images/containers
```

---

## 🔐 Security Considerations

### For Development (Current Setup)
- Default credentials: admin/admin
- Default Fernet key (not secure for production)
- Volumes mounted with write access

### For Production
1. **Change Airflow Admin Password:**
   ```bash
   docker-compose exec webserver airflow users modify -u admin -p your_secure_password
   ```

2. **Generate New Fernet Key:**
   ```bash
   python -c "from cryptography.fernet import Fernet; key = Fernet.generate_key().decode(); print(key); print('Update AIRFLOW__CORE__FERNET_KEY in .env')"
   ```

3. **Change PostgreSQL Password:**
   - Update .env POSTGRES_PASSWORD
   - Recreate postgres container: `docker-compose down -v && docker-compose up -d postgres`

4. **Use Environment Variables for Secrets:**
   ```bash
   export POSTGRES_PASSWORD=$(openssl rand -base64 32)
   export AIRFLOW__WEBSERVER__SECRET_KEY=$(openssl rand -base64 32)
   ```

5. **Restrict Volume Access:**
   - Use `ro` (read-only) mounts where possible
   - Don't mount entire project directories in production

---

## 📈 Performance Tuning

### Increase Worker Count
Edit `docker-compose.yaml`:
```yaml
environment:
  AIRFLOW__WEBSERVER__WORKERS: "4"  # Increase from 2
```

### Increase PostgreSQL Resources
```yaml
postgres:
  # Add after image:
  shm_size: '1gb'  # For shared memory
  environment:
    POSTGRES_MAX_CONNECTIONS: "100"
```

### Enable Airflow Metrics
```yaml
environment:
  AIRFLOW__METRICS__STATSD_ON: "True"
  AIRFLOW__METRICS__STATSD_HOST: "127.0.0.1"
  AIRFLOW__METRICS__STATSD_PORT: "8125"
```

---

## 🔄 Backup & Restore

### Backup PostgreSQL Data
```bash
docker-compose exec postgres pg_dump -U airflow airflow > backup.sql
```

### Restore PostgreSQL Data
```bash
docker-compose exec -T postgres psql -U airflow airflow < backup.sql
```

### Backup Volumes
```bash
# Backup all data
docker run --rm -v etl_postgres-db:/data -v $(pwd):/backup alpine tar czf /backup/postgres-db-backup.tar.gz /data
```

---

## 📚 Docker Compose Commands Reference

```bash
# Start services in background
docker-compose up -d

# Start services in foreground (see logs)
docker-compose up

# Stop services (keeps data)
docker-compose stop

# Stop and remove containers
docker-compose down

# Remove containers and volumes
docker-compose down -v

# View logs
docker-compose logs
docker-compose logs -f          # Follow logs
docker-compose logs scheduler   # Service-specific logs

# Execute command in container
docker-compose exec webserver bash
docker-compose exec postgres psql -U airflow

# View running containers
docker-compose ps

# Restart services
docker-compose restart

# View service status
docker-compose config

# Validate docker-compose.yaml
docker-compose config --quiet
```

---

## 🎯 Next Steps

1. ✅ Run `docker-compose up -d` in Docker/ directory
2. ✅ Wait for all services to be healthy (check `docker-compose ps`)
3. ✅ Access Airflow at http://localhost:8080 (admin/admin)
4. ✅ Verify PostgreSQL connection
5. ✅ Trigger `unified_etl_dag` from web UI
6. ✅ Check execution logs

---

## 📞 Support

### Check Logs
```bash
docker-compose logs -f
```

### Verify Services
```bash
docker-compose ps
```

### Test Database
```bash
docker-compose exec webserver python /opt/airflow/scripts/postgres_config.py
```

### Run ETL Test
```bash
docker-compose exec webserver python /opt/airflow/scripts/Load.py
```

---

## 📄 Summary

Your complete ETL pipeline is now containerized and ready to run:
- PostgreSQL for data persistence
- Airflow for orchestration
- 3 DAGs for different data sources
- Complete Python ETL scripts included
- All configuration pre-set in docker-compose.yaml

**Quick start:** `docker-compose up -d` and access http://localhost:8080

---

**Docker Setup Complete!** 🐳✨
