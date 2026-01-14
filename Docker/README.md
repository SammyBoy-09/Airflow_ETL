# 🐳 Docker Deployment Guide - Multi-Source ETL Pipeline

## ⚡ Super Quick Start (60 Seconds)

```bash
# 1. Navigate to Docker directory
cd D:\sam\Projects\Infosys\Airflow\Docker

# 2. Start all services
docker-compose up -d

# 3. Wait 30 seconds, then open browser
# Access Airflow: http://localhost:8080
# Login: admin / admin
```

**That's it!** Your ETL pipeline is running. 🎉

---

## 📋 What's Running

| Service | Status | Access |
|---------|--------|--------|
| PostgreSQL | 🟢 Running | localhost:5432 |
| Redis | 🟢 Running | localhost:6379 |
| Airflow Webserver | 🟢 Running | http://localhost:8080 |
| Airflow Scheduler | 🟢 Running | Background process |

---

## 🚀 Step-by-Step Instructions

### Step 1: Open Terminal
```bash
# Windows PowerShell or Command Prompt
cd D:\sam\Projects\Infosys\Airflow\Docker
```

### Step 2: Verify Docker is Running
```bash
docker --version
docker-compose --version
```

Should show versions like:
```
Docker version 24.x.x, build xxxxxxx
Docker Compose version 2.x.x, build xxxxxxx
```

### Step 3: Start Services
```bash
docker-compose up -d
```

Output:
```
Creating docker-postgres-1     ... done
Creating docker-redis-1        ... done
Creating docker-webserver-1    ... done
Creating docker-scheduler-1    ... done
```

### Step 4: Verify Services Started
```bash
docker-compose ps
```

Should show:
```
NAME                STATUS
docker-postgres-1   Up (healthy)
docker-redis-1      Up (healthy)
docker-webserver-1  Up (running)
docker-scheduler-1  Up (running)
```

### Step 5: Access Airflow
1. Open browser: http://localhost:8080
2. Login: **admin** / **admin**
3. You'll see DAGs:
   - customer_etl_dag
   - sales_etl_dag
   - unified_etl_dag

---

## 🎯 Trigger Your First ETL Run

### Option A: Via Web UI (Easiest)
1. Go to http://localhost:8080
2. Find **unified_etl_dag**
3. Click the "▶️ Trigger DAG" button
4. Click "Trigger"
5. Watch in real-time as it runs

### Option B: Via Command Line
```bash
docker-compose exec scheduler airflow dags trigger unified_etl_dag
```

### Monitor Execution
```bash
# Watch logs in real-time
docker-compose logs -f scheduler

# Or in webserver
docker-compose logs -f webserver
```

---

## 🔍 Check Your Data

### Via PostgreSQL
```bash
# Connect to database
docker-compose exec postgres psql -U airflow -d airflow

# Inside psql, run:
SELECT * FROM etl.output_customers_cleaned;
SELECT * FROM etl.rejected_records;
SELECT * FROM etl.pipeline_runs;

# Exit
\q
```

### Via Python
```bash
docker-compose exec webserver python -c "
import pandas as pd
import psycopg2

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

## 🛠️ Common Tasks

### View Logs
```bash
# All services
docker-compose logs

# Follow in real-time
docker-compose logs -f

# Specific service
docker-compose logs -f scheduler
docker-compose logs -f webserver
docker-compose logs -f postgres
```

### Run Python Scripts Directly
```bash
# Extract data
docker-compose exec webserver python /opt/airflow/scripts/Extract.py

# Transform data
docker-compose exec webserver python /opt/airflow/scripts/TransformAmazon.py

# Load data
docker-compose exec webserver python /opt/airflow/scripts/Load.py

# Setup database
docker-compose exec webserver python /opt/airflow/scripts/postgres_config.py
```

### Access Container Shell
```bash
# Airflow webserver bash
docker-compose exec webserver bash

# PostgreSQL psql
docker-compose exec postgres psql -U airflow -d airflow

# Redis CLI
docker-compose exec redis redis-cli
```

### Restart Services
```bash
# Restart all
docker-compose restart

# Restart specific
docker-compose restart scheduler
docker-compose restart webserver
```

### Stop Services (Keep Data)
```bash
docker-compose stop
```

### Stop and Clean (Delete Everything)
```bash
docker-compose down -v
```

---

## 🐛 Troubleshooting

### Services won't start
```bash
# Check logs
docker-compose logs

# Make sure ports are free
# 5432 (PostgreSQL) and 8080 (Airflow UI)
```

### Port already in use
Edit `docker-compose.yaml` and change ports:
```yaml
postgres:
  ports:
    - "5433:5432"  # Use 5433 instead of 5432

webserver:
  ports:
    - "8081:8080"  # Use 8081 instead of 8080
```

### Webserver not responding
```bash
# Check webserver logs
docker-compose logs webserver

# Restart webserver
docker-compose restart webserver
```

### Database connection failed
```bash
# Test PostgreSQL connection
docker-compose exec webserver python -c "
import psycopg2
try:
    conn = psycopg2.connect(
        host='postgres',
        user='airflow',
        password='airflow',
        database='airflow'
    )
    print('✅ Connected to PostgreSQL')
except Exception as e:
    print(f'❌ Failed: {e}')
"
```

### Reset Everything
```bash
# Stop and remove all
docker-compose down -v

# Start fresh
docker-compose up -d

# Wait 30 seconds for initialization
# Then access http://localhost:8080
```

---

## 🔐 Credentials & Configuration

### Default Credentials
```
Airflow Web UI:
  Username: admin
  Password: admin

PostgreSQL:
  Host: localhost (or 'postgres' from inside container)
  Port: 5432
  Username: airflow
  Password: airflow
  Database: airflow
```

### Change Airflow Admin Password
```bash
docker-compose exec webserver airflow users modify -u admin -p your_new_password
```

### Change PostgreSQL Password
Edit `Docker/.env`:
```env
POSTGRES_PASSWORD=your_new_password
```

Then recreate:
```bash
docker-compose down -v
docker-compose up -d
```

---

## 📊 Monitoring

### Check DAG Status
```bash
# List all DAGs
docker-compose exec scheduler airflow dags list

# List DAG runs
docker-compose exec scheduler airflow dags list-runs --dag-id unified_etl_dag

# Describe DAG
docker-compose exec scheduler airflow dags show unified_etl_dag
```

### Check Logs in Airflow UI
1. Go to http://localhost:8080
2. Click on a DAG
3. Click on a run
4. Click on a task
5. Click "Log" tab to see execution log

---

## 📈 Performance Tips

### Increase Resources
```yaml
# In docker-compose.yaml
postgres:
  shm_size: '1gb'  # Add this line
  
webserver:
  environment:
    AIRFLOW__WEBSERVER__WORKERS: "4"  # Increase from 2
```

Then restart:
```bash
docker-compose up -d
```

---

## 🔄 Backup Your Data

### Backup PostgreSQL
```bash
docker-compose exec postgres pg_dump -U airflow airflow > backup.sql
```

### Restore PostgreSQL
```bash
docker-compose exec -T postgres psql -U airflow airflow < backup.sql
```

---

## 📚 Quick Reference

```bash
# Start services
docker-compose up -d

# Stop services
docker-compose stop

# Restart services
docker-compose restart

# View status
docker-compose ps

# View logs
docker-compose logs -f

# Execute command
docker-compose exec webserver <command>

# Full reset
docker-compose down -v && docker-compose up -d
```

---

## 🎯 Next Steps

1. ✅ Run: `docker-compose up -d`
2. ✅ Wait 30 seconds
3. ✅ Open: http://localhost:8080
4. ✅ Login: admin / admin
5. ✅ Trigger: unified_etl_dag
6. ✅ Monitor: Watch execution logs
7. ✅ Verify: Check data in PostgreSQL

---

## 📞 Support

### Verify Everything Works
```bash
# Run test ETL
docker-compose exec webserver python /opt/airflow/scripts/Load.py

# Expected output:
# ✓ Load complete: 25 records loaded, 0 rejected
```

### Check Database
```bash
docker-compose exec postgres psql -U airflow -d airflow -c "
SELECT COUNT(*) as customer_count FROM etl.output_customers_cleaned;
SELECT COUNT(*) as sales_count FROM etl.output_sales_cleaned;
"
```

---

## 🎉 You're All Set!

Your Multi-Source ETL Pipeline is running in Docker with:
- ✅ PostgreSQL for data storage
- ✅ Airflow for orchestration
- ✅ 3 ETL DAGs configured
- ✅ Complete Python ETL scripts
- ✅ All monitoring and logging

**Start with:** `docker-compose up -d` in `Docker/` directory

Then access: http://localhost:8080 (admin/admin)

---

**Questions?** Refer to DOCKER_SETUP_GUIDE.md for detailed information.

**Happy data processing!** 🚀
