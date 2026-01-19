"""
Flask Web Dashboard for Airflow API Testing
Simple GUI to interact with API endpoints
"""
from flask import Flask, render_template, request, jsonify
import requests
from typing import Dict, Any
import os

app = Flask(__name__)

# Configuration
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
API_KEY = os.getenv("API_KEY", "dev-key-12345")
HEADERS = {"X-API-Key": API_KEY}


def make_api_request(endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
    """Make request to API and return response"""
    try:
        url = f"{API_BASE_URL}{endpoint}"
        response = requests.get(url, headers=HEADERS, params=params, timeout=10)
        return {
            "success": True,
            "status_code": response.status_code,
            "data": response.json()
        }
    except requests.exceptions.RequestException as e:
        return {
            "success": False,
            "error": str(e)
        }


@app.route("/")
def index():
    """Main dashboard page"""
    return render_template("dashboard.html")


@app.route("/api/health")
def health_check():
    """Check API health"""
    return jsonify(make_api_request("/health"))


@app.route("/api/dags")
def get_dags():
    """Get all DAGs"""
    return jsonify(make_api_request("/api/v1/dags"))


@app.route("/api/dags/<dag_id>/status")
def get_dag_status(dag_id: str):
    """Get DAG status"""
    return jsonify(make_api_request(f"/api/v1/dags/{dag_id}/status"))


@app.route("/api/dags/<dag_id>/runs")
def get_dag_runs(dag_id: str):
    """Get DAG runs with optional filters"""
    params = {
        "page": request.args.get("page", 1),
        "page_size": request.args.get("page_size", 50),
    }
    if request.args.get("state"):
        params["state"] = request.args.get("state")
    
    return jsonify(make_api_request(f"/api/v1/dags/{dag_id}/runs", params))


@app.route("/api/dags/<dag_id>/runs/<run_id>")
def get_dag_run(dag_id: str, run_id: str):
    """Get specific DAG run"""
    return jsonify(make_api_request(f"/api/v1/dags/{dag_id}/runs/{run_id}"))


@app.route("/api/dags/<dag_id>/runs/<run_id>/tasks")
def get_run_tasks(dag_id: str, run_id: str):
    """Get tasks for a run"""
    return jsonify(make_api_request(f"/api/v1/dags/{dag_id}/runs/{run_id}/tasks"))


@app.route("/api/metadata/summary")
def get_metadata_summary():
    """Get metadata summary"""
    return jsonify(make_api_request("/api/v1/metadata/summary"))


@app.route("/api/metadata/tables")
def get_table_stats():
    """Get table statistics"""
    return jsonify(make_api_request("/api/v1/metadata/tables"))


@app.route("/api/metadata/metrics")
def get_metrics():
    """Get pipeline metrics"""
    return jsonify(make_api_request("/api/v1/metadata/metrics"))


@app.route("/api/logs/<dag_id>")
def get_dag_logs(dag_id: str):
    """List available logs for DAG"""
    return jsonify(make_api_request(f"/api/v1/logs/dags/{dag_id}"))


@app.route("/api/logs/<dag_id>/<run_id>")
def get_run_logs(dag_id: str, run_id: str):
    """Get logs for DAG run"""
    params = {}
    if request.args.get("max_lines"):
        params["max_lines"] = request.args.get("max_lines")
    
    return jsonify(make_api_request(f"/api/v1/logs/dags/{dag_id}/runs/{run_id}", params))


@app.route("/api/logs/<dag_id>/<run_id>/<task_id>")
def get_task_logs(dag_id: str, run_id: str, task_id: str):
    """Get logs for task"""
    params = {
        "try_number": request.args.get("try_number", 1)
    }
    if request.args.get("max_lines"):
        params["max_lines"] = request.args.get("max_lines")
    
    return jsonify(make_api_request(f"/api/v1/logs/dags/{dag_id}/runs/{run_id}/tasks/{task_id}", params))


if __name__ == "__main__":
    print("=" * 60)
    print("🚀 Airflow API Web Dashboard")
    print("=" * 60)
    print(f"API Endpoint: {API_BASE_URL}")
    print(f"API Key: {API_KEY[:15]}...")
    print(f"\n📱 Dashboard URL: http://localhost:5000")
    print("=" * 60)
    app.run(debug=True, host="0.0.0.0", port=5000)
