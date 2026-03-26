#!/bin/bash
# start_airflow.sh — Start all TTCS services
#
# Usage:
#   cd /home/peterjohn/TTCS-05-2026/infra
#   chmod +x start_airflow.sh
#   ./start_airflow.sh

set -e

echo "=== Starting all TTCS services ==="
docker compose up -d

echo ""
echo "=== Waiting for services to be healthy ==="
sleep 10

echo ""
echo "=========================================="
echo "  TTCS Data Platform + Airflow is UP!"
echo "=========================================="
echo ""
echo "  Services:"
echo "    MinIO Console  : http://localhost:9011  (minioadmin / minioadmin123)"
echo "    ClickHouse     : http://localhost:8124"
echo "    Source API      : http://localhost:8001"
echo "    Spark UI       : http://localhost:4041"
echo "    Streamlit      : http://localhost:8502"
echo "    Airflow Web UI : http://localhost:8082  (admin / admin)"
echo ""
echo "  Next steps:"
echo "    1. Open Airflow at http://localhost:8082"
echo "    2. Trigger 'customer360_initial_load' DAG manually for first-time setup"
echo "    3. Enable 'customer360_daily_etl' DAG for automated runs (every 5 min)"
echo ""
