#!/usr/bin/env bash
# Register the Debezium Postgres source connector with Kafka Connect
set -euo pipefail
until curl -sf http://localhost:8083/connectors > /dev/null; do echo "waiting for Kafka Connect..."; sleep 3; done
curl -s -X POST -H "Content-Type: application/json" \
  --data @connectors/postgres-source.json http://localhost:8083/connectors | python3 -m json.tool
