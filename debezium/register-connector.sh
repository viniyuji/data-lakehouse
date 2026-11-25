#!/bin/sh
set -eu

CONNECT_URL="${CONNECT_URL:-http://debezium:8083}"
CONNECTOR_NAME="postgres-lakehouse-connector"

echo "Waiting for Debezium Connect REST API at ${CONNECT_URL}..."
until curl -sf "${CONNECT_URL}/connectors" >/dev/null; do
  sleep 2
done

echo "Registering connector '${CONNECTOR_NAME}'..."
curl -sf -X PUT \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  --data @/debezium/connector-config.json \
  "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config"

echo ""
echo "Connector '${CONNECTOR_NAME}' registered successfully."
