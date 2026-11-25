#!/bin/bash
set -euo pipefail

JM_URL="${JM_URL:-http://flink-jobmanager:8081}"
JOB_NAME="sales-cdc-enrichment"

echo "Waiting for Flink JobManager and at least 1 available TaskManager slot at ${JM_URL}..."
until curl -sf "${JM_URL}/overview" | grep -Eq '"slots-available":[1-9]'; do
  sleep 2
done

if curl -sf "${JM_URL}/jobs/overview" | sed 's/"jid"/\n"jid"/g' | grep -F "\"name\":\"${JOB_NAME}\"" | grep -Fq '"state":"RUNNING"'; then
  echo "Flink job '${JOB_NAME}' is already RUNNING. Skipping submission."
  exit 0
fi

echo "Submitting Flink SQL job '${JOB_NAME}'..."
/opt/flink/bin/sql-client.sh -f /opt/flink/sql/job.sql
echo "Flink SQL job '${JOB_NAME}' submitted successfully."
