# data-lakehouse

Local streaming CDC proof of concept built with **PostgreSQL**, **Debezium**, **Apache Kafka (KRaft)**, **Apache Flink (SQL)**, and **MinIO**.

## Architecture

1. **PostgreSQL (`:5432`)** — Stores `clients`, `products`, and `sales` with `wal_level=logical` and `REPLICA IDENTITY FULL`.
2. **Debezium Connect (`:8083`)** — Captures row-level `INSERT` / `UPDATE` / `DELETE` changes via `pgoutput` and publishes JSON CDC events to Kafka topics:
   - `lakehouse.public.clients`
   - `lakehouse.public.products`
   - `lakehouse.public.sales`
3. **Apache Kafka (`:9092`)** — Single-node KRaft broker (no ZooKeeper).
4. **Apache Flink (`:8081`)** — Runs the `sales-cdc-enrichment` Flink SQL streaming job joining `sales` + `clients` + `products`, computing `total_value = unity * value`, and writing upserts to the `sales_enriched` Kafka topic.
5. **MinIO (`:9000` API / `:9001` Console)** — Stores Flink state checkpoints in `s3://flink-checkpoints/checkpoints` (`admin` / `password123`).

## Quickstart

### 1. Start the infrastructure

```bash
docker compose up -d --build
```

All topics, the `flink-checkpoints` MinIO bucket, the Debezium PostgreSQL connector, and the Flink SQL streaming job are initialized automatically.

### 2. Generate streaming fake data

With [`uv`](https://docs.astral.sh/uv/) (reads inline dependencies automatically):

```bash
uv run generate_data.py
```

Or with a standard virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python generate_data.py
```

Optional flags:

```bash
uv run generate_data.py --clients 25 --products 15 --interval 1.0 --max-events 50
```

### 3. Inspect the pipeline

- **Flink Web UI**: http://localhost:8081
- **MinIO Console**: http://localhost:9001 (`admin` / `password123`)
- **Debezium Connector Status**:
  ```bash
  curl http://localhost:8083/connectors/postgres-lakehouse-connector/status
  ```
- **Consume enriched Kafka events (`sales_enriched`)**:
  ```bash
  docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:29092 \
    --topic sales_enriched \
    --from-beginning \
    --formatter-property print.key=true
  ```

### 4. Tear down

```bash
docker compose down -v
```

## Documentation (Spec-Driven / AI Harness)

Architectural decisions, data contracts, invariants, and the Spec-Driven Development workflow for AI coding harnesses live in [`docs/INDEX.md`](docs/INDEX.md):

- [`docs/INDEX.md`](docs/INDEX.md) — Entrypoint routing map and hard invariants (`MUST` / `MUST NOT`).
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — Service topology, internal vs. host network matrix, startup DAG, and Lakehouse evolution path.
- [`docs/DATA_CONTRACTS.md`](docs/DATA_CONTRACTS.md) — PostgreSQL DDL, Debezium CDC JSON rules, and Flink SQL source/sink contracts.
- [`docs/ADR.md`](docs/ADR.md) — Architectural Decision Records and operational pitfalls to avoid.
- [`docs/SPEC_WORKFLOW.md`](docs/SPEC_WORKFLOW.md) — Spec-first workflow and mandatory verification gate for AI agents.

