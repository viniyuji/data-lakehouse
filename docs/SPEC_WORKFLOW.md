# Spec-Driven Development (SDD) Workflow for AI Harnesses

> **Audience**: Autonomous and interactive AI coding agents operating in this repository.

## 1. Core Principle: Spec Before Implementation

In this repository, `docs/` is the **source of truth** for system contracts and architectural invariants. Code changes MUST NOT silently diverge from `docs/`.

Whenever a task introduces or modifies:
- Tables, columns, data types, or CDC settings $\rightarrow$ update **`docs/DATA_CONTRACTS.md`** first.
- Containers, images, ports, volumes, or startup dependencies $\rightarrow$ update **`docs/ARCHITECTURE.md`** first.
- Architectural trade-offs, new components (e.g., Iceberg, Polaris, DuckDB, Trino), or workarounds for runtime constraints $\rightarrow$ append a new **`ADR-00X`** in **`docs/ADR.md`** and, if it introduces a hard rule, add a bullet to **`docs/INDEX.md`**.

---

## 2. Execution Loop for AI Agents

### Phase 1 — Context & Impact Check
1. Read `docs/INDEX.md` to review active invariants.
2. Read the targeted spec (`docs/ARCHITECTURE.md`, `docs/DATA_CONTRACTS.md`, `docs/ADR.md`).
3. Check running Docker state (`docker compose ps`) before mutating ports or volumes. Ask the user before running destructive volume teardowns (`docker compose down -v`) if user data may exist.

### Phase 2 — Spec Update
1. Edit the relevant markdown specs in `docs/` to reflect the target state.
2. Verify cross-layer alignment:
   - PostgreSQL DDL (`postgres/init.sql`)
   - Data generator (`generate_data.py`)
   - Debezium `table.include.list` / converters (`debezium/connector-config.json`)
   - Kafka pre-created topics (`docker-compose.yml` $\rightarrow$ `kafka-init`)
   - Flink SQL source/sink DDL & query (`flink/sql/job.sql`)

### Phase 3 — Implementation
1. Keep changes minimal and idiomatic to the existing stack.
2. Preserve idempotency across all `*-init` and job-submission scripts.
3. Validate Compose syntax before starting containers:
   ```bash
   docker compose config --quiet
   ```

### Phase 4 — Mandatory Verification Gate

Do NOT consider a pipeline change complete until every applicable check below passes against the live stack:

1. **Services Healthy & Init Containers Exited Cleanly (`0`)**:
   ```bash
   docker compose up -d --build
   docker compose ps -a
   ```
2. **Debezium Connector & Task `RUNNING`**:
   ```bash
   curl -sf http://localhost:8083/connectors/postgres-lakehouse-connector/status
   ```
3. **Flink Job `RUNNING` with Zero Failed Tasks (and Idempotent Re-run)**:
   ```bash
   curl -sf http://localhost:8081/jobs/overview
   ```
4. **End-to-End CDC Data Flow (`INSERT` + `UPDATE`)**:
   ```bash
   uv run generate_data.py --clients 10 --products 8 --interval 0.2 --max-events 10
   docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
     --bootstrap-server localhost:29092 \
     --topic sales_enriched \
     --from-beginning \
     --formatter-property print.key=true \
     --timeout-ms 5000
   ```
5. **Checkpoints Persisting in MinIO (`completed >= 1`, `failed == 0`)**:
   ```bash
   docker exec minio mc ls -r local/flink-checkpoints
   ```

---

## 3. Authoring Guidelines for Future Specs

When updating `docs/`:
- **Be token-efficient and exact**: Prefer concrete schemas, ports, env vars, and `MUST` / `MUST NOT` rules over generic prose.
- **Document failure modes**: Whenever a configuration exists to prevent a specific error (e.g., `ConnectException`, Base64 decimals, Calcite reserved words), state the exact error so future AI sessions never "clean up" load-bearing lines.
- **Keep diagrams truthful**: Update Mermaid diagrams in `docs/ARCHITECTURE.md` whenever data flow or service names change.
