# AI Harness Documentation Index (`docs/`)

> **Purpose**: Authoritative specification and decision log for AI coding agents and human contributors working on `data-lakehouse` under **Spec-Driven Development (SDD)**.

## Quick Routing Map for AI Agents

Read only the documents relevant to your current task before modifying code:

- **`docs/ARCHITECTURE.md`** — Read when changing `docker-compose.yml`, container images, ports, networking, startup order, or adding new infrastructure services (e.g., Iceberg catalog, query engine, new sinks).
- **`docs/DATA_CONTRACTS.md`** — Read when modifying PostgreSQL tables (`postgres/init.sql`), `generate_data.py`, Debezium configuration (`debezium/`), Kafka topics, or Flink SQL schemas (`flink/sql/job.sql`).
- **`docs/ADR.md`** — Read **before refactoring** any existing configuration. Documents non-obvious technical constraints and failure modes discovered in this stack (WAL replica identity, Decimal serialization, distroless Chainguard MinIO, Flink 1.20 entrypoint behavior, idempotency).
- **`docs/SPEC_WORKFLOW.md`** — Read when planning or implementing a new feature. Defines the spec-first workflow and mandatory end-to-end verification commands.

---

## System Snapshot

- **Goal**: Local streaming Change Data Capture (CDC) PoC evolving toward a local Data Lakehouse.
- **Data Flow**: `generate_data.py` $\rightarrow$ **PostgreSQL 16** (`wal_level=logical`) $\rightarrow$ **Debezium 3.7** (`pgoutput`) $\rightarrow$ **Kafka 4.4** (KRaft) $\rightarrow$ **Flink 1.20 SQL** (state checkpointed to **MinIO**) $\rightarrow$ **Kafka** (`sales_enriched`).
- **Compose Project Name**: `lakehouse` (`docker-compose.yml`).

---

## Hard Invariants (MUST / MUST NOT)

1. **PostgreSQL CDC Tables MUST set `REPLICA IDENTITY FULL`**:
   - Flink's `debezium-json` changelog consumer requires full `before` row images to emit retract (`-U`) records during streaming joins. Never leave CDC-captured tables on `REPLICA IDENTITY DEFAULT`.
2. **Debezium MUST keep `decimal.handling.mode = "double"` and `schemas.enable = "false"`** (unless migrating the entire pipeline to Schema Registry + Avro/Protobuf):
   - Default `precise` mode encodes PostgreSQL `NUMERIC` as Base64 bytes, which breaks Flink SQL `DECIMAL` JSON parsing.
3. **Flink SQL MUST escape SQL/Calcite reserved words**:
   - Column `` `value` `` in `sales` is a reserved keyword in Apache Calcite and MUST always be backtick-escaped in Flink SQL (`flink/sql/job.sql`).
4. **Flink Containers MUST NOT override `entrypoint` (`["/docker-entrypoint.sh"]`)**:
   - Official Flink images translate `FLINK_PROPERTIES` into `/opt/flink/conf/config.yaml` inside `/docker-entrypoint.sh`. Custom scripts (such as `flink-sql-submit`) MUST use `command:`, never `entrypoint:`.
5. **Chainguard `minio-client` (`minio-init`) has NO `/bin/sh`**:
   - `cgr.dev/chainguard/minio-client:latest` is distroless (`entrypoint: ["/usr/bin/mc"]`, `uid=65532`). Configure credentials via `MC_HOST_<alias>` and `MC_CONFIG_DIR=/tmp/.mc`, passing subcommands directly in `command:`.
6. **Bootstrap Services (`*-init`, `flink-sql-submit`) MUST remain idempotent**:
   - Running `docker compose up -d` repeatedly against an already-running stack must succeed with exit code `0` without duplicating Flink jobs or failing on existing buckets/topics/connectors.
7. **Fake Data Generator MUST remain a single Python file (`generate_data.py`)**:
   - Keep inline PEP 723 script metadata (`# /// script`) synchronized with `requirements.txt` so both `uv run generate_data.py` and `pip` virtual environments work out of the box.
