# Architectural Decision Records (ADR)

> **Note for AI Harnesses**: Each ADR records **why** a design choice was made and the exact failure mode that occurs if an agent reverts or simplifies it without understanding the constraint.

---

## ADR-001: PostgreSQL `pgoutput` Logical Replication with `REPLICA IDENTITY FULL`

- **Status**: Accepted
- **Context**: Debezium needs logical WAL decoding on PostgreSQL 16, and Flink SQL (`debezium-json`) needs changelog semantics (`+I`, `-U`, `+U`, `-D`) across multi-table joins (`sales` $\bowtie$ `clients` $\bowtie$ `products`).
- **Decision**:
  1. Use standard `postgres:16-alpine` with `-c wal_level=logical` and Debezium's native `pgoutput` plugin (`publication.autocreate.mode = "filtered"`).
  2. Set `ALTER TABLE ... REPLICA IDENTITY FULL;` on `clients`, `products`, and `sales`.
- **Consequences & Guardrails**:
  - Zero custom PostgreSQL C plugins (`decoderbufs` / `wal2json`) are needed.
  - **Pitfall if removed**: Under PostgreSQL's default `REPLICA IDENTITY DEFAULT`, `UPDATE` WAL events omit non-key columns in the `"before"` object (`"before": {"id": 5, "client_id": null, ...}`). Flink's `debezium-json` parser uses `"before"` to construct `-U` (`UPDATE_BEFORE`) retractions, causing downstream join state corruption or null-key failures. Every new CDC table MUST include `REPLICA IDENTITY FULL`.

---

## ADR-002: Single-Node Apache Kafka 4.4 in KRaft Mode + Pre-Created Topics

- **Status**: Accepted
- **Context**: A local PoC should minimize container count and memory footprint while supporting both Docker-internal services and host-side inspection.
- **Decision**:
  1. Run `apache/kafka:4.4.0` in combined `broker,controller` KRaft mode (no ZooKeeper), exposing two listeners: `PLAINTEXT://kafka:29092` (internal) and `PLAINTEXT_HOST://localhost:9092` (host).
  2. Configure all single-node internal topic replication factors to `1`, explicitly including Kafka 4.x share-coordinator settings (`KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR=1`, `KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR=1`).
  3. Run a one-shot `kafka-init` container to create `lakehouse.public.clients`, `lakehouse.public.products`, `lakehouse.public.sales`, and `sales_enriched` before Flink submits its SQL job.
- **Consequences & Guardrails**:
  - **Pitfall if `kafka-init` is removed**: Debezium lazily creates table topics only when the first row exists in PostgreSQL. If the database starts empty, `flink-sql-submit` would attempt to subscribe to non-existent source/sink topics on startup.

---

## ADR-003: Schema-Less Debezium JSON with `decimal.handling.mode = "double"`

- **Status**: Accepted
- **Context**: Kafka Connect's `JsonConverter` defaults to `schemas.enable=true` (bloated payloads) and Debezium defaults `decimal.handling.mode` to `precise` (Avro/Kafka Connect `Decimal` logical type encoded as Base64 byte strings).
- **Decision**:
  - Set `key.converter.schemas.enable = "false"` and `value.converter.schemas.enable = "false"`.
  - Set `decimal.handling.mode = "double"` in `debezium/connector-config.json`.
- **Consequences & Guardrails**:
  - Kafka messages remain compact, human-readable JSON and map directly to Flink's `'format' = 'debezium-json'`.
  - **Pitfall if `decimal.handling.mode` is reverted**: `sales.value` (`NUMERIC(12, 2)`) will serialize as Base64 strings (e.g., `"value": "Elc="`), causing Flink SQL `DECIMAL(12, 2)` deserialization exceptions at runtime. *(If strict arbitrary-precision decimal fidelity is ever required for financial production workloads, switch either to `"decimal.handling.mode": "string"` or Avro/Protobuf with Schema Registry).*

---

## ADR-004: Pure Flink SQL (`flink:1.20.5`) over PyFlink/Java + Entrypoint Preservation

- **Status**: Accepted
- **Context**: Flink must consume three Debezium CDC streams, join them statefully, and emit enriched records to Kafka while keeping build times fast and container images small.
- **Decision**:
  1. Author the streaming job in pure **Flink SQL** (`flink/sql/job.sql`), adding only `flink-sql-connector-kafka-3.3.0-1.20.jar` to `/opt/flink/lib/`.
  2. Write output to `sales_enriched` using `upsert-kafka` keyed by `sale_id`.
  3. Submit the job via a one-shot `flink-sql-submit` container using `command: ["bash", "/opt/flink/submit-job.sh"]` (preserving the base image's `/docker-entrypoint.sh`).
- **Consequences & Guardrails**:
  - **Pitfall 1 — Overriding `entrypoint:`**: In official Flink 1.20 images, `/docker-entrypoint.sh` parses `FLINK_PROPERTIES` into `/opt/flink/conf/config.yaml` before executing `"$@"`. Setting `entrypoint:` on `flink-sql-submit` bypasses `prepare_configuration`, leaving `rest.address: localhost` and failing with `java.net.ConnectException: Connection refused`. Always use `command:`.
  - **Pitfall 2 — Reserved keyword `VALUE`**: `VALUE` is a reserved SQL keyword in Apache Calcite. Always reference `sales.value` as `` `value` `` in Flink SQL.

---

## ADR-005: S3 Checkpointing on MinIO via Chainguard Images and `s3-fs-presto`

- **Status**: Accepted
- **Context**: Public `minio/mc` pulls on Docker Hub fail with `pull access denied`, and Flink requires an S3 filesystem plugin to store `EXACTLY_ONCE` checkpoints in S3-compatible object storage.
- **Decision**:
  1. Use `cgr.dev/chainguard/minio:latest` for the MinIO server and `cgr.dev/chainguard/minio-client:latest` for bucket initialization (`s3://flink-checkpoints`).
  2. Enable Flink's bundled `flink-s3-fs-presto` plugin (`/opt/flink/opt/flink-s3-fs-presto-*.jar` $\rightarrow$ `/opt/flink/plugins/s3-fs-presto/`) and configure path-style access (`s3.endpoint: http://minio:9000`, `s3.path.style.access: true`).
- **Consequences & Guardrails**:
  - **Pitfall — Distroless `minio-client`**: `cgr.dev/chainguard/minio-client:latest` runs as non-root (`uid=65532`) with `entrypoint: ["/usr/bin/mc"]` and **contains no `/bin/sh`**. Never wrap `minio-init` commands in `sh -c`; instead pass `MC_HOST_local=http://<user>:<pass>@minio:9000` and `MC_CONFIG_DIR=/tmp/.mc` via environment variables and pass `mc` arguments directly in `command:`.
  - `s3-fs-presto` is preferred for Flink checkpoints; if Flink FileSystem/Iceberg sinks later need `s3a://`, enable `flink-s3-fs-hadoop` in its own isolated plugin directory (`/opt/flink/plugins/s3-fs-hadoop/`) rather than `/opt/flink/lib/`.

---

## ADR-006: Idempotent One-Shot Initialization Pattern

- **Status**: Accepted
- **Context**: Developers and AI agents frequently run `docker compose up -d` after small edits. Naive initialization scripts fail on existing resources or submit duplicate streaming jobs.
- **Decision**:
  - `kafka-init`: uses `--create --if-not-exists`.
  - `minio-init`: uses `mc mb --ignore-existing`.
  - `debezium-init`: uses HTTP `PUT /connectors/postgres-lakehouse-connector/config` (creates or updates in place, avoiding `POST /connectors` `409 Conflict`).
  - `flink-sql-submit`: inspects `GET /jobs/overview`, splits JSON per `"jid"` (`sed 's/"jid"/\n"jid"/g'`), and exits `0` if an entry with `"name":"sales-cdc-enrichment"` and `"state":"RUNNING"` already exists.
  - **Pitfall**: Do not use naive single-line regexes like `\{[^{}]*\}` on `/jobs/overview` because each job object nests `"tasks":{...}`.

---

## ADR-007: Single-File Python Data Generator with PEP 723 Metadata

- **Status**: Accepted
- **Context**: The fake data generator must stay inside a single Python file (`generate_data.py`), generate realistic `clients`, `products`, and `sales`, and be trivial to execute locally.
- **Decision**:
  - Implement `generate_data.py` using `faker` and `psycopg[binary]`, embedding PEP 723 `# /// script` metadata at the top alongside `requirements.txt`.
  - Generate a mix of `INSERT` (~70%) and status `UPDATE` (~30%, transitioning `PENDING` sales to `PAID`, `SHIPPED`, or `CANCELLED`) plus occasional new `clients` (~5%) so both insert and update CDC paths are exercised continuously.
