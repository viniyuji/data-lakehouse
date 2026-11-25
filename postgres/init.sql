-- Tables for the streaming PoC
CREATE TABLE IF NOT EXISTS clients (
    id SERIAL PRIMARY KEY,
    client_name VARCHAR(255) NOT NULL,
    client_number VARCHAR(50) NOT NULL,
    client_address TEXT NOT NULL,
    document VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS sales (
    id SERIAL PRIMARY KEY,
    client_id INTEGER NOT NULL REFERENCES clients(id),
    product_id INTEGER NOT NULL REFERENCES products(id),
    unity INTEGER NOT NULL,
    value NUMERIC(12, 2) NOT NULL,
    sale_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status VARCHAR(50) NOT NULL
);

-- Operational heartbeat table used by Debezium's heartbeat.action.query
-- to advance confirmed_flush_lsn even when business tables are idle.
CREATE TABLE IF NOT EXISTS debezium_heartbeat (
    id INTEGER PRIMARY KEY,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Ensure UPDATE and DELETE WAL events include full 'before' row images
-- so Flink's debezium-json changelog processor can retract/update joined rows cleanly.
ALTER TABLE clients REPLICA IDENTITY FULL;
ALTER TABLE products REPLICA IDENTITY FULL;
ALTER TABLE sales REPLICA IDENTITY FULL;
ALTER TABLE debezium_heartbeat REPLICA IDENTITY FULL;

-- Pre-create the logical replication publication including debezium_heartbeat
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'dbz_lakehouse_publication') THEN
        CREATE PUBLICATION dbz_lakehouse_publication
            FOR TABLE clients, products, sales, debezium_heartbeat;
    END IF;
END $$;
