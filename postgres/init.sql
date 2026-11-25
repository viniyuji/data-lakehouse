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

-- Ensure UPDATE and DELETE WAL events include full 'before' row images
-- so Flink's debezium-json changelog processor can retract/update joined rows cleanly.
ALTER TABLE clients REPLICA IDENTITY FULL;
ALTER TABLE products REPLICA IDENTITY FULL;
ALTER TABLE sales REPLICA IDENTITY FULL;
