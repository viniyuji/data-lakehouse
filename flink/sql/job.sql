-- Pipeline & Checkpointing Configuration (RocksDB incremental checkpoints in MinIO via s3-fs-presto)
SET 'pipeline.name' = 'sales-cdc-enrichment';
SET 'state.backend.type' = 'rocksdb';
SET 'state.backend.incremental' = 'true';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'state.checkpoints.num-retained' = '3';

-- Source 1: Clients CDC changelog from Debezium
CREATE TABLE clients_cdc (
    id INT,
    client_name STRING,
    client_number STRING,
    client_address STRING,
    document STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'lakehouse.public.clients',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-clients-cdc',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

-- Source 2: Products CDC changelog from Debezium
CREATE TABLE products_cdc (
    id INT,
    product_name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'lakehouse.public.products',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-products-cdc',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

-- Source 3: Sales CDC changelog from Debezium (`value` is escaped as a SQL reserved word)
CREATE TABLE sales_cdc (
    id INT,
    client_id INT,
    product_id INT,
    unity INT,
    `value` DECIMAL(12, 2),
    sale_date STRING,
    status STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'lakehouse.public.sales',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-sales-cdc',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

-- Sink: Enriched Sales topic (Upsert Kafka keyed by sale_id)
CREATE TABLE sales_enriched (
    sale_id INT,
    sale_date STRING,
    status STRING,
    unity INT,
    unit_value DECIMAL(12, 2),
    total_value DECIMAL(14, 2),
    client_id INT,
    client_name STRING,
    client_number STRING,
    client_address STRING,
    client_document STRING,
    product_id INT,
    product_name STRING,
    PRIMARY KEY (sale_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'sales_enriched',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- Streaming CDC Join & Enrichment
INSERT INTO sales_enriched
SELECT
    s.id AS sale_id,
    s.sale_date,
    UPPER(s.status) AS status,
    s.unity,
    s.`value` AS unit_value,
    CAST(s.unity * s.`value` AS DECIMAL(14, 2)) AS total_value,
    c.id AS client_id,
    c.client_name,
    c.client_number,
    c.client_address,
    c.document AS client_document,
    p.id AS product_id,
    p.product_name
FROM sales_cdc AS s
INNER JOIN clients_cdc AS c ON s.client_id = c.id
INNER JOIN products_cdc AS p ON s.product_id = p.id;
