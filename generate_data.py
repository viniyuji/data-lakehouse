#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "faker>=30.0.0",
#     "psycopg[binary]>=3.2.0",
# ]
# ///
"""Single-file fake data generator for PostgreSQL CDC streaming PoC."""

from __future__ import annotations

import argparse
import os
import random
import time
from datetime import datetime, timezone
from decimal import Decimal

import psycopg
from faker import Faker

SCHEMA_SQL = """
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
"""

PRODUCT_CATEGORIES = [
    "Wireless Mouse",
    "Mechanical Keyboard",
    "USB-C Hub",
    "27-inch Monitor",
    "Noise-Canceling Headphones",
    "NVMe SSD 1TB",
    "Laptop Stand",
    "Webcam 4K",
    "Ergonomic Chair",
    "Portable Charger",
    "Smartwatch",
    "Bluetooth Speaker",
    "External Hard Drive",
    "Wi-Fi 6 Router",
    "Graphics Tablet",
]


def get_connection() -> psycopg.Connection:
    return psycopg.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "lakehouse"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
        autocommit=True,
    )


def build_product_name(fake: Faker) -> str:
    item = random.choice(PRODUCT_CATEGORIES)
    brand = fake.company().split()[0].strip(",")
    model = fake.bothify(text="??-###").upper()
    return f"{brand} {item} {model}"


def insert_client(cur: psycopg.Cursor, fake: Faker) -> int:
    cur.execute(
        """
        INSERT INTO clients (client_name, client_number, client_address, document)
        VALUES (%s, %s, %s, %s)
        RETURNING id
        """,
        (
            fake.name(),
            fake.phone_number()[:50],
            fake.address().replace("\n", ", "),
            fake.ssn(),
        ),
    )
    row = cur.fetchone()
    assert row is not None
    return int(row[0])


def insert_product(cur: psycopg.Cursor, fake: Faker) -> int:
    cur.execute(
        """
        INSERT INTO products (product_name)
        VALUES (%s)
        RETURNING id
        """,
        (build_product_name(fake),),
    )
    row = cur.fetchone()
    assert row is not None
    return int(row[0])


def seed_dimensions(
    cur: psycopg.Cursor, fake: Faker, min_clients: int, min_products: int
) -> tuple[list[int], list[int]]:
    cur.execute("SELECT id FROM clients")
    client_ids = [int(r[0]) for r in cur.fetchall()]
    while len(client_ids) < min_clients:
        client_ids.append(insert_client(cur, fake))

    cur.execute("SELECT id FROM products")
    product_ids = [int(r[0]) for r in cur.fetchall()]
    while len(product_ids) < min_products:
        product_ids.append(insert_product(cur, fake))

    print(
        f"Ready with {len(client_ids)} clients and {len(product_ids)} products in PostgreSQL."
    )
    return client_ids, product_ids


def insert_sale(
    cur: psycopg.Cursor, client_ids: list[int], product_ids: list[int]
) -> tuple[int, int, int, int, Decimal, str]:
    client_id = random.choice(client_ids)
    product_id = random.choice(product_ids)
    unity = random.randint(1, 5)
    value = Decimal(str(round(random.uniform(19.90, 899.90), 2)))
    sale_date = datetime.now(timezone.utc)
    status = random.choices(["PENDING", "PAID"], weights=[0.7, 0.3], k=1)[0]

    cur.execute(
        """
        INSERT INTO sales (client_id, product_id, unity, value, sale_date, status)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (client_id, product_id, unity, value, sale_date, status),
    )
    row = cur.fetchone()
    assert row is not None
    return int(row[0]), client_id, product_id, unity, value, status


def update_random_pending_sale(cur: psycopg.Cursor) -> tuple[int, str] | None:
    cur.execute(
        """
        SELECT id FROM sales
        WHERE status = 'PENDING'
        ORDER BY RANDOM()
        LIMIT 1
        """
    )
    row = cur.fetchone()
    if row is None:
        return None

    sale_id = int(row[0])
    new_status = random.choices(
        ["PAID", "SHIPPED", "CANCELLED"], weights=[0.65, 0.20, 0.15], k=1
    )[0]
    cur.execute(
        "UPDATE sales SET status = %s WHERE id = %s",
        (new_status, sale_id),
    )
    return sale_id, new_status


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate fake clients, products, and streaming sales in PostgreSQL."
    )
    parser.add_argument(
        "--clients", type=int, default=25, help="Initial number of clients to seed"
    )
    parser.add_argument(
        "--products", type=int, default=15, help="Initial number of products to seed"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="Seconds between generated CDC events (default: 2.0)",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Stop after N sales events (0 = stream continuously until Ctrl+C)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    fake = Faker()

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
        client_ids, product_ids = seed_dimensions(
            cur, fake, min_clients=args.clients, min_products=args.products
        )

        events = 0
        print("Streaming sales events into PostgreSQL (press Ctrl+C to stop)...")
        try:
            while args.max_events == 0 or events < args.max_events:
                # Occasionally add a brand-new client so dimension CDC also streams live
                if random.random() < 0.05:
                    new_cid = insert_client(cur, fake)
                    client_ids.append(new_cid)
                    print(f"[INSERT clients] id={new_cid}")

                # Mix INSERTs (70%) and status UPDATEs (30%) to exercise full CDC
                if random.random() < 0.30:
                    updated = update_random_pending_sale(cur)
                    if updated is not None:
                        sale_id, new_status = updated
                        print(f"[UPDATE sales]   id={sale_id} -> status={new_status}")
                        events += 1
                        if args.max_events == 0 or events < args.max_events:
                            time.sleep(args.interval)
                        continue

                sale_id, cid, pid, unity, val, status = insert_sale(
                    cur, client_ids, product_ids
                )
                print(
                    f"[INSERT sales]   id={sale_id} client_id={cid} product_id={pid} "
                    f"unity={unity} value={val} status={status}"
                )
                events += 1

                if args.max_events == 0 or events < args.max_events:
                    time.sleep(args.interval)
        except KeyboardInterrupt:
            print(f"\nStopped after {events} streaming events.")


if __name__ == "__main__":
    main()
