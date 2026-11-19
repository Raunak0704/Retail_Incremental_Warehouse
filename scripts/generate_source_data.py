from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import random

import numpy as np
import snowflake.connector

from retail_etl.config import seed_snowflake_config
from retail_etl.source_writer import write_source_rows

CITIES = ["Hyderabad", "Bengaluru", "Pune", "Mumbai", "Chennai", "Delhi", "Jaipur", "Kochi"]
STATES = ["Telangana", "Karnataka", "Maharashtra", "Tamil Nadu", "Delhi", "Rajasthan", "Kerala"]
SEGMENTS = ["STANDARD", "SILVER", "GOLD", "PLATINUM"]
CATEGORIES = ["Electronics", "Home", "Fashion", "Books", "Sports", "Beauty"]
BRANDS = ["Aster", "Northstar", "Vertex", "UrbanLeaf", "Nova"]
ORDER_STATUSES = ["PLACED", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]
PAYMENT_STATUSES = ["PAID", "PENDING", "REFUNDED", "FAILED"]
SOURCE_DB = "RETAIL_SOURCE"
SOURCE_SCHEMA = "RETAIL"


def main():
    parser = argparse.ArgumentParser(description="Generate deterministic Snowflake retail source data")
    parser.add_argument("--customers", type=int, default=10000)
    parser.add_argument("--products", type=int, default=1000)
    parser.add_argument("--orders", type=int, default=50000)
    parser.add_argument("--items-per-order", type=int, default=2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--append", action="store_true", help="Do not truncate existing source tables")
    args = parser.parse_args()
    if min(args.customers, args.products, args.orders, args.items_per_order) < 0:
        parser.error("Counts cannot be negative")
    if args.customers == 0 or args.products == 0:
        parser.error("At least one customer and one product are required")

    rng = random.Random(args.seed)
    np_rng = np.random.default_rng(args.seed)
    now = datetime.now(timezone.utc)

    customer_rows = []
    customer_created_at = {}
    for cid in range(1, args.customers + 1):
        created = now - timedelta(days=int(np_rng.integers(1, 1400)))
        customer_created_at[cid] = created
        city_idx = rng.randrange(len(CITIES))
        customer_rows.append((
            cid, f"Customer {cid:07d}", f"customer{cid}@example.test",
            f"+91-9{cid % 10}{cid % 100000000:08d}", CITIES[city_idx],
            STATES[city_idx % len(STATES)], "India", rng.choice(SEGMENTS),
            False, created, created,
        ))

    product_rows = []
    for pid in range(1, args.products + 1):
        created = now - timedelta(days=int(np_rng.integers(1, 1000)))
        category = rng.choice(CATEGORIES)
        price = Decimal(str(round(float(np_rng.uniform(99, 150000)), 2)))
        product_rows.append((
            pid, f"SKU-{pid:07d}", f"{category} Product {pid}", category,
            rng.choice(BRANDS), price, True, False, created, created,
        ))

    order_rows = []
    order_item_rows = []
    item_id = 1
    for oid in range(1, args.orders + 1):
        customer_id = rng.randint(1, args.customers)
        proposed_order_ts = now - timedelta(
            days=int(np_rng.integers(0, 900)), minutes=int(np_rng.integers(0, 1440))
        )
        order_ts = max(proposed_order_ts, customer_created_at[customer_id])
        status = rng.choice(ORDER_STATUSES)
        payment = "REFUNDED" if status == "CANCELLED" else rng.choice(PAYMENT_STATUSES[:3])
        total = Decimal("0.00")
        for _ in range(args.items_per_order):
            product_id = rng.randint(1, args.products)
            product_price = product_rows[product_id - 1][5]
            quantity = rng.randint(1, 4)
            discount = (
                product_price * Decimal(str(rng.choice([0, 0, 0, 0.05, 0.10])))
            ).quantize(Decimal("0.01"))
            line = (product_price * quantity - discount).quantize(Decimal("0.01"))
            order_item_rows.append((
                item_id, oid, product_id, quantity, product_price, discount,
                line, "ACTIVE", False, order_ts, order_ts,
            ))
            total += line
            item_id += 1
        order_rows.append((
            oid, customer_id, order_ts, status, payment, total, "INR", False,
            order_ts, order_ts,
        ))

    conn = snowflake.connector.connect(**seed_snowflake_config())
    try:
        cur = conn.cursor()
        try:
            if not args.append:
                for table in ("ORDER_ITEMS", "ORDERS", "PRODUCTS", "CUSTOMERS"):
                    cur.execute(f"TRUNCATE TABLE {SOURCE_DB}.{SOURCE_SCHEMA}.{table}")
        finally:
            cur.close()

        counts = {}
        definitions = [
            ("CUSTOMERS", ["CUSTOMER_ID", "CUSTOMER_NAME", "EMAIL", "PHONE", "CITY", "STATE",
                           "COUNTRY", "CUSTOMER_SEGMENT", "IS_DELETED", "CREATED_AT", "UPDATED_AT"], customer_rows),
            ("PRODUCTS", ["PRODUCT_ID", "SKU", "PRODUCT_NAME", "CATEGORY", "BRAND", "UNIT_PRICE",
                          "IS_ACTIVE", "IS_DELETED", "CREATED_AT", "UPDATED_AT"], product_rows),
            ("ORDERS", ["ORDER_ID", "CUSTOMER_ID", "ORDER_TS", "ORDER_STATUS", "PAYMENT_STATUS",
                        "TOTAL_AMOUNT", "CURRENCY", "IS_DELETED", "CREATED_AT", "UPDATED_AT"], order_rows),
            ("ORDER_ITEMS", ["ORDER_ITEM_ID", "ORDER_ID", "PRODUCT_ID", "QUANTITY", "UNIT_PRICE",
                             "DISCOUNT_AMOUNT", "LINE_AMOUNT", "ITEM_STATUS", "IS_DELETED",
                             "CREATED_AT", "UPDATED_AT"], order_item_rows),
        ]
        for table, columns, rows in definitions:
            counts[table.lower()] = write_source_rows(
                conn, database=SOURCE_DB, schema=SOURCE_SCHEMA, table=table,
                columns=columns, rows=rows,
            )
        print("Snowflake source data loaded:", counts)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
