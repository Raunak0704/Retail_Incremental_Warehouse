from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
import random

import snowflake.connector

from retail_etl.config import seed_snowflake_config
from retail_etl.source_writer import write_source_rows

CITIES = ["Hyderabad", "Bengaluru", "Pune", "Mumbai", "Chennai", "Delhi", "Kochi"]
SEGMENTS = ["STANDARD", "SILVER", "GOLD", "PLATINUM"]
CATEGORIES = ["Electronics", "Home", "Fashion", "Books", "Sports", "Beauty"]
SOURCE_DB = "RETAIL_SOURCE"
SOURCE_SCHEMA = "RETAIL"


def main():
    parser = argparse.ArgumentParser(description="Simulate daily changes in Snowflake source tables")
    parser.add_argument("--customer-updates", type=int, default=100)
    parser.add_argument("--product-updates", type=int, default=40)
    parser.add_argument("--new-products", type=int, default=20)
    parser.add_argument("--new-orders", type=int, default=500)
    parser.add_argument("--order-updates", type=int, default=100)
    parser.add_argument("--item-updates", type=int, default=100)
    parser.add_argument("--soft-delete-customers", type=int, default=5)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()
    if min(args.customer_updates, args.product_updates, args.new_products,
           args.new_orders, args.order_updates, args.item_updates,
           args.soft_delete_customers) < 0:
        parser.error("Counts cannot be negative")

    rng = random.Random(args.seed)
    conn = snowflake.connector.connect(**seed_snowflake_config())
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                f"SELECT CUSTOMER_ID, CITY, CUSTOMER_SEGMENT FROM {SOURCE_DB}.{SOURCE_SCHEMA}.CUSTOMERS "
                "WHERE IS_DELETED = FALSE ORDER BY RANDOM() LIMIT %s",
                (args.customer_updates + args.soft_delete_customers,),
            )
            selected_customers = cur.fetchall()
            for cid, current_city, current_segment in selected_customers[:args.customer_updates]:
                next_city = rng.choice([city for city in CITIES if city != current_city])
                next_segment = rng.choice([segment for segment in SEGMENTS if segment != current_segment])
                cur.execute(
                    f"UPDATE {SOURCE_DB}.{SOURCE_SCHEMA}.CUSTOMERS "
                    "SET CITY=%s, CUSTOMER_SEGMENT=%s, UPDATED_AT=CURRENT_TIMESTAMP() "
                    "WHERE CUSTOMER_ID=%s",
                    (next_city, next_segment, cid),
                )
            for cid, _, _ in selected_customers[args.customer_updates:]:
                cur.execute(
                    f"UPDATE {SOURCE_DB}.{SOURCE_SCHEMA}.CUSTOMERS "
                    "SET IS_DELETED=TRUE, UPDATED_AT=CURRENT_TIMESTAMP() WHERE CUSTOMER_ID=%s",
                    (cid,),
                )

            cur.execute(
                f"SELECT PRODUCT_ID FROM {SOURCE_DB}.{SOURCE_SCHEMA}.PRODUCTS "
                "WHERE IS_DELETED=FALSE ORDER BY RANDOM() LIMIT %s", (args.product_updates,)
            )
            for (pid,) in cur.fetchall():
                cur.execute(
                    f"UPDATE {SOURCE_DB}.{SOURCE_SCHEMA}.PRODUCTS "
                    "SET UNIT_PRICE=ROUND(UNIT_PRICE * %s, 2), UPDATED_AT=CURRENT_TIMESTAMP() "
                    "WHERE PRODUCT_ID=%s",
                    (Decimal(str(round(rng.uniform(1.01, 1.12), 5))), pid),
                )

            cur.execute(f"SELECT COALESCE(MAX(PRODUCT_ID), 0) FROM {SOURCE_DB}.{SOURCE_SCHEMA}.PRODUCTS")
            next_product = int(cur.fetchone()[0]) + 1
            cur.execute(f"SELECT COALESCE(MAX(ORDER_ID), 0) FROM {SOURCE_DB}.{SOURCE_SCHEMA}.ORDERS")
            next_order = int(cur.fetchone()[0]) + 1
            cur.execute(f"SELECT COALESCE(MAX(ORDER_ITEM_ID), 0) FROM {SOURCE_DB}.{SOURCE_SCHEMA}.ORDER_ITEMS")
            next_item = int(cur.fetchone()[0]) + 1
            cur.execute(
                f"SELECT CUSTOMER_ID FROM {SOURCE_DB}.{SOURCE_SCHEMA}.CUSTOMERS "
                "WHERE IS_DELETED=FALSE ORDER BY CUSTOMER_ID"
            )
            customer_ids = [row[0] for row in cur.fetchall()]
            cur.execute(
                f"SELECT PRODUCT_ID, UNIT_PRICE FROM {SOURCE_DB}.{SOURCE_SCHEMA}.PRODUCTS "
                "WHERE IS_DELETED=FALSE ORDER BY PRODUCT_ID"
            )
            products = cur.fetchall()

            now = datetime.now(timezone.utc)
            product_rows = []
            for pid in range(next_product, next_product + args.new_products):
                category = rng.choice(CATEGORIES)
                product_rows.append((
                    pid, f"SKU-{pid:07d}", f"{category} New Product {pid}", category,
                    "NewBrand", Decimal(str(round(rng.uniform(199, 70000), 2))),
                    True, False, now, now,
                ))
            if product_rows:
                write_source_rows(
                    conn, database=SOURCE_DB, schema=SOURCE_SCHEMA, table="PRODUCTS",
                    columns=["PRODUCT_ID", "SKU", "PRODUCT_NAME", "CATEGORY", "BRAND", "UNIT_PRICE",
                             "IS_ACTIVE", "IS_DELETED", "CREATED_AT", "UPDATED_AT"],
                    rows=product_rows,
                )
                products.extend((row[0], row[5]) for row in product_rows)

            if args.new_orders and not customer_ids:
                raise RuntimeError("No active customers available to create new orders")
            if args.new_orders and not products:
                raise RuntimeError("No active products available to create new orders")

            order_rows = []
            item_rows = []
            for offset in range(args.new_orders):
                oid = next_order + offset
                customer_id = rng.choice(customer_ids)
                status = rng.choice(["PLACED", "PROCESSING", "SHIPPED"])
                item_count = rng.randint(1, 3)
                order_total = Decimal("0.00")
                for _ in range(item_count):
                    product_id, price = rng.choice(products)
                    quantity = rng.randint(1, 3)
                    line = (price * quantity).quantize(Decimal("0.01"))
                    item_rows.append((
                        next_item, oid, product_id, quantity, price, Decimal("0.00"),
                        line, "ACTIVE", False, now, now,
                    ))
                    order_total += line
                    next_item += 1
                order_rows.append((
                    oid, customer_id, now, status, "PAID", order_total, "INR", False, now, now,
                ))

            if order_rows:
                write_source_rows(
                    conn, database=SOURCE_DB, schema=SOURCE_SCHEMA, table="ORDERS",
                    columns=["ORDER_ID", "CUSTOMER_ID", "ORDER_TS", "ORDER_STATUS", "PAYMENT_STATUS",
                             "TOTAL_AMOUNT", "CURRENCY", "IS_DELETED", "CREATED_AT", "UPDATED_AT"],
                    rows=order_rows,
                )
            if item_rows:
                write_source_rows(
                    conn, database=SOURCE_DB, schema=SOURCE_SCHEMA, table="ORDER_ITEMS",
                    columns=["ORDER_ITEM_ID", "ORDER_ID", "PRODUCT_ID", "QUANTITY", "UNIT_PRICE",
                             "DISCOUNT_AMOUNT", "LINE_AMOUNT", "ITEM_STATUS", "IS_DELETED",
                             "CREATED_AT", "UPDATED_AT"],
                    rows=item_rows,
                )

            cur.execute(
                f"SELECT ORDER_ID, ORDER_STATUS, PAYMENT_STATUS FROM {SOURCE_DB}.{SOURCE_SCHEMA}.ORDERS "
                "WHERE IS_DELETED=FALSE ORDER BY RANDOM() LIMIT %s", (args.order_updates,)
            )
            for oid, current_status, current_payment in cur.fetchall():
                next_status = rng.choice([
                    value for value in ["PLACED", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]
                    if value != current_status
                ])
                next_payment = rng.choice([
                    value for value in ["PAID", "PENDING", "REFUNDED", "FAILED"]
                    if value != current_payment
                ])
                cur.execute(
                    f"UPDATE {SOURCE_DB}.{SOURCE_SCHEMA}.ORDERS "
                    "SET ORDER_STATUS=%s, PAYMENT_STATUS=%s, UPDATED_AT=CURRENT_TIMESTAMP() "
                    "WHERE ORDER_ID=%s",
                    (next_status, next_payment, oid),
                )

            cur.execute(
                f"SELECT ORDER_ITEM_ID, ITEM_STATUS FROM {SOURCE_DB}.{SOURCE_SCHEMA}.ORDER_ITEMS "
                "WHERE IS_DELETED=FALSE ORDER BY RANDOM() LIMIT %s", (args.item_updates,)
            )
            for item_id, current_status in cur.fetchall():
                next_status = rng.choice([
                    value for value in ["ACTIVE", "RETURNED", "REFUNDED"] if value != current_status
                ])
                cur.execute(
                    f"UPDATE {SOURCE_DB}.{SOURCE_SCHEMA}.ORDER_ITEMS "
                    "SET ITEM_STATUS=%s, UPDATED_AT=CURRENT_TIMESTAMP() WHERE ORDER_ITEM_ID=%s",
                    (next_status, item_id),
                )
        finally:
            cur.close()

        print("Snowflake source mutation complete:", {
            "customer_updates": min(args.customer_updates, len(selected_customers)),
            "soft_deleted_customers": max(0, len(selected_customers) - args.customer_updates),
            "product_updates": args.product_updates,
            "new_products": args.new_products,
            "new_orders": args.new_orders,
            "order_updates": args.order_updates,
            "item_updates": args.item_updates,
        })
    finally:
        conn.close()


if __name__ == "__main__":
    main()
