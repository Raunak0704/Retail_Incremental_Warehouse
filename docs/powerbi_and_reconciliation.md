# Reporting and reconciliation ideas

## Power BI model

Recommended semantic model:

- `FACT_ORDERS` (one row per order)
- `FACT_ORDER_ITEMS` (one row per order line)
- `DIM_CUSTOMER` (SCD2; join via `CUSTOMER_SK`)
- `DIM_PRODUCT` (SCD1; join via `PRODUCT_SK`)
- A dedicated Date table related to `FACT_ORDERS[ORDER_TS]`

Do not join facts to the customer dimension by `CUSTOMER_ID` in the reporting model: use `CUSTOMER_SK` so an order retains the customer attributes valid at the order timestamp.

## Suggested DAX measures

```DAX
Gross Order Value =
CALCULATE(
    SUM(FACT_ORDERS[TOTAL_AMOUNT]),
    FACT_ORDERS[IS_DELETED] = FALSE(),
    FACT_ORDERS[ORDER_STATUS] <> "CANCELLED"
)

Order Count =
CALCULATE(
    DISTINCTCOUNT(FACT_ORDERS[ORDER_ID]),
    FACT_ORDERS[IS_DELETED] = FALSE()
)

Average Order Value =
DIVIDE([Gross Order Value], [Order Count])

Units Sold =
CALCULATE(
    SUM(FACT_ORDER_ITEMS[QUANTITY]),
    FACT_ORDER_ITEMS[IS_DELETED] = FALSE(),
    FACT_ORDER_ITEMS[ITEM_STATUS] <> "RETURNED"
)
```

## Dashboard pages

1. Executive sales: gross order value, order count, average order value, daily/monthly trend.
2. Product performance: category, brand, SKU, units sold and revenue.
3. Customer lifecycle: segment, geography, current vs historical profile, orders by customer version.
4. Operations: order/payment status, returns, cancellations and soft-deleted records.
5. Pipeline health: latest batch status, rows extracted, HWM position, failed quality checks and duration.

## Excel reconciliation

Export a batch-level reconciliation containing:

- source table and batch ID
- source rows extracted
- raw rows loaded
- deduplicated staging rows
- inserted/updated/unchanged target counts (add action counts to SQL if desired)
- rejected/quarantined rows
- final HWM timestamp and primary key
- quality-check status

A difference between source and final target row counts is not automatically an error for SCD2: the dimension intentionally has multiple historical rows per customer. Reconcile by current business keys and version counts instead.
