# Retail Incremental Data Warehouse

An end-to-end data engineering project demonstrating **initial full loads, Snowflake-source high-watermark extraction, hash-based change detection, SCD Type 1, SCD Type 2, incremental fact loading, S3 landing, Snowflake warehousing, and Apache Airflow orchestration**.

This is deliberately a focused architecture:

- **Snowflake** is both the source platform (`RETAIL_SOURCE`) and the target warehouse (`RETAIL_DW`). No separate OLTP database is used.
- **Python** performs bounded, chunked extraction from Snowflake and writes Parquet files. It does not pull whole tables into Pandas.
- **Amazon S3** stores immutable batch files between source extraction and target loading.
- **Snowflake** owns source tables, raw ingestion, transformations, dimensions, facts, controls, and audit records.
- **Apache Airflow** schedules and orchestrates the work.
- **Power BI / Excel** consume curated Snowflake views and support reconciliation.
- **AWS Glue is intentionally not used.** Airflow orchestrates Python and Snowflake SQL directly.

> Cloud resources are not provisioned by this repository. You need an AWS account, an S3 bucket, and a Snowflake account. The same Snowflake account/credentials are used for the source and target databases. Credentials, an S3 storage integration, and a Snowflake external stage must be configured before running the full cloud DAG. Local Airflow development uses SQLite only for Airflow's own metadata; all project source and warehouse data remain in Snowflake.

## 1. Loading design

| Source | Initial load | Subsequent loads | Warehouse target |
|---|---|---|---|
| `RETAIL_SOURCE.RETAIL.CUSTOMERS` | Full snapshot; establish SHA-256 attribute hash | HWM extraction; hash only changed candidates | `RETAIL_DW.DW.DIM_CUSTOMER` — SCD2 |
| `RETAIL_SOURCE.RETAIL.PRODUCTS` | Full load | HWM extraction | `RETAIL_DW.DW.DIM_PRODUCT` — SCD1 |
| `RETAIL_SOURCE.RETAIL.ORDERS` | Full load | HWM extraction | `RETAIL_DW.DW.FACT_ORDERS` — incremental upsert |
| `RETAIL_SOURCE.RETAIL.ORDER_ITEMS` | Full load | HWM extraction | `RETAIL_DW.DW.FACT_ORDER_ITEMS` — incremental upsert |

The first run is automatically detected when there is no committed watermark for a source table. Later runs extract only rows in the recent update-time range. The customer input is **not** re-read in full every day: its initial snapshot establishes the target baseline, and later runs process only rows returned by the HWM query.

### High-watermark semantics

Each source uses a composite high watermark `(updated_at, primary_key)`. At the beginning of a batch, the extractor captures a fixed cutoff from the Snowflake source clock and pairs it with the maximum BIGINT primary-key sentinel. It fetches bounded batches from one fixed source query result without imposing an expensive global sort. The cutoff advances even when no rows changed, so an idle table does not cause the same lookback window to be scanned forever.

A configurable lookback (default 5 minutes) overlaps the last committed timestamp. This makes processing resilient to small commit delays and timestamp ties; overlapping rows are safe because staging is deduplicated and target merges are idempotent. The watermark is advanced **only after all four table pipelines and data-quality checks succeed**.

**Snowflake performance note:** Snowflake uses micro-partitions rather than row-store B-tree indexes. HWM predicates benefit from micro-partition pruning. Keep `UPDATED_AT` populated consistently, review query profiles/bytes scanned, and consider the optional day-level clustering key in `sql/03_source_clustering_optional.sql` only when table size and measured pruning justify its automatic-reclustering credit cost. Do not add clustering blindly to small tables. `SYSTEM$CLUSTERING_INFORMATION` is included as a diagnostic example.

**Important production boundary:** an `UPDATED_AT` HWM is reliable only when every source writer maintains that column and hard deletes are represented as soft deletes. A finite lookback cannot guarantee capture of arbitrarily late-committing transactions or hard deletes. The sample implements timestamp HWM intentionally. For every intermediate change at high volume, use a Snowflake Stream/change-event table or another log-based CDC feed as the source offset rather than relying on a current-state table.

### Hashing and SCD rules

- Customer hash is calculated in Snowflake over explicitly selected, normalized business attributes (including the soft-delete flag). `UPDATED_AT` and ingestion metadata are excluded.
- Unchanged customer hashes create no new version. Changed attributes expire the current version and insert a new SCD2 row.
- Product changes overwrite the active product record (SCD1); no product history is retained.
- Orders and order items are upserted by their business primary keys. Soft deletes update the fact row's `IS_DELETED` flag.
- Fact rows resolve the customer surrogate key using the SCD2 effective-date interval at the order timestamp. Order-item rows resolve the current SCD1 product surrogate key.

## 2. Repository layout

```text
retail_incremental_warehouse/
├── airflow/dags/                 # Master Airflow DAG
├── docs/                         # Architecture and operations notes
├── scripts/                      # Generate and mutate Snowflake source data
├── sql/                           # Snowflake source/target DDL, stage, SCD and fact SQL
├── src/retail_etl/                # Extractor, S3, Snowflake and pipeline code
├── tests/                         # Unit tests for HWM and deterministic hashing
├── airflow-data/                  # Local Airflow SQLite metadata (gitignored)
├── docker-compose.yml             # Local Airflow development environment
├── .env.example
└── requirements.txt
```

## 3. Prerequisites

- Docker Desktop / Docker Engine with Compose
- AWS S3 bucket and IAM permissions for `s3:PutObject`, `s3:AbortMultipartUpload`, and `s3:ListBucket` on the project prefix
- Snowflake user/role with rights to create/use both databases and schemas, create tables/sequences/stages/file formats, and execute DML
- Snowflake storage integration administrator to create the S3 integration (one-time account setup)

## 4. Configure the project

```bash
cp .env.example .env
```

Edit `.env`:

1. Set `AWS_REGION`, `S3_BUCKET`, and `S3_PREFIX`.
2. Set Snowflake account, user, password, warehouse and role.
3. Set `SOURCE_SNOWFLAKE_DATABASE=RETAIL_SOURCE` and `SOURCE_SNOWFLAKE_SCHEMA=RETAIL` for the source tables.
4. Set `SNOWFLAKE_DATABASE=RETAIL_DW` for the target warehouse.
5. Set `SNOWFLAKE_STORAGE_INTEGRATION` and `SNOWFLAKE_STAGE` to the names created in Snowflake.
6. For local S3 access, supply AWS access keys in `.env`; in deployed environments, prefer an IAM role/workload identity.

The same Snowflake connection identity is used for source reads and source-data seeding/mutation in this development project. For a shared or production environment, split those duties into least-privilege roles and use a secrets manager/key-pair authentication.

Do not commit `.env`, cloud keys, private keys, or production data.

## 5. Initialize Snowflake

Run these scripts in order in a Snowflake worksheet or with SnowSQL:

1. `sql/00_create_warehouse.sql` — creates the target database, schemas, tables, sequences, control/audit tables and reporting views.
2. `sql/02_create_source.sql` — creates the four source tables in `RETAIL_SOURCE.RETAIL`.

Both are safe to rerun because they use `IF NOT EXISTS` and do not drop source or target data.

The source primary/foreign-key declarations document intended relationships. Snowflake standard-table constraints are not enforced, so the project includes explicit data-quality checks. Snowflake has no row-level triggers; any source application or update script must set `UPDATED_AT` explicitly. The included mutation script does this with `CURRENT_TIMESTAMP()`.

## 6. Create the S3 storage integration and Snowflake stage

The Snowflake account administrator runs a one-time setup, replacing the placeholders:

```sql
CREATE STORAGE INTEGRATION RETAIL_S3_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<AWS_ACCOUNT_ID>:role/<SNOWFLAKE_S3_ROLE>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<BUCKET>/<PREFIX>/');

DESC INTEGRATION RETAIL_S3_INT;
```

Use `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` from `DESC INTEGRATION` in the AWS role trust policy. The role also needs read/list access to the exact bucket prefix. Grant integration usage to the runtime role (replace `SYSADMIN` if using another role):

```sql
GRANT USAGE ON INTEGRATION RETAIL_S3_INT TO ROLE SYSADMIN;
```

Then, in Snowflake:

```sql
CREATE FILE FORMAT IF NOT EXISTS RETAIL_DW.RAW.RETAIL_PARQUET_FF
  TYPE = PARQUET;

CREATE STAGE IF NOT EXISTS RETAIL_DW.RAW.RETAIL_S3_STAGE
  URL = 's3://<BUCKET>/<PREFIX>/'
  STORAGE_INTEGRATION = RETAIL_S3_INT
  FILE_FORMAT = RETAIL_DW.RAW.RETAIL_PARQUET_FF;
```

Set the same integration and stage names in `.env`. The stage URL prefix must match `S3_PREFIX`. Verify access with:

```sql
LIST @RETAIL_DW.RAW.RETAIL_S3_STAGE;
```

## 7. Start local Airflow

```bash
mkdir -p airflow-data
# Linux/macOS only, if Docker reports a metadata-directory permission error:
chmod 777 airflow-data
docker compose up -d --build airflow-init
# Wait for airflow-init to exit successfully, then:
docker compose up -d airflow-webserver airflow-scheduler
```

Airflow UI: `http://localhost:8080` (default development login: `admin` / `admin`; change it before any shared deployment).

The local Compose environment uses Airflow's `SequentialExecutor` with a persistent SQLite metadata file so the project does not require a separate external Airflow metadata database for local development. This is a single-node development setup; it is not a production Airflow executor. For production, use a managed Airflow service or a properly secured Airflow deployment with its supported managed metadata backend, secrets backend, TLS, and worker isolation. The pipeline's source and warehouse data remain in Snowflake.

## 8. Generate source data

After source DDL is initialized and Airflow is up, seed the Snowflake source. The script is deterministic by default and supports larger volumes:

```bash
docker compose run --rm airflow-webserver python /opt/airflow/project/scripts/generate_source_data.py \
  --customers 10000 --products 1000 --orders 50000 --items-per-order 2
```

To simulate a following day's source changes:

```bash
docker compose run --rm airflow-webserver python /opt/airflow/project/scripts/mutate_source.py \
  --customer-updates 100 --product-updates 40 --new-products 20 \
  --new-orders 500 --order-updates 100 --item-updates 100 \
  --soft-delete-customers 5
```

The mutation script updates `UPDATED_AT` and sets `IS_DELETED` rather than hard-deleting rows. The source load utilities use Snowflake's `write_pandas` bulk-loading path; the Snowflake role needs the required source-schema write privileges and permission to create temporary stages/file formats used by the connector.

## 9. Run the pipeline

In Airflow, enable `retail_incremental_warehouse_daily` and trigger it.

The DAG has four source branches:

1. Extract and upload Snowflake source batches to S3.
2. `COPY INTO` Snowflake raw tables.
3. Apply customer SCD2, product SCD1, and incremental fact merges.
4. Run quality checks for all tables.
5. Commit watermarks only after every branch and quality check succeeds.

The first successful run is a full load. Later runs are incremental. Re-triggering a batch is safe: each source batch is uniquely identified, Snowflake raw staging is de-duplicated by batch/source key, and dimension/fact mutations are idempotent.

## 10. Useful verification queries

```sql
-- Source record counts
SELECT 'CUSTOMERS' AS SOURCE_TABLE, COUNT(*) AS ROW_COUNT FROM RETAIL_SOURCE.RETAIL.CUSTOMERS
UNION ALL SELECT 'PRODUCTS', COUNT(*) FROM RETAIL_SOURCE.RETAIL.PRODUCTS
UNION ALL SELECT 'ORDERS', COUNT(*) FROM RETAIL_SOURCE.RETAIL.ORDERS
UNION ALL SELECT 'ORDER_ITEMS', COUNT(*) FROM RETAIL_SOURCE.RETAIL.ORDER_ITEMS;

-- Current and historical customer versions
SELECT customer_id, customer_name, city, valid_from, valid_to, is_current, is_deleted
FROM RETAIL_DW.DW.DIM_CUSTOMER
ORDER BY customer_id, valid_from;

-- Exactly one current row per customer (should return no rows)
SELECT customer_id, COUNT(*)
FROM RETAIL_DW.DW.DIM_CUSTOMER
WHERE is_current = TRUE
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Product SCD1 state
SELECT product_id, product_name, category, unit_price, is_deleted
FROM RETAIL_DW.DW.DIM_PRODUCT;

-- HWM/control state
SELECT * FROM RETAIL_DW.CONTROL.ETL_WATERMARK ORDER BY source_table;

-- Batch execution history
SELECT * FROM RETAIL_DW.AUDIT.ETL_BATCH_LOG ORDER BY started_at DESC;

-- Data quality results
SELECT * FROM RETAIL_DW.AUDIT.DATA_QUALITY_RESULTS ORDER BY checked_at DESC;
```

## 11. Tests

Run the unit tests locally:

```bash
python -m pip install -r requirements.txt
PYTHONPATH=src pytest -q
```

Tests cover deterministic row hashing, timestamp normalization, and composite HWM pagination boundaries. Integration tests require live Snowflake and AWS services and are not claimed to have run in this generated environment.

To export a bounded Excel reconciliation workbook from Snowflake:

```bash
docker compose run --rm airflow-webserver python /opt/airflow/project/scripts/export_audit_report.py --days 30
```

## 12. Operational notes and deliberate trade-offs

- Extraction uses a Snowflake connector cursor and bounded `fetchmany` batches; it does not materialize a huge source table in Pandas.
- Snowflake HWM performance relies on micro-partition pruning, not indexes. Inspect query profiles and bytes scanned; apply optional clustering only when volume and measured pruning justify the cost.
- Parquet is written in moderate-sized chunks. Tune `EXTRACT_CHUNK_SIZE` and `PARQUET_COMPRESSION` based on row width, source query latency, network throughput and Airflow worker memory.
- S3 paths are immutable and batch-specific: `raw/<table>/ingest_date=YYYY-MM-DD/batch_id=<uuid>/`.
- Snowflake `COPY INTO` reads only the current batch path.
- The control watermark is committed after data transforms and checks. If the DAG fails before this point, the next run overlaps and replays the same changes.
- SCD2 uses source `UPDATED_AT` as the effective timestamp. The current-state demo source exposes only the latest customer row, so multiple customer edits between extraction runs collapse to the last observed state. Use a source change-event table or Snowflake Stream when every intermediate change must be historized. If the business requires a separate business-effective date, add that column and use it instead.
- The sample schema uses soft deletes. For physical deletes, introduce a tombstone/change-event feed or periodic key reconciliation; an HWM over the current source table cannot detect a row that has vanished.
- This project demonstrates production patterns, but production deployment still requires security hardening, observability integration, secrets management, cost controls, and source-specific SLA tuning.
