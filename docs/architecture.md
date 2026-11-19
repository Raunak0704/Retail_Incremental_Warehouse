# Architecture and design decisions

## Data flow

1. Airflow starts one batch with four extraction branches.
2. Each extractor reads the committed watermark from the target Snowflake control schema, captures a source-clock upper cutoff from `RETAIL_SOURCE`, and performs bounded Snowflake extraction.
3. Python writes each fetched chunk to Parquet and uploads it to an immutable S3 batch path.
4. Target Snowflake `COPY INTO` loads only that batch path into an append-only RAW table.
5. SQL rebuilds the batch-specific typed staging slice and applies SCD/merge logic.
6. A dependency barrier ensures customers/products are transformed before facts that need their surrogate keys.
7. Quality checks are recorded in target Snowflake. Failed checks fail the DAG and prevent watermark advancement.
8. A final Snowflake transaction writes the batch audit records and advances all four HWM rows.

## High-watermark algorithm

The persistent offset contains `LAST_SUCCESS_UPDATED_AT` and `LAST_SUCCESS_PRIMARY_KEY`. At the start of a run, the source Snowflake clock is captured as the immutable upper timestamp, with a maximum BIGINT key sentinel. For later runs, extraction uses the inclusive lower timestamp (previous committed timestamp minus the configured lookback) and fixed composite upper bound:

```sql
WHERE UPDATED_AT >= :lower_timestamp
  AND (UPDATED_AT < :upper_timestamp
       OR (UPDATED_AT = :upper_timestamp AND SOURCE_PK <= :upper_pk))
```

The lower timestamp deliberately uses primary-key floor `0` so every key sharing the overlap timestamp is replayed. The upper timestamp uses the maximum BIGINT sentinel so every key at the captured source-clock cutoff is included. A Snowflake connector cursor reads at most `EXTRACT_CHUNK_SIZE` records at a time from the same statement result. No global `ORDER BY` is imposed, avoiding a large sort during full extraction; the chunk/file order is not semantically significant. The batch upper cutoff is fixed before reading so changes after that boundary are deferred to the next run. The cutoff advances even when there are no changed rows.

Snowflake does not provide traditional B-tree indexes. The source predicate is optimized by micro-partition pruning; table clustering may improve pruning for very large change tables but has a compute/storage maintenance cost. Measure query profiles and `SYSTEM$CLUSTERING_INFORMATION` before applying the optional day-level `UPDATED_AT` clustering key. Small tables generally do not need it.

The lookback overlap is deliberate. It catches small commit delays and makes equal timestamps safe. Replayed rows are deduplicated in staging and compared by source update timestamp/hash before mutation. A finite overlap is not a substitute for a change-event stream if transactions may become visible arbitrarily late.

## Why hash only customers?

The customer source represents a full-load-to-incremental migration pattern: the initial full snapshot creates a hash baseline, then HWM extraction limits subsequent rows to recently updated customers. The hash distinguishes meaningful tracked-attribute changes from technical updates. Product SCD1 and both fact tables use direct incremental upsert logic, so hashing each of those rows would add complexity without adding value to the teaching objective.

## Source timestamp maintenance

Snowflake tables do not provide row-level update triggers. Every source writer must set `UPDATED_AT` whenever it changes a row. The included `mutate_source.py` uses `CURRENT_TIMESTAMP()` on each update. Production source applications must follow the same contract or publish a CDC/change-event stream.

## Retry and recovery behavior

- S3 batch prefixes are unique and immutable.
- Snowflake COPY file-load metadata avoids reloading the same file on normal retries.
- Staging rows are deleted and rebuilt by batch ID before each transform attempt.
- SCD2 checks for a current matching version before inserting; SCD1/facts use MERGE.
- A retry after successful transforms but before the final HWM commit is safe: target merges see the same version/source timestamp and do not create duplicates.
- HWM commits happen only after the quality barrier.

## Known boundaries

- Source deletes are modeled as soft deletes (`IS_DELETED`). Physical deletes cannot be detected with a current-table HWM query.
- Timestamps are assumed to be trustworthy, maintained by all writers, stored in `TIMESTAMP_TZ`, and populated for every row.
- The effective-from timestamp for a customer's first warehouse version uses source `CREATED_AT`; subsequent SCD2 versions use source `UPDATED_AT`. This allows historical orders to resolve to the initial known customer version, but it does not reconstruct changes that happened before the warehouse began tracking history.
- Because the demo source exposes a current-state table plus `UPDATED_AT`, multiple updates to the same customer between extraction runs collapse to the latest row. SCD2 records each observed batch state, not every intermediate source mutation. For every-change history, replace the current-state HWM feed with a Snowflake Stream or source change-event table and process each event in sequence.
- Snowflake standard-table primary/unique constraints are not enforced. The DAG runs explicit duplicate checks instead.
- The local Airflow Compose stack uses SQLite only for Airflow metadata and `SequentialExecutor`; the project's source and analytical data are Snowflake-only. Production Airflow should use a supported managed deployment and metadata backend.
- This reference build uses password authentication. A production deployment should use key-pair authentication or a secrets manager-backed identity and avoid static credentials.
