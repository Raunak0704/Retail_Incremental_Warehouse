# Operations runbook

## Before first run

- Confirm `RETAIL_SOURCE.RETAIL` contains all four source tables.
- Confirm source writers maintain `UPDATED_AT` on every update and use `TIMESTAMP_TZ` values consistently.
- Confirm source deletes are soft deletes.
- Confirm the role used by the source generator can insert/update source tables and create the temporary stages/file formats required by `write_pandas`.
- Confirm S3 bucket encryption and IAM permissions.
- Confirm Snowflake storage integration, external stage and warehouse permissions.
- Run the target and source Snowflake DDL scripts.

## First full load

Run the Airflow DAG once. The absence of a row in `RETAIL_DW.CONTROL.ETL_WATERMARK` causes the extractor to select `load_mode=full`. All source records at or before the batch cutoff are extracted in bounded chunks. The initial customer hash is computed while creating `DIM_CUSTOMER`; the other tables initialize their respective targets.

## Subsequent incremental run

Run the DAG again after using `scripts/mutate_source.py`. The extractor reads only the configured HWM time window, writes only returned rows, and advances the committed cutoff after the global quality gate.

## Failure handling

- If extraction fails, the HWM is unchanged.
- If S3 upload fails partway through, the task fails and the HWM is unchanged. Orphaned batch files can lifecycle-expire after review.
- If COPY fails, the HWM is unchanged; rerun the task/DAG.
- If a transform fails, the batch staging slice is rebuilt on retry; target operations are idempotent.
- If quality fails, failure details are stored in `AUDIT.DATA_QUALITY_RESULTS`; no watermark is advanced.
- After recovery, rerun the DAG. The overlap window replays recent changes.

## Scale tuning

- Start with 50,000 rows per chunk. Tune based on row width, source query latency, network throughput and Airflow worker memory.
- Keep Parquet files at a practical size (roughly 128–512 MB compressed is a useful target, not a hard limit).
- Use a dedicated Snowflake source role with SELECT privileges for extraction and a separate writer identity for controlled data generation/mutation where practical.
- Review Snowflake query profile, partitions scanned, bytes scanned and `SYSTEM$CLUSTERING_INFORMATION` for the source HWM query.
- Apply `sql/03_source_clustering_optional.sql` only when measured pruning justifies automatic-reclustering credits; small tables generally do not need clustering.
- If an update-time range is still too large for the daily SLA, increase cadence, tune clustering after measurement, or migrate the source feed to a Snowflake Stream/change-event CDC pattern rather than weakening correctness.
- Keep `max_active_runs=1` unless batches are explicitly isolated and watermark updates are made concurrency-safe.
