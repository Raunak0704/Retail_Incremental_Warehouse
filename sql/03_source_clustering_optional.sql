-- OPTIONAL SCALE TUNING.
-- Snowflake does not use B-tree indexes. HWM queries benefit from micro-partition pruning.
-- Apply clustering only after measuring large source tables with query profiles and
-- SYSTEM$CLUSTERING_INFORMATION; automatic reclustering consumes credits.
-- A day-level key is a reasonable starting point for timestamp-based incremental feeds.

ALTER TABLE RETAIL_SOURCE.RETAIL.CUSTOMERS CLUSTER BY (TO_DATE(UPDATED_AT));
ALTER TABLE RETAIL_SOURCE.RETAIL.PRODUCTS CLUSTER BY (TO_DATE(UPDATED_AT));
ALTER TABLE RETAIL_SOURCE.RETAIL.ORDERS CLUSTER BY (TO_DATE(UPDATED_AT));
ALTER TABLE RETAIL_SOURCE.RETAIL.ORDER_ITEMS CLUSTER BY (TO_DATE(UPDATED_AT));

-- Example clustering-depth inspection:
-- SELECT SYSTEM$CLUSTERING_INFORMATION(
--   'RETAIL_SOURCE.RETAIL.CUSTOMERS', '(TO_DATE(UPDATED_AT))'
-- );
