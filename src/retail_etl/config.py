from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class TableConfig:
    source_table: str
    primary_key: str
    source_schema: str = "RETAIL"


TABLES: dict[str, TableConfig] = {
    "customers": TableConfig("CUSTOMERS", "CUSTOMER_ID"),
    "products": TableConfig("PRODUCTS", "PRODUCT_ID"),
    "orders": TableConfig("ORDERS", "ORDER_ID"),
    "order_items": TableConfig("ORDER_ITEMS", "ORDER_ITEM_ID"),
}


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _snowflake_base_config() -> dict:
    config = {
        "account": _required("SNOWFLAKE_ACCOUNT"),
        "user": _required("SNOWFLAKE_USER"),
        "warehouse": _required("SNOWFLAKE_WAREHOUSE"),
        "autocommit": False,
    }
    password = os.getenv("SNOWFLAKE_PASSWORD", "").strip()
    if password:
        config["password"] = password
    role = os.getenv("SNOWFLAKE_ROLE", "").strip()
    if role:
        config["role"] = role
    if not password:
        raise RuntimeError(
            "SNOWFLAKE_PASSWORD is not set. For key-pair authentication, extend "
            "the Snowflake connection configuration with private_key_file/private_key_file_pwd."
        )
    return config


def source_snowflake_config() -> dict:
    """Connection to the Snowflake source database/schema."""
    config = _snowflake_base_config()
    config.update({
        "database": os.getenv("SOURCE_SNOWFLAKE_DATABASE", "RETAIL_SOURCE").strip(),
        "schema": os.getenv("SOURCE_SNOWFLAKE_SCHEMA", "RETAIL").strip(),
        "application": "retail_hwm_extractor",
        "autocommit": True,
    })
    return config


def seed_snowflake_config() -> dict:
    """Source connection used by the deterministic seeder and mutation utility."""
    config = source_snowflake_config()
    config["application"] = "retail_source_data_generator"
    return config


def snowflake_config() -> dict:
    """Connection to the target Snowflake warehouse."""
    config = _snowflake_base_config()
    config.update({
        "database": os.getenv("SNOWFLAKE_DATABASE", "RETAIL_DW").strip(),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "CONTROL").strip(),
        "application": "retail_incremental_warehouse",
    })
    return config


def pipeline_config() -> dict:
    chunk_size = int(os.getenv("EXTRACT_CHUNK_SIZE", "50000"))
    lookback_minutes = int(os.getenv("HWM_LOOKBACK_MINUTES", "5"))
    s3_prefix = os.getenv("S3_PREFIX", "retail-incremental-warehouse").strip("/")
    if chunk_size < 1:
        raise ValueError("EXTRACT_CHUNK_SIZE must be greater than zero")
    if lookback_minutes < 0:
        raise ValueError("HWM_LOOKBACK_MINUTES cannot be negative")
    if not s3_prefix:
        raise ValueError("S3_PREFIX cannot be empty")
    return {
        "aws_region": os.getenv("AWS_REGION", "ap-south-1"),
        "s3_bucket": _required("S3_BUCKET"),
        "s3_prefix": s3_prefix,
        "s3_sse": os.getenv("S3_SSE", "AES256"),
        "chunk_size": chunk_size,
        "lookback_minutes": lookback_minutes,
        "parquet_compression": os.getenv("PARQUET_COMPRESSION", "snappy"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "RETAIL_DW"),
        "stage": os.getenv("SNOWFLAKE_STAGE", "RETAIL_S3_STAGE"),
        "source_database": os.getenv("SOURCE_SNOWFLAKE_DATABASE", "RETAIL_SOURCE"),
        "source_schema": os.getenv("SOURCE_SNOWFLAKE_SCHEMA", "RETAIL"),
    }
