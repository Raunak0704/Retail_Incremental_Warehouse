from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import boto3


def s3_client(region: str):
    return boto3.client("s3", region_name=region)


def upload_batch(
    *,
    bucket: str,
    base_prefix: str,
    table: str,
    batch_id: str,
    local_files: list[Path],
    manifest: dict,
    region: str,
    sse: str = "AES256",
) -> str:
    client = s3_client(region)
    ingest_date = datetime.now(timezone.utc).date().isoformat()
    prefix = f"{base_prefix}/raw/{table}/ingest_date={ingest_date}/batch_id={batch_id}/".lstrip("/")

    for file_path in local_files:
        key = prefix + file_path.name
        extra = {"ServerSideEncryption": sse} if sse else {}
        client.upload_file(str(file_path), bucket, key, ExtraArgs=extra)

    manifest_key = prefix + "_manifest.json"
    client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(manifest, default=str, indent=2).encode("utf-8"),
        ContentType="application/json",
        ServerSideEncryption=sse if sse else "AES256",
    )
    return prefix
