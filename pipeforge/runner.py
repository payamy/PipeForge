"""Execute a validated job spec and produce the machine-readable result."""
from __future__ import annotations

import importlib
import logging
import time
from datetime import timedelta
from typing import Any, Dict, List

from pipeforge.checkpoint import Checkpoint
from pipeforge.connector.postgresql import PostgresConnector
from pipeforge.engine.spark import SparkWrapper
from pipeforge.errors import SpecError
from pipeforge.sink.delta import DeltaSink
from pipeforge.spec import resolve_secrets, validate_spec

log = logging.getLogger("pipeforge")


def _load_transformers(paths: List[str]) -> List[type]:
    classes = []
    for ref in paths:
        mod, _, name = ref.partition(":")
        try:
            classes.append(getattr(importlib.import_module(mod), name))
        except (ImportError, AttributeError) as exc:
            raise SpecError(f"cannot import transformer '{ref}': {exc}") from exc
    return classes


def configure_checkpoints(resolved: Dict[str, Any]) -> None:
    st = resolved["storage"]
    s3 = st["s3"]
    Checkpoint.retry_attempts = resolved.get("retry", {}).get("attempts", 3)
    Checkpoint.retry_base_delay = resolved.get("retry", {}).get("base_delay_seconds", 1.0)
    Checkpoint.configure_s3(
        bucket_name=st["checkpoint_bucket"], endpoint_url=s3["endpoint"],
        access_key=s3["access_key"], secret_key=s3["secret_key"], region=s3.get("region"),
        path_style=s3.get("path_style", True), create_bucket=st.get("create_checkpoint_bucket", False))


def build_connector(resolved: Dict[str, Any]) -> PostgresConnector:
    src, tgt, st = resolved["source"], resolved["target"], resolved["storage"]
    sp = resolved.get("spark", {})
    conn = src["connection"]
    retry = resolved.get("retry", {})
    engine = SparkWrapper(
        master=sp.get("master", "local[*]"), app_name=sp.get("app_name", "pipeforge"),
        enable_delta=True, warehouse_location=st["warehouse"],
        metastore_uri=resolved.get("metastore", {}).get("uri"),
        spark_conf=sp.get("conf"), s3=dict(st["s3"]))
    sink = DeltaSink(
        table=tgt["table"], db_name=tgt["database"], partition_col=tgt.get("partition_by"),
        hive_style=bool(tgt.get("partition_by")), unique_field=src.get("unique_key"),
        on_duplicate_keys=tgt.get("on_duplicate_keys", "fail"),
        order_by=src.get("cursor_fields"), schema_evolution=tgt.get("schema_evolution", False))
    return PostgresConnector(
        engine=engine, sink=sink, cursor_fields=src.get("cursor_fields", []),
        host=conn["host"], port=conn.get("port", 5432), db=conn["database"],
        username=conn["username"], password=conn["password"], sslmode=conn.get("sslmode"),
        table=src["table"], schema=src.get("schema", "public"), columns=src.get("columns"),
        transformers=_load_transformers(resolved.get("transformers", [])),
        read_partition_column=src.get("read_partition_column"),
        partition_size=src.get("read_partition_size", 10000),
        max_partitions=src.get("max_read_partitions", 64),
        retry_attempts=retry.get("attempts", 3), retry_base_delay=retry.get("base_delay_seconds", 1.0))


def run_spec(spec: Dict[str, Any], environ: Dict[str, str] | None = None) -> Dict[str, Any]:
    """Run a job. Returns the result dict (``status`` = succeeded). Raises on failure."""
    validate_spec(spec)
    resolved = resolve_secrets(spec, environ)
    start = time.monotonic()
    configure_checkpoints(resolved)
    connector = build_connector(resolved)
    res = connector.migrate(
        full_refresh=resolved["mode"] == "full",
        incremental_offset=timedelta(seconds=resolved.get("incremental", {}).get("lookback_seconds", 0)),
        on_missing_checkpoint=resolved.get("on_missing_checkpoint", "full_refresh"))
    out = res.to_dict()
    out.update(status="succeeded", duration_seconds=round(time.monotonic() - start, 3),
               run_id=spec.get("run_id"), spec_version=spec["spec_version"], error=None)
    return out


def check_spec(spec: Dict[str, Any], environ: Dict[str, str] | None = None) -> None:
    """Connection validation only (source and checkpoint store); loads nothing."""
    validate_spec(spec)
    resolved = resolve_secrets(spec, environ)
    configure_checkpoints(resolved)
    Checkpoint.validate_connection()
    build_connector(resolved).validate_connection()
