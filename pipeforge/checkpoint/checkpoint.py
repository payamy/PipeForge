from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
import botocore.config
from botocore.exceptions import BotoCoreError, ClientError

from pipeforge.errors import CheckpointError
from pipeforge.util.retry import retry_call
from pipeforge.util.sql import to_utc

log = logging.getLogger("pipeforge")

_NOT_FOUND = {"NoSuchKey", "404", "NotFound"}


def _safe(part: str) -> str:
    return str(part).replace("/", "_").replace("\\", "_")


def _transient_s3(exc: BaseException) -> bool:
    if isinstance(exc, ClientError):
        code = str(exc.response.get("Error", {}).get("Code", ""))
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        return code in {"SlowDown", "RequestTimeout", "InternalError", "ServiceUnavailable"} or status >= 500
    return isinstance(exc, BotoCoreError)


class Checkpoint:
    """Watermark store, in S3 or on local disk.

    Object key: ``checkpoints/<db>/<schema>/<table>.json``. The value is a
    timezone-aware UTC instant, the source-consistent high-water mark.
    ``get_checkpoint`` returns ``None`` only when no checkpoint exists. Every
    other failure raises :class:`CheckpointError`.
    """

    KEY_PREFIX = "checkpoints"
    checkpoint_dir = Path(os.getenv("PIPEFORGE_CHECKPOINT_DIR", "pipeforge_checkpoint"))
    use_s3 = False
    s3_bucket_name = None
    s3_endpoint_url = None
    s3_client = None
    retry_attempts = 3
    retry_base_delay = 1.0

    @classmethod
    def reset(cls) -> None:
        cls.use_s3 = False
        cls.s3_bucket_name = cls.s3_endpoint_url = cls.s3_client = None

    @classmethod
    def configure_s3(cls, bucket_name: str, endpoint_url: str | None, access_key: str | None,
                     secret_key: str | None, region: str | None = None, path_style: bool = True,
                     create_bucket: bool = False, verify: bool | str | None = None) -> None:
        """Configure S3 storage. A missing bucket raises unless ``create_bucket`` is True."""
        cfg = botocore.config.Config(
            signature_version="s3v4",
            s3={"addressing_style": "path" if path_style else "auto"},
            retries={"max_attempts": 2},
        )
        client = boto3.client("s3", endpoint_url=endpoint_url, aws_access_key_id=access_key,
                              aws_secret_access_key=secret_key, region_name=region or "us-east-1",
                              config=cfg, verify=verify)
        try:
            retry_call(lambda: client.head_bucket(Bucket=bucket_name), attempts=cls.retry_attempts,
                       base_delay=cls.retry_base_delay, should_retry=_transient_s3, what="head_bucket")
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchBucket", "NotFound"}:
                if not create_bucket:
                    raise CheckpointError(
                        f"checkpoint bucket '{bucket_name}' does not exist and create_bucket is disabled"
                    ) from exc
                try:
                    client.create_bucket(Bucket=bucket_name)
                except (ClientError, BotoCoreError) as exc2:
                    raise CheckpointError(f"cannot create bucket '{bucket_name}': {exc2}") from exc2
            else:
                raise CheckpointError(f"cannot access checkpoint bucket '{bucket_name}': {exc}") from exc
        except BotoCoreError as exc:
            raise CheckpointError(f"cannot reach S3 endpoint: {exc}") from exc
        cls.use_s3 = True
        cls.s3_bucket_name = bucket_name
        cls.s3_endpoint_url = endpoint_url
        cls.s3_client = client

    @classmethod
    def configure_local(cls, directory: str | os.PathLike) -> None:
        cls.use_s3 = False
        cls.checkpoint_dir = Path(directory)

    @classmethod
    def validate_connection(cls) -> None:
        """Raise CheckpointError unless the store is reachable."""
        if cls.use_s3:
            try:
                retry_call(lambda: cls.s3_client.head_bucket(Bucket=cls.s3_bucket_name),
                           attempts=cls.retry_attempts, base_delay=cls.retry_base_delay,
                           should_retry=_transient_s3, what="checkpoint validation")
            except (ClientError, BotoCoreError) as exc:
                raise CheckpointError(f"checkpoint store unreachable: {exc}") from exc
        else:
            try:
                cls.checkpoint_dir.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                raise CheckpointError(f"checkpoint dir not writable: {exc}") from exc

    @classmethod
    def object_key(cls, db: str, table: str, schema: str = "main") -> str:
        return f"{cls.KEY_PREFIX}/{_safe(db)}/{_safe(schema)}/{_safe(table)}.json"

    @staticmethod
    def _legacy_key(db: str, table: str, schema: str) -> str:
        return f"{db}-{schema}-{table}.txt"

    @staticmethod
    def _serialize(watermark: datetime) -> str:
        return json.dumps({"version": 1, "watermark": to_utc(watermark).isoformat(),
                           "written_at": datetime.now(timezone.utc).isoformat()})

    @staticmethod
    def _parse(content: str) -> datetime:
        content = content.strip()
        try:
            if content.startswith("{"):
                return to_utc(datetime.fromisoformat(json.loads(content)["watermark"]))
            # legacy "%Y-%m-%d %H:%M:%S" naive driver-local value, interpreted as UTC
            return to_utc(datetime.strptime(content, "%Y-%m-%d %H:%M:%S"))
        except (ValueError, KeyError, TypeError) as exc:
            raise CheckpointError(f"corrupt checkpoint content: {content[:80]!r}") from exc

    @classmethod
    def _s3_read(cls, key: str) -> str | None:
        def _get():
            try:
                obj = cls.s3_client.get_object(Bucket=cls.s3_bucket_name, Key=key)
                return obj["Body"].read().decode("utf-8")
            except ClientError as exc:
                if str(exc.response.get("Error", {}).get("Code", "")) in _NOT_FOUND:
                    return None
                raise
        try:
            return retry_call(_get, attempts=cls.retry_attempts, base_delay=cls.retry_base_delay,
                              should_retry=_transient_s3, what=f"read checkpoint {key}")
        except (ClientError, BotoCoreError) as exc:
            raise CheckpointError(f"cannot read checkpoint '{key}': {exc}") from exc

    @classmethod
    def get_checkpoint(cls, db: str, table: str, schema: str = "main") -> datetime | None:
        """Return the UTC watermark, or None if there is none. Errors raise."""
        key = cls.object_key(db, table, schema)
        legacy = cls._legacy_key(db, table, schema)
        if cls.use_s3:
            if cls.s3_client is None:
                raise CheckpointError("S3 is not configured. Call 'configure_s3' first.")
            content = cls._s3_read(key)
            if content is None:
                content = cls._s3_read(legacy)
                if content is not None:
                    log.warning("using legacy checkpoint %s (naive time assumed UTC)", legacy)
        else:
            content = None
            for rel in (key, legacy):
                path = cls.checkpoint_dir / rel
                try:
                    content = path.read_text(encoding="utf-8")
                    break
                except FileNotFoundError:
                    continue
                except OSError as exc:
                    raise CheckpointError(f"cannot read checkpoint '{path}': {exc}") from exc
        return None if content is None else cls._parse(content)

    @classmethod
    def set_checkpoint(cls, checkpoint_datetime: datetime, db: str, table: str,
                       schema: str = "main") -> None:
        """Persist the watermark. Raises CheckpointError on any failure."""
        key = cls.object_key(db, table, schema)
        body = cls._serialize(checkpoint_datetime)
        if cls.use_s3:
            if cls.s3_client is None:
                raise CheckpointError("S3 is not configured. Call 'configure_s3' first.")
            try:
                retry_call(lambda: cls.s3_client.put_object(
                    Bucket=cls.s3_bucket_name, Key=key, Body=body.encode("utf-8"),
                    ContentType="application/json"),
                    attempts=cls.retry_attempts, base_delay=cls.retry_base_delay,
                    should_retry=_transient_s3, what=f"write checkpoint {key}")
            except (ClientError, BotoCoreError) as exc:
                raise CheckpointError(f"cannot write checkpoint '{key}': {exc}") from exc
        else:
            path = cls.checkpoint_dir / key
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                tmp = path.with_suffix(".tmp")
                tmp.write_text(body, encoding="utf-8")
                os.replace(tmp, path)
            except OSError as exc:
                raise CheckpointError(f"cannot write checkpoint '{path}': {exc}") from exc
