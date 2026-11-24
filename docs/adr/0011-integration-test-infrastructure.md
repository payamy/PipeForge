# 0011. Integration test infrastructure

- Status: Accepted
- Date: 2026-11-25

## Context
Integration tests need Postgres, an S3-compatible store and Spark with the standard jars, runnable locally and in CI.

## Decision
`docker-compose.test.yml` runs Postgres 16.4, **RustFS 1.0.6** and a test runner built from `docker/Dockerfile.test`
(`spark:3.5.2-scala2.12-java17-python3-ubuntu` plus the baked jars). The runner sets `TZ=Asia/Tehran` on purpose.
The tests wait for the S3 endpoint themselves because the RustFS image's tooling for a healthcheck is unknown.

## Consequences
**Findings that shaped this:**
- `minio/minio` is no longer pullable from Docker Hub and the pinned quay.io tag returned 401, so MinIO was dropped.
- RustFS 1.0.6 (2026-11-15) is the current stable release and the production target, so the tests exercise it.
- `spark:3.5.2-python3` has Python 3.8; the Ubuntu variant has 3.10 (see ADR 0008).
- Image tags are pinned but not digest-pinned, and the jars are downloaded without checksums at test-image build time.
