# 0004. Checkpoint layout; errors raise

- Status: Accepted
- Date: 2026-11-25

## Context
`get_checkpoint` swallowed S3 errors and returned `None`, so `migrate()` silently ran a FULL REFRESH that
overwrote the table. `set_checkpoint` printed errors. `configure_s3` assumed `get_bucket_location` returns `None`
for a missing bucket (it raises) and auto-created buckets. Every read listed the whole bucket.

## Decision
- "Not found" returns `None`; every other failure raises `CheckpointError`. `set_checkpoint` raises too.
- Key layout: `checkpoints/<db>/<schema>/<table>.json`, fetched with a direct `get_object`, never a listing.
  Legacy `<db>-<schema>-<table>.txt` is still read.
- `configure_s3` checks the bucket with `head_bucket` and creates it only when `create_bucket=True`.
- Transient S3 errors retry with backoff; permission and missing-bucket errors do not.
- A missing checkpoint on an incremental job does an initial full load (or fails with
  `on_missing_checkpoint: fail`). A checkpoint read error always fails the job.

## Consequences
A storage outage can no longer wipe a table. Operators must create the checkpoint bucket (or opt in to creation).
The checkpoint is written after the load, so a failed write leaves data loaded; a rerun is safe because loads are
idempotent.
