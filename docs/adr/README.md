# Architecture Decision Records

Short records of significant decisions: context, what was decided, why, and the consequences.
New records get the next number and start from [template.md](template.md). Records are not
rewritten after acceptance; a changed decision gets a new ADR that supersedes the old one.

| # | Decision | Status |
|---|---|---|
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0002](0002-standard-stack-and-delta-version.md) | Standard stack; Delta 3.2.0 | Accepted |
| [0003](0003-utc-source-consistent-watermark.md) | UTC, source-consistent watermark | Accepted |
| [0004](0004-checkpoint-storage-and-error-handling.md) | Checkpoint layout; errors raise | Accepted |
| [0005](0005-delta-merge-via-spark-sql.md) | Delta MERGE through Spark SQL | Accepted |
| [0006](0006-versioned-job-spec-secrets-by-reference.md) | Versioned job spec; secrets by reference | Accepted |
| [0007](0007-cli-result-line-and-exit-codes.md) | CLI result line and exit codes | Accepted |
| [0008](0008-job-image-on-spark-base.md) | Job image built on `spark-base` | Accepted |
| [0009](0009-central-identifier-and-timestamp-quoting.md) | Central identifier and timestamp quoting | Accepted |
| [0010](0010-duplicate-keys-and-schema-evolution.md) | Duplicate keys and schema evolution | Accepted |
| [0011](0011-integration-test-infrastructure.md) | Integration test infrastructure | Accepted |
| [0012](0012-breaking-changes-in-2-0-0.md) | Breaking changes in 2.0.0 | Accepted |
