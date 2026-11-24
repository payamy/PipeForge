# 0008. Job image built on `spark-base`

- Status: Accepted
- Date: 2026-11-25

## Context
Production clusters may have unreliable internet, so nothing may download at container start. SparkLake owns the
Spark image; PipeForge must not depend on SparkLake's source. PipeForge previously required Python 3.11 and hard
depended on pyspark and delta-spark.

## Decision
- `docker/Dockerfile` builds `pipeforge-job` FROM SparkLake's published `spark-base` (build args
  `SPARK_BASE_IMAGE`, `SPARK_BASE_TAG`), installs PipeForge **without** the `spark` extra, and uses the CLI as its
  entrypoint. The build asserts pyspark was not pip-installed.
- `python_requires` is `>=3.10,<3.12`; `pyspark` and `delta-spark` are the optional `pipeforge[spark]` extra.
- Dependencies are pinned exactly.

## Consequences
Spark is not installed twice, and the jars come from one place. **Finding:** the official `spark:3.5.2-python3`
image ships Python 3.8.10, not 3.10. `spark-base` must start from an image with Python 3.10 or newer, for example
`spark:3.5.2-scala2.12-java17-python3-ubuntu`, or `pipeforge-job` cannot install. The image build is unverified
until `spark-base` exists (SparkLake Task 2).
