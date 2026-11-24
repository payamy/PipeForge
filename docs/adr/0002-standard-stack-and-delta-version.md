# 0002. Standard stack; Delta 3.2.0

- Status: Accepted
- Date: 2026-11-25

## Context
The projects must share one stack. PipeForge pinned `delta-spark~=3.0.0`. SparkLake's Helm chart used Delta 3.0.0
but its docker-compose pulled 3.3.0 at runtime onto a 3.0.0 image. We needed one Delta 3.x version compatible with
Spark 3.5.2, checked against published metadata rather than assumed:

| Delta | `delta-spark` pip requires | Built against Spark |
|---|---|---|
| 3.2.0 | `pyspark>=3.5.0,<3.6` | 3.5.0 |
| 3.2.1 | `pyspark>=3.5.3,<3.6` | 3.5.3 |
| 3.3.0 / 3.3.1 / 3.3.2 | `pyspark>=3.5.3,<3.6` | 3.5.3 |
| 3.3.3 | `pyspark>=3.5.3,<3.6` | 3.5.6 |

## Decision
Standardise on Spark 3.5.2, Scala 2.12, **Delta 3.2.0**, hadoop-aws 3.3.4 with AWS SDK bundle 1.12.262 (the bundle
hadoop-aws 3.3.4 was built against), Postgres JDBC 42.7.3.

## Consequences
Delta 3.2.0 is the newest 3.x whose own metadata declares support for pyspark 3.5.2, so `pip install
pipeforge[spark]` resolves without overriding pins. We give up newer 3.3.x features. 3.3.x jars might run fine on
Spark 3.5.2, but that is undeclared and untested here; revisit by upgrading Spark to 3.5.3 or later across all
projects at once. Jars are baked into images, never resolved at container start.
