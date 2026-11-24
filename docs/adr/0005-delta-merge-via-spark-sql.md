# 0005. Delta MERGE through Spark SQL

- Status: Accepted
- Date: 2026-11-25

## Context
The merge used `from delta import DeltaTable`. The first integration run failed with `ModuleNotFoundError: delta`:
the `spark-base` image bakes in the Delta **jars** only. The Python package comes from `delta-spark` on pip, whose
metadata requires a pyspark of its own (see ADR 0002) and which we deliberately do not install in the job image
(ADR 0008).

## Decision
Run the merge as `MERGE INTO ... USING <temp view> ... UPDATE SET * / INSERT *` through Spark SQL, and read affected
rows from the statement's result. PipeForge's runtime code does not import `delta`.

## Consequences
The job image needs only the jars on the Spark classpath. Behaviour is the same for the user. `pipeforge[spark]`
still installs `delta-spark` for development outside an image. Per-row metrics rely on Delta's MERGE result columns,
with a batch-count fallback.
