# 0006. Versioned job spec; secrets by reference

- Status: Accepted
- Date: 2026-11-25

## Context
The data-platform control plane needs a stable, machine-checkable way to describe a job, and must be able to put
credentials in a Kubernetes Secret rather than in the spec.

## Decision
- The spec is JSON with `spec_version: "1"`. The JSON Schema is published at `schemas/job-spec.v1.schema.json`
  and packaged with the library; a test keeps the two copies identical.
- `username`, `password`, `access_key`, `secret_key` accept only `{"env": NAME}` or `{"file": PATH}`. Inline values
  fail validation. Secrets are resolved in memory at start-up and never logged.
- The spec comes from `--spec FILE`, `PIPEFORGE_SPEC_PATH`, or inline `PIPEFORGE_SPEC`.
- Unknown keys are rejected (`additionalProperties: false`) so typos fail early.

## Consequences
A control plane can render specs and validate them against the published schema. Incompatible changes require
`spec_version: "2"` and a new schema file. Non-secret fields (host, bucket) are plain values.
