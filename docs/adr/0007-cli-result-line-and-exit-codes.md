# 0007. CLI result line and exit codes

- Status: Accepted
- Date: 2026-11-25

## Context
A control plane reads job outcome from pod logs, which also contain verbose Spark output.

## Decision
- The last stdout line of `pipeforge run` is always `PIPEFORGE_RESULT <json>`, on success and failure, with
  `status`, `mode`, `initial_load`, `rows_read`, `rows_written`, `old_watermark`, `new_watermark`,
  `duration_seconds`, `run_id`, `spec_version`, `error`.
- Exit codes: `0` success, `1` job failure, `2` invalid spec.
- Logs go to stderr; stdout carries only the result line.

## Consequences
Parsing is "last line starting with the prefix". Exit codes work without parsing. Failure before the spec is read
still prints a result line with `status: failed`.
