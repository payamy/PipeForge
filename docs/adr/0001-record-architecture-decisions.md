# 0001. Record architecture decisions

- Status: Accepted
- Date: 2026-11-25

## Context
PipeForge is one of four separate projects (PipeForge, SparkLake, rustfs-deploy, data-platform) that
integrate only through published images, Helm values, documented env vars and job-spec formats. Choices made
here (versions, spec format, exit codes) are contracts for the other projects, and reviewers need to see why.

## Decision
Keep short ADRs in `docs/adr/`, one per significant decision, numbered and never rewritten after acceptance.

## Consequences
Anyone can read the reasoning behind a contract without digging through PR threads. Each ADR costs a few minutes
to write. A reversed decision needs a new ADR that supersedes the old one.
