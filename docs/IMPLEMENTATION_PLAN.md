# Implementation Plan

## Scope

Build a production-grade, local-first ingestion system for Binance UM futures minute dataset with a migration-safe layout for cloud object storage.

## Phase 1: Foundation

1. Packaging and quality tooling (`pyproject`, lint, typecheck, tests).
2. Canonical schema registry (66 columns, support class, fill policy).
3. Config and logging bootstrap.

## Phase 2: Data Plane

1. Vision URL builder + existence checks.
2. REST collectors for hot/warm windows with retry/backoff.
3. Transform engine to normalize to 1-minute canonical schema.

## Phase 3: State + Idempotency

1. SQLite watermark and partition ledger.
2. Atomic parquet commit with temp-write + rename.
3. Partition schema/content hashing and DQ validation gates.

## Phase 4: Orchestration

1. Poll loop every 60s with safety lag.
2. Missing-range computation and hour partition execution.
3. Watermark advancement conditioned on successful earliest-minute commit.

## Phase 5: Hardening

1. Metrics zip schema inspection utility.
2. Structured operational logs and provenance fields.
3. Extend optional live collectors (depth/markPrice/forceOrder) when needed.
