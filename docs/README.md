# Technical Documentation Index

This folder contains handover, onboarding, maintenance, and operations documentation for the repository as implemented today.

Recommended reading order:

1. [High-Level Design](HLD.md) - system scope, major components, data architecture, runtime model, and known gaps.
2. [Architecture Diagrams](ARCHITECTURE_DIAGRAMS.md) - Mermaid diagrams for context, data flow, API flow, storage, and state models.
3. [Low-Level Design](LLD.md) - module-level behavior, algorithms, state schemas, validation, API routing, and test traceability.
4. [Developer Guide](DEVELOPER_GUIDE.md) - local setup, common commands, extension points, and review checklist.
5. [Operations Guide](OPERATIONS_GUIDE.md) - process startup, backfill/repair, validation, cleanup, failure modes, and monitoring recommendations.

Existing supporting docs:

- [Implementation Plan](IMPLEMENTATION_PLAN.md)
- [Missing Fields Implementation Spec](missing_fields_implementation.md)
- [Requirements Addendum](../REQUIREMENTS_ADDENDUM.md)

Documentation labels:

- **Observed** means directly verified from code, configs, scripts, tests, or current repository files.
- **Inferred** means strongly implied by the implementation but not explicitly declared as a formal requirement.
- **Unclear / needs confirmation** means the repository does not fully define the behavior or operational contract.

