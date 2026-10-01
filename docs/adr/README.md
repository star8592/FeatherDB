# Architecture Decision Records

No major FeatherDB architecture decision should live only in chat, code or README prose.

## States

`proposed` -> `experimenting` -> `accepted` / `rejected` / `superseded`

During the research rounds most ADRs should remain `proposed` or `experimenting`.

## Required sections

- Context / user pain
- Decision status
- Constraints and resource budget
- Alternatives
- Evidence (papers, systems, issues, experiments)
- Safety invariants
- Liveness expectations
- Failure modes
- Operational consequences
- Memory/CPU/network/disk cost
- Deterministic simulation plan
- Real-cluster fault test plan
- Reversal/migration strategy

## Initial ADR queue

- ADR-0001: scope — byte KV before SQL
- ADR-0002: logical tablet indirection
- ADR-0003: membership/failure detection
- ADR-0004: placement algorithm
- ADR-0005: replication/version model
- ADR-0006: metadata consistency boundary
- ADR-0007: repair/anti-entropy
- ADR-0008: storage engine abstraction
- ADR-0009: transport
- ADR-0010: deterministic simulation architecture
- ADR-0011: resource budgets/backpressure
- ADR-0012: node identity/security bootstrap

These are questions, not predetermined decisions.
