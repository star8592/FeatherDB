# ADR-0003: Rust 1.99 Toolchain Baseline

Status: Accepted
Date: 2026-10-08

## Context

FeatherDB originally declared Rust 1.85 while current storage candidates Fjall 3.1.12 and redb 4.3.0 require Rust 1.90. The development machine already runs the current stable Rust 1.99.0. Maintaining an older advertised MSRV would constrain dependency selection and create a split between production and benchmark toolchains.

## Decision

FeatherDB standardizes on:

    rustc 1.99.0
    Cargo 1.99.x
    Edition 2024

The root rust-toolchain.toml pins 1.99.0 and installs rustfmt/clippy. Workspace and standalone benchmark/experiment manifests declare rust-version = 1.99.

## Consequences

- Current Fjall/redb releases are toolchain-compatible.
- Contributors get a reproducible compiler/formatter/linter baseline through rustup.
- Rust versions older than 1.99 are no longer part of the supported build contract.
- Future stable upgrades must update the toolchain pin, rust-version declaration, CI and this development record together, followed by the full quality gate.


## Follow-up toolchain alignment

The repository also standardizes on Cargo resolver 3. The pinned toolchain includes rust-analyzer and rust-src in addition to rustfmt/clippy. CI is kept on current stable GitHub Action generations and Dependabot performs daily Cargo/GitHub Actions update checks.

As of 2026-10-08, Fjall 3.1.12, redb 4.3.0, rustup 1.29.1 and local gitleaks 8.30.1 are already current, and cargo update reports no newer Rust-1.99-compatible transitive packages for the storage benchmark/adapter lockfiles.
