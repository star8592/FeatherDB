# Split Boundary Strategy Experiment — 2026-10-07

## Goal

Test whether FeatherDB should replace hash-midpoint tablet splitting with a data-aware boundary.

## Strategies

The research module compares:

- HashMidpoint
- ByteMedian
- HeatMedian

The input is an ordered set of token samples carrying:

    bytes
    heat

The chosen boundary must remain strictly inside the parent range.

## Skewed workload

The synthetic workload intentionally separates storage skew from access heat.

Observed:

| Strategy | Byte imbalance (ppm) | Heat imbalance (ppm) |
|---|---:|---:|
| HashMidpoint | 800,000 | 923,076 |
| ByteMedian | **500,000** | 948,717 |
| HeatMedian | 934,000 | **358,974** |

Interpretation:

- ByteMedian materially improves byte balance but slightly worsens heat balance.
- HeatMedian materially improves heat balance but produces very poor byte balance.
- HashMidpoint is poor on both signals here, but requires no telemetry and is deterministic from metadata alone.

## Same-token hotspot

Two equally hot samples at exactly the same token cannot be separated by any token boundary.

Observed:

    HeatMedian boundary = token + 1
    heat imbalance = 1,000,000 ppm

This is an important negative result.

A single-key/same-token hotspot requires another mechanism such as:

- request-level hotspot handling;
- key-aware sharding above the hash tablet layer;
- caching/admission controls;
- application-level decomposition.

Tablet range split alone cannot solve it.

## Quality gate

- cargo test: **68 passed, 0 failed**
- cargo clippy --all-targets --all-features -- -D warnings: **pass**
- cargo fmt --check: **pass**

## Decision

Do not replace midpoint split with one universal data-aware strategy.

Keep the architecture:

    split trigger
      -> boundary objective
      -> boundary strategy
      -> range transform

Current candidate policy:

1. HashMidpoint remains the deterministic fallback.
2. ByteMedian may be used when the objective is storage-size equalization and telemetry confidence is sufficient.
3. HeatMedian may be used for a hot-range isolation experiment, not as a general storage-balancing boundary.
4. A future multi-objective/Pareto policy should be tested before any production default changes.

The split boundary is therefore a policy input, not a hidden implementation detail.
